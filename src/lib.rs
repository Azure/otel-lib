// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![deny(rust_2018_idioms)]
#![warn(clippy::all, clippy::pedantic)]

use std::time::Duration;

use log::{error, info, warn};

use hyper::StatusCode;
use opentelemetry::{global, KeyValue};

use axum::{http, Extension};

use opentelemetry_otlp::{ExportConfig, Protocol, TonicExporterBuilder, WithExportConfig};
use opentelemetry_sdk::{
    logs::LoggerProvider,
    metrics::{
        data::Temporality,
        reader::{DefaultAggregationSelector, DefaultTemporalitySelector, TemporalitySelector},
        InstrumentKind, PeriodicReader, SdkMeterProvider,
    },
    runtime, Resource,
};
use opentelemetry_stdout::MetricsExporterBuilder;
use prometheus::{Encoder, Registry, TextEncoder};
use tonic::transport::{Certificate, ClientTlsConfig};

use self::config::Config;

pub mod config;
mod filtered_log_processor;
pub mod loggers;
pub mod syslog_writer;

pub(crate) const SERVICE_NAME_KEY: &str = "service.name";

struct PrometheusRegistry {
    registry: Registry,
    port: u16,
}

pub struct Otel {
    registry: Option<PrometheusRegistry>,
    meter_provider: SdkMeterProvider,
    logger_provider: Option<LoggerProvider>,
}

impl Otel {
    #[must_use]
    pub fn new(config: Config) -> Otel {
        let logger_provider = match loggers::init_logs(config.clone()) {
            Ok(logger_provider) => Some(logger_provider),
            Err(e) => {
                warn!("unable to initialize otel logger as another library has already initialized a global logger:{:?}",e);
                None
            }
        };

        let (registry, meter_provider) = init_metrics(config);
        Otel {
            registry,
            meter_provider,
            logger_provider,
        }
    }

    /// Long running tasks for otel propagation.
    pub async fn run(&self) {
        if let Some(prometheus_registry) = &self.registry {
            let _ = httpserver_init(
                prometheus_registry.port,
                prometheus_registry.registry.clone(),
            )
            .await;
        }
    }

    /// Graceful shutdown that flushes any pending metrics and logs to the exporter.
    pub fn shutdown(&self) {
        if let Err(metrics_error) = self.meter_provider.force_flush() {
            warn!(
                "ecountered error while flushing metrics: {:?}",
                metrics_error
            );
        }
        if let Err(metrics_error) = self.meter_provider.shutdown() {
            warn!(
                "ecountered error while shutting down meter provider: {:?}",
                metrics_error
            );
        }

        if let Some(logger_provider) = self.logger_provider.clone() {
            logger_provider.force_flush();
            let _ = logger_provider.shutdown();
        }
    }
}

#[derive(Default, Debug)]
/// A temporality selector that returns Delta for all instruments

pub(crate) struct DeltaTemporalitySelector {}

impl DeltaTemporalitySelector {
    /// Create a new default temporality selector
    fn new() -> Self {
        Self::default()
    }
}

impl TemporalitySelector for DeltaTemporalitySelector {
    fn temporality(&self, _kind: InstrumentKind) -> Temporality {
        Temporality::Delta
    }
}

/// Initialize metrics based on passed in config.
/// This function will setup metrics exporters, create a Prometheus registry if enabled,
/// setup the stdout metrics writer if enabled, and initializes STATIC Metrics.
///
/// Returns the Prometheus Registry or None if Prometheus was disabled.
///
fn init_metrics(config: Config) -> (Option<PrometheusRegistry>, SdkMeterProvider) {
    let mut keys = vec![KeyValue::new(SERVICE_NAME_KEY, config.service_name.clone())];
    if let Some(resource_attributes) = config.resource_attributes {
        for attribute in resource_attributes {
            keys.push(KeyValue::new(attribute.key, attribute.value));
        }
    }
    let mut meter_provider_builder = SdkMeterProvider::builder().with_resource(Resource::new(keys));

    // Setup Prometheus Registry if configured
    let prometheus_registry = if let Some(prometheus_config) = config.prometheus_config {
        let registry = prometheus::Registry::new();
        match opentelemetry_prometheus::exporter()
            .with_registry(registry.clone())
            .build()
        {
            Ok(exporter) => {
                meter_provider_builder = meter_provider_builder.with_reader(exporter);
                Some(PrometheusRegistry {
                    registry,
                    port: prometheus_config.port,
                })
            }
            Err(e) => {
                error!("unable to setup prometheus endpoint due to: {:?}", e);
                None
            }
        }
    } else {
        None
    };

    // Add Metrics Exporters
    if let Some(export_targets_list) = config.metrics_export_targets {
        for export_target in export_targets_list {
            let export_config = ExportConfig {
                endpoint: export_target.url.clone(),
                timeout: Duration::from_secs(export_target.timeout),
                protocol: Protocol::Grpc,
            };

            let temporality_selector: Box<dyn TemporalitySelector> =
                if let Some(temporality) = export_target.temporality {
                    match temporality {
                        Temporality::Delta => Box::new(DeltaTemporalitySelector::new()),
                        _ => Box::new(DefaultTemporalitySelector::new()),
                    }
                } else {
                    Box::new(DefaultTemporalitySelector::new())
                };

            let mut exporter_builder = opentelemetry_otlp::new_exporter().tonic();
            exporter_builder = match handle_tls(
                exporter_builder,
                &export_target.url,
                export_target.ca_cert_path,
            ) {
                Ok(exporter_builder) => exporter_builder,
                Err(_) => {
                    continue;
                }
            };

            let exporter = match exporter_builder
                .with_export_config(export_config)
                .build_metrics_exporter(
                    // TODO: Make this also part of config?
                    Box::new(DefaultAggregationSelector::new()),
                    temporality_selector,
                ) {
                Ok(exporter) => exporter,
                Err(e) => {
                    error!(
                        "unable to set export to {} due to {:?}",
                        export_target.url, e
                    );
                    continue;
                }
            };

            let reader = PeriodicReader::builder(exporter, runtime::Tokio)
                .with_interval(Duration::from_secs(export_target.interval_secs))
                .build();
            meter_provider_builder = meter_provider_builder.with_reader(reader);
        }
    }

    if config.emit_metrics_to_stdout {
        let exporter = MetricsExporterBuilder::default()
            .with_encoder(|writer, data| {
                if let Err(e) = serde_json::to_writer_pretty(writer, &data) {
                    error!("writing metrics to log failed due to: {:?}", e);
                }
                Ok(())
            })
            .build();

        let reader = PeriodicReader::builder(exporter, runtime::Tokio).build();
        meter_provider_builder = meter_provider_builder.with_reader(reader);
    }

    let meter_provider = meter_provider_builder.build();
    global::set_meter_provider(meter_provider.clone());

    (prometheus_registry, meter_provider)
}

/// Setup the http server for the prometheus end point
///
/// # Arguments
/// * `http_port` - The port to listen on for http requests
/// * `registry` - The prometheus registry that contains the metrics
///
/// # Errors
/// * `hyper::Error` - If the http server fails to start
async fn httpserver_init(http_port: u16, registry: Registry) -> Result<(), hyper::Error> {
    info!("initializing prometheus metrics endpoint");
    let router = axum::Router::new()
        .route("/metrics", axum::routing::get(metrics_handler))
        .layer(Extension(registry));
    axum::Server::bind(&([0u8; 4], http_port).into())
        .serve(router.into_make_service())
        .await
}

async fn metrics_handler(
    Extension(data): Extension<Registry>,
) -> axum::response::Result<impl axum::response::IntoResponse> {
    let mut buffer = vec![];
    let encoder = TextEncoder::new();
    let metric_families = data.gather();
    match encoder.encode(&metric_families, &mut buffer) {
        Ok(()) => {
            let content_type = encoder.format_type().to_owned();
            let body = String::from_utf8_lossy(&buffer).into_owned();
            Ok((
                StatusCode::OK,
                [(http::header::CONTENT_TYPE, content_type)],
                body,
            ))
        }
        Err(e) => Ok((
            StatusCode::INTERNAL_SERVER_ERROR,
            [(http::header::CONTENT_TYPE, "text".to_string())],
            e.to_string(),
        )),
    }
}

fn handle_tls(
    exporter_builder: TonicExporterBuilder,
    url: &str,
    ca_cert_path: Option<String>,
) -> Result<TonicExporterBuilder, std::io::Error> {
    if url.starts_with("https") || url.starts_with("grpcs") {
        if let Some(ca_cert_path) = ca_cert_path {
            let ca_cert = std::fs::read(ca_cert_path);
            match ca_cert {
                Ok(ca_cert) => {
                    let ca_cert = Certificate::from_pem(ca_cert);
                    let tls_config = ClientTlsConfig::new().ca_certificate(ca_cert);
                    Ok(exporter_builder.with_tls_config(tls_config))
                }
                Err(e) => {
                    error!("unable to load ca_cert_file {:?}", e);
                    Err(e)
                }
            }
        } else {
            Ok(exporter_builder)
        }
    } else {
        Ok(exporter_builder.with_tls_config(ClientTlsConfig::new().with_native_roots()))
    }
}
