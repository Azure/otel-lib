// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![deny(rust_2018_idioms)]
#![warn(clippy::all, clippy::pedantic)]

use std::{path::PathBuf, sync::Arc, time::Duration};

use hyper::StatusCode;
use hyper_util::rt::TokioIo;
use log::{error, info, warn};

use openssl::ssl::{SslConnector, SslConnectorBuilder, SslMethod};
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

// TODO: evaluate if we should keep supporting writing metrics to stdout.
use opentelemetry_stdout::MetricsExporterBuilder;

use prometheus::{Encoder, Registry, TextEncoder};
use tokio::net::TcpStream;
use tokio_openssl::SslStream;
use url::Url;

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
) -> Result<TonicExporterBuilder, ObsError> {
    let ep = Endpoint::new(url, ca_cert_path.clone()).unwrap();
    let timeout = Duration::from_secs(30); // TODO: Make this timeout configurable
    let addr = format!("{}:{}", ep.server_name(), ep.server_port());
    let server_name = ep.server_name().to_owned();
    let scheme = ep.url().scheme();

    if scheme.eq("https") || scheme.eq("grpcs") {
        let url_modified = ep.url().to_string().replace("https", "http");
        let tonic_endpoint = tonic::transport::channel::Endpoint::try_from(url_modified).unwrap();
        let method = SslMethod::tls();
        let mut ssl_connector: SslConnectorBuilder = SslConnector::builder(method).unwrap();
        if let Some(ca_cert_path) = ca_cert_path {
            ssl_connector
                .set_ca_file(ca_cert_path.clone())
                .map_err(|e| {
                    ObsError::GrpcClientError(format!(
                        "error setting CA file to {ca_cert_path:?}: {e}"
                    ))
                })?;
        } else {
            ssl_connector.set_default_verify_paths().map_err(|e| {
                ObsError::GrpcClientError(format!("error setting default verify paths: {e}"))
            })?;
        }

        // Create a custom tonic connector that uses openssl instead of rustls
        let ssl_connector = Arc::new(ssl_connector.build());
        let custom_connector = tower::service_fn(move |_: tonic::transport::Uri| {
            let connector = Arc::clone(&ssl_connector);
            let addr = addr.clone();
            let server_name = server_name.clone();
            async move {
                let tcp_stream = TcpStream::connect(addr.clone()).await?;
                let config = connector.configure()?;
                let ssl = config.into_ssl(&server_name)?;
                let mut ssl_stream = SslStream::new(ssl, tcp_stream)?;
                std::pin::Pin::new(&mut ssl_stream).connect().await?;
                Ok::<_, ObsError>(TokioIo::new(ssl_stream))
            }
        });
        let channel = tonic_endpoint
            .timeout(timeout)
            .connect_with_connector_lazy(custom_connector);
        Ok(exporter_builder.with_channel(channel))
    } else {
        Ok(exporter_builder)
    }
}

#[derive(Clone, Debug, PartialEq)]
struct Endpoint {
    server_name: String,
    server_port: u16,
    trusted_ca_path: Option<PathBuf>,
    url: Url,
}

impl Endpoint {
    fn new(url: &str, trusted_ca_path: Option<String>) -> Result<Self, ObsError> {
        let url = Url::parse(url).map_err(ObsError::InvalidEndpointUrl)?;
        let server_name = url
            .host_str()
            .ok_or(ObsError::EndpointMissingHost(url.to_string()))?;
        let server_port = url
            .port()
            .ok_or(ObsError::EndpointMissingPort(url.to_string()))?;
        Ok(Self {
            server_name: server_name.to_owned(),
            server_port,
            trusted_ca_path: trusted_ca_path.map(PathBuf::from),
            url,
        })
    }

    fn server_name(&self) -> &str {
        &self.server_name
    }

    fn server_port(&self) -> u16 {
        self.server_port
    }

    fn url(&self) -> &Url {
        &self.url
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ObsError {
    #[error("gRPC client error: {0:}")]
    GrpcClientError(String),

    #[error("io error: {0:?}")]
    IoError(#[from] std::io::Error),

    #[error("failed to create TLS server config: {0:?}")]
    TlsServerConfig(#[from] openssl::error::ErrorStack),

    #[error("tokio decoder failed: {0:?}")]
    TokioDecoderError(String),

    #[error("file does not contain a private key in a supported encoding")]
    NoSupportedPrivateKeyFound,

    #[error("tls handshake error {0:?}")]
    TlsError(#[from] openssl::ssl::Error),

    #[error("could not parse endpoint url: {0:?}")]
    InvalidEndpointUrl(#[source] url::ParseError),

    #[error("could not parse port from endpoint: {0:?}")]
    EndpointMissingPort(String),

    #[error("could not parse host from endpoint: {0:?}")]
    EndpointMissingHost(String),
}
