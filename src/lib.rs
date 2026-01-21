// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![deny(rust_2018_idioms)]
#![warn(clippy::all, clippy::pedantic)]

use std::{collections::HashSet, path::PathBuf, str::FromStr, sync::Arc, time::Duration};

use hyper::StatusCode;
use hyper_util::rt::TokioIo;
use log::{debug, error, info, warn};

use notify::{RecommendedWatcher, RecursiveMode};
use notify_debouncer_mini::{new_debouncer, DebouncedEvent, Debouncer};
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
use tokio::{net::TcpStream, sync::mpsc};
use tokio_openssl::SslStream;
use tonic::{metadata::AsciiMetadataValue, service::Interceptor, Status};
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
    ca_cert_paths: HashSet<String>,
    shutdown_tx: mpsc::Sender<()>,
    shutdown_rx: mpsc::Receiver<()>,
}

pub struct ShutdownHandle {
    sender: mpsc::Sender<()>,
}

impl ShutdownHandle {
    /// Send a shutdown signal
    /// # Errors
    /// * `mpsc::error::SendError` - If the shutdown signal could not be sent
    pub async fn shutdown(&self) -> Result<(), mpsc::error::SendError<()>> {
        self.sender.send(()).await
    }
}

impl Otel {
    #[must_use]
    pub fn new(config: Config) -> Otel {
        let logger_provider = match loggers::init_logs(config.clone()) {
            Ok(logger_provider) => Some(logger_provider),
            Err(e) => {
                warn!("unable to initialize otel logger as another library has already initialized a global logger:{e:?}");
                None
            }
        };

        let (registry, meter_provider) = init_metrics(config.clone());

        // Gather a list of CA cert paths from logs and metrics config
        let mut ca_cert_paths: HashSet<String> = HashSet::new();
        if let Some(metric_targets) = config.metrics_export_targets {
            for metric_target in metric_targets {
                if let Some(ca_cert_path) = &metric_target.ca_cert_path {
                    ca_cert_paths.insert(ca_cert_path.clone());
                }
            }
        }
        if let Some(log_targets) = config.log_export_targets {
            for log_target in log_targets {
                if let Some(ca_cert_path) = &log_target.ca_cert_path {
                    ca_cert_paths.insert(ca_cert_path.clone());
                }
            }
        }

        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        Otel {
            registry,
            meter_provider,
            logger_provider,
            ca_cert_paths,
            shutdown_tx,
            shutdown_rx,
        }
    }

    /// Long running tasks for otel propagation.
    ///
    /// # Errors
    /// * `OtelError::WatcherError` - If there is an error with the CA file watcher
    pub async fn run(&mut self) -> Result<(), OtelError> {
        // Create watcher task to detect changes to the CA files
        let (mut ca_watcher, mut ca_watcher_rx) = create_watcher()?;
        self.ca_cert_paths.iter().for_each(|path| {
            let _ = ca_watcher
                .watcher()
                .watch(&PathBuf::from(path), RecursiveMode::NonRecursive);
            debug!("watching for changes to {path}");
        });

        tokio::select! {
            notification = ca_watcher_rx.recv(), if !self.ca_cert_paths.is_empty() => {
                match notification {
                    Some(Ok(event)) => {
                        info!("CA cert path changed: {event:?}");
                    }
                    Some(Err(e)) => {
                        error!("error received from CA file watcher: {e:?}");
                    }
                    None => {
                        warn!("CA file watcher channel closed.");
                    }
                }
                warn!("Exiting to allow restart of component.");
                return Err(OtelError::CaWatcherEvent)
            }
            () = async {
                if let Some(prometheus_registry) = &self.registry {
                    let _ = httpserver_init(
                        prometheus_registry.port,
                        prometheus_registry.registry.clone(),
                    ).await;
                }
            }, if self.registry.is_some() => {
                error!("prometheus server stopped. Exiting.");
                return Err(OtelError::PrometheusServerStopped)
            }
            _ = self.shutdown_rx.recv() => {
                // Graceful shutdown that flushes any pending metrics and logs to the exporter.
                info!("shutting down otel component");

                // Use a timeout for flush/shutdown operations to prevent hanging
                // when the server is unavailable
                let shutdown_timeout = Duration::from_secs(10);

                let flush_result = tokio::time::timeout(
                    shutdown_timeout,
                    tokio::task::spawn_blocking({
                        let meter_provider = self.meter_provider.clone();
                        move || meter_provider.force_flush()
                    })
                ).await;

                match flush_result {
                    Err(_) => warn!("meter provider force_flush timed out"),
                    Ok(Err(e)) => warn!("meter provider force_flush task failed: {e:?}"),
                    Ok(Ok(Err(e))) => warn!("encountered error while flushing metrics: {e:?}"),
                    Ok(Ok(Ok(()))) => debug!("meter provider force_flush completed"),
                }

                let shutdown_result = tokio::time::timeout(
                    shutdown_timeout,
                    tokio::task::spawn_blocking({
                        let meter_provider = self.meter_provider.clone();
                        move || meter_provider.shutdown()
                    })
                ).await;

                match shutdown_result {
                    Err(_) => warn!("meter provider shutdown timed out"),
                    Ok(Err(e)) => warn!("meter provider shutdown task failed: {e:?}"),
                    Ok(Ok(Err(e))) => warn!("encountered error while shutting down meter provider: {e:?}"),
                    Ok(Ok(Ok(()))) => debug!("meter provider shutdown completed"),
                }

                if let Some(logger_provider) = self.logger_provider.clone() {
                    let flush_result = tokio::time::timeout(
                        shutdown_timeout,
                        tokio::task::spawn_blocking({
                            let lp = logger_provider.clone();
                            move || lp.force_flush()
                        })
                    ).await;

                    if let Err(_) | Ok(Err(_)) = flush_result {
                        warn!("logger provider force_flush timed out or failed");
                    }

                    let shutdown_result = tokio::time::timeout(
                        shutdown_timeout,
                        tokio::task::spawn_blocking(move || logger_provider.shutdown())
                    ).await;

                    if let Err(_) | Ok(Err(_)) = shutdown_result {
                        warn!("logger provider shutdown timed out or failed");
                    }
                }

            }
        }

        Ok(())
    }

    /// Get a shutdown handle that can be used to trigger shutdown from other contexts.
    #[must_use]
    pub fn shutdown_handle(&self) -> ShutdownHandle {
        ShutdownHandle {
            sender: self.shutdown_tx.clone(),
        }
    }

    /// Convenience function to trigger shutdown from the Otel struct directly.
    ///
    /// # Errors
    /// * `mpsc::error::SendError` - If the shutdown signal could not be sent
    pub async fn shutdown(&self) -> Result<(), mpsc::error::SendError<()>> {
        self.shutdown_tx.send(()).await // Just sends signal, run() handles the rest
    }
}

#[allow(clippy::type_complexity)]
fn create_watcher() -> Result<
    (
        Debouncer<RecommendedWatcher>,
        mpsc::Receiver<Result<Vec<DebouncedEvent>, notify::Error>>,
    ),
    OtelError,
> {
    // Create async channel for watcher notifications
    let (async_tx, async_rx) = mpsc::channel(1);

    // Create watcher with event handler that sends events to async channel
    let watcher = new_debouncer(Duration::from_secs(1), move |res| {
        let async_tx = async_tx.clone();
        futures::executor::block_on(async {
            if let Err(e) = async_tx.send(res).await {
                error!("error sending watcher notification: {e}");
            }
        });
    })
    .map_err(OtelError::WatcherError)?;

    // Return watch and receiver
    Ok((watcher, async_rx))
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
                error!("unable to setup prometheus endpoint due to: {e:?}");
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
            if let Some(bearer_token_provider_fn) = export_target.bearer_token_provider_fn {
                let auth_interceptor = AuthIntercepter {
                    bearer_token_provider_fn,
                };
                exporter_builder = exporter_builder.with_interceptor(auth_interceptor);
            }
            exporter_builder = match handle_tls(
                exporter_builder,
                &export_target.url,
                export_target.ca_cert_path,
                Duration::from_secs(export_target.timeout),
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
                .with_timeout(Duration::from_secs(export_target.timeout))
                .build();
            meter_provider_builder = meter_provider_builder.with_reader(reader);
        }
    }

    if config.emit_metrics_to_stdout {
        let exporter = MetricsExporterBuilder::default()
            .with_encoder(|writer, data| {
                if let Err(e) = serde_json::to_writer_pretty(writer, &data) {
                    error!("writing metrics to log failed due to: {e:?}");
                }
                Ok(())
            })
            .build();

        let reader = PeriodicReader::builder(exporter, runtime::Tokio)
            .with_timeout(Duration::from_secs(30))
            .build();
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
    timeout: Duration,
) -> Result<TonicExporterBuilder, OtelError> {
    let (server_name, server_port, scheme) = {
        let url = Url::parse(url).map_err(OtelError::InvalidEndpointUrl)?;
        let server_name = url
            .host_str()
            .ok_or(OtelError::EndpointMissingHost(url.to_string()))?
            .to_owned();
        let server_port = url
            .port()
            .ok_or(OtelError::EndpointMissingPort(url.to_string()))?;
        (server_name, server_port, url.scheme().to_owned())
    };

    let addr = format!("{server_name}:{server_port}");

    if scheme.eq("https") || scheme.eq("grpcs") {
        let tonic_endpoint = tonic::transport::channel::Endpoint::try_from(url.to_owned())
            .map_err(|e| {
                OtelError::GrpcClientError(format!("error creating tonic channel to {url}: {e:?}",))
            })?;

        let method = SslMethod::tls();
        let mut ssl_connector_builder: SslConnectorBuilder = SslConnector::builder(method)
            .map_err(|e| {
                OtelError::GrpcClientError(format!("error creating SSL connector: {e:?}"))
            })?;

        if let Some(ca_cert_path) = ca_cert_path {
            ssl_connector_builder
                .set_ca_file(ca_cert_path.clone())
                .map_err(|e| {
                    OtelError::GrpcClientError(format!(
                        "error setting CA file to {ca_cert_path:?}: {e}"
                    ))
                })?;
        } else {
            ssl_connector_builder
                .set_default_verify_paths()
                .map_err(|e| {
                    OtelError::GrpcClientError(format!("error setting default verify paths: {e}"))
                })?;
        }

        // Set ALPN property for HTTP/2 over TLS as per RFC 7540. See `https://datatracker.ietf.org/doc/html/rfc7540#section-3.3`
        ssl_connector_builder
            .set_alpn_protos(b"\x02h2")
            .map_err(|e| {
                OtelError::GrpcClientError(format!("error setting `h2` ALPN Header: {e}"))
            })?;

        // Create a custom tonic connector that uses openssl instead of rustls
        let ssl_connector = Arc::new(ssl_connector_builder.build());
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
                Ok::<_, OtelError>(TokioIo::new(ssl_stream))
            }
        });
        let channel = tonic_endpoint
            .connect_timeout(timeout)
            .timeout(timeout)
            .connect_with_connector_lazy(custom_connector);
        Ok(exporter_builder.with_channel(channel))
    } else {
        Ok(exporter_builder)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum OtelError {
    #[error("gRPC client error: {0:}")]
    GrpcClientError(String),

    #[error("io error: {0:?}")]
    IoError(#[from] std::io::Error),

    #[error("failed to create TLS server config: {0:?}")]
    TlsServerConfig(#[from] openssl::error::ErrorStack),

    #[error("tls handshake error {0:?}")]
    TlsError(#[from] openssl::ssl::Error),

    #[error("could not parse endpoint url: {0:?}")]
    InvalidEndpointUrl(#[source] url::ParseError),

    #[error("could not parse port from endpoint: {0:?}")]
    EndpointMissingPort(String),

    #[error("could not parse host from endpoint: {0:?}")]
    EndpointMissingHost(String),

    #[error("watcher error: {0:?}")]
    WatcherError(#[from] notify::Error),

    #[error("CA path watcher event occurred")]
    CaWatcherEvent,

    #[error("prometheus server stopped")]
    PrometheusServerStopped,
}

#[derive(Clone)]
struct AuthIntercepter {
    bearer_token_provider_fn: fn() -> String,
}

impl Interceptor for AuthIntercepter {
    fn call(&mut self, request: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        let bearer_token = (self.bearer_token_provider_fn)();
        let mut modified_request = request;
        let metadata = modified_request.metadata_mut();

        match AsciiMetadataValue::from_str(&format!("Bearer {bearer_token}")) {
            Ok(auth_header) => {
                metadata.append("authorization", auth_header);
            }
            Err(e) => {
                error!("unable to set auth header due to {e:?}");
            }
        }
        Ok(modified_request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_shutdown_approaches_both_work() {
        // Test that both shutdown approaches trigger the same graceful shutdown behavior
        {
            // Arrange
            let config = Config::default();
            let mut otel = Otel::new(config);
            let shutdown_handle = otel.shutdown_handle();

            let run_task = tokio::spawn(async move { otel.run().await });

            tokio::time::sleep(Duration::from_millis(50)).await;

            // Act
            shutdown_handle
                .shutdown()
                .await
                .expect("Should be able to send shutdown signal");

            // Assert
            let result = timeout(Duration::from_secs(1), run_task).await;
            assert!(
                result.is_ok(),
                "Run task should complete after direct shutdown"
            );
            assert!(result.unwrap().is_ok(), "Run task should exit cleanly");
        }

        {
            // Arrange
            let config = Config::default();
            let otel = Otel::new(config);

            // Act
            let shutdown_result = otel.shutdown().await;

            // Assert
            assert!(shutdown_result.is_ok(), "Convenience shutdown should work");
        }
    }
}
