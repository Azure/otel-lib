// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![warn(clippy::all, clippy::pedantic)]

use std::{
    fs::{read, remove_file, File},
    io::Write,
    net::SocketAddr,
    path::PathBuf,
};

use opentelemetry_proto::tonic::collector::{
    logs::v1::{
        logs_service_server::{LogsService, LogsServiceServer},
        ExportLogsServiceRequest, ExportLogsServiceResponse,
    },
    metrics::v1::{
        metrics_service_server::{MetricsService, MetricsServiceServer},
        ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    },
};

use tokio::sync::mpsc::{self, Receiver, Sender};
use tonic::{
    async_trait,
    transport::{Identity, Server, ServerTlsConfig},
    Request, Response, Status,
};
use uuid::Uuid;

pub struct MockServer {
    pub endpoint: String,
    pub shutdown_tx: Sender<()>,
    pub metrics_rx: Receiver<ExportMetricsServiceRequest>,
    pub logs_rx: Receiver<ExportLogsServiceRequest>,
    pub server: OtlpServer,
}

impl MockServer {
    #[must_use]
    /// Create a new mock server
    ///
    /// # Panics
    ///
    /// Will panic if socketaddr parse fails
    pub fn new(port: u16, self_signed_cert: Option<SelfSignedCert>) -> Self {
        // Setup mock otlp server
        let socketaddr = format!("127.0.0.1:{port}");

        let endpoint = if self_signed_cert.is_some() {
            format!("https://localhost:{port}")
        } else {
            format!("http://localhost:{port}")
        };

        let (metrics_tx, metrics_rx) = mpsc::channel(10);
        let (logs_tx, logs_rx) = mpsc::channel(10);
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
        let server = OtlpServer::new(
            socketaddr.parse().unwrap(),
            shutdown_rx,
            metrics_tx,
            logs_tx,
            self_signed_cert,
        );

        Self {
            endpoint,
            shutdown_tx,
            metrics_rx,
            logs_rx,
            server,
        }
    }
}

pub struct OtlpServer {
    endpoint: SocketAddr,
    shutdown_rx: Receiver<()>,
    echo_metric_tx: Sender<ExportMetricsServiceRequest>,
    echo_logs_tx: Sender<ExportLogsServiceRequest>,
    self_signed_cert: Option<SelfSignedCert>,
}

impl OtlpServer {
    fn new(
        endpoint: SocketAddr,
        shutdown_rx: Receiver<()>,
        echo_metric_tx: Sender<ExportMetricsServiceRequest>,
        echo_logs_tx: Sender<ExportLogsServiceRequest>,
        self_signed_cert: Option<SelfSignedCert>,
    ) -> Self {
        Self {
            endpoint,
            shutdown_rx,
            echo_metric_tx,
            echo_logs_tx,
            self_signed_cert,
        }
    }

    /// Run the server
    ///
    /// # Panics
    /// Will panic if the port is already in use
    ///
    pub async fn run(self) {
        let mut server_builder = Server::builder();

        if let Some(self_signed_cert) = self.self_signed_cert {
            let cert_bytes = read(&self_signed_cert.server_cert).unwrap();
            let key_bytes = read(&self_signed_cert.server_key).unwrap();
            let mut tls_config = ServerTlsConfig::new();
            tls_config = tls_config.identity(Identity::from_pem(cert_bytes, key_bytes));
            server_builder = server_builder.tls_config(tls_config).unwrap();
        }

        let () = server_builder
            .add_service(MetricsServiceServer::new(MockMetricsService::new(
                self.echo_metric_tx,
            )))
            .add_service(LogsServiceServer::new(MockLogsService::new(
                self.echo_logs_tx,
            )))
            .serve_with_shutdown(self.endpoint, recv_wrapper(self.shutdown_rx))
            .await
            .unwrap();
    }
}

async fn recv_wrapper(mut receiver: Receiver<()>) {
    let _ = receiver.recv().await;
}

#[derive(Clone)]
pub struct SelfSignedCert {
    pub server_cert: PathBuf,
    pub server_key: PathBuf,
    pub ca_cert: PathBuf,
}

impl SelfSignedCert {
    /// Clean up cert files
    /// # Panics
    ///
    /// Will panic remove file fails
    ///
    pub fn cleanup(&self) {
        remove_file(&self.server_cert).unwrap();
        remove_file(&self.server_key).unwrap();
    }

    #[must_use]
    pub fn get_ca_cert_path(&self) -> String {
        self.ca_cert.to_string_lossy().into_owned()
    }
}
/// Convenience function to generate certs
/// # Panics
///
/// Will panic if cert generation fails
#[must_use]
pub fn generate_self_signed_cert() -> SelfSignedCert {
    let prefix = Uuid::new_v4();
    // Generate a self-signed cert and key
    let cert =
        rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
    let cert_pem = cert.serialize_pem().unwrap();
    let cert_path = PathBuf::from(format!("./{prefix}_cert.pem"));
    let mut cert_file = File::create(cert_path.clone()).unwrap();
    cert_file.write_all(cert_pem.as_bytes()).unwrap();

    let key_pem = cert.serialize_private_key_pem();
    let key_path = PathBuf::from(format!("./{prefix}_key.pem"));
    let mut key_file = File::create(key_path.clone()).unwrap();
    key_file.write_all(key_pem.as_bytes()).unwrap();

    SelfSignedCert {
        server_cert: cert_path.clone(),
        server_key: key_path,
        ca_cert: cert_path,
    }
}

struct MockLogsService {
    echo_sender: Sender<ExportLogsServiceRequest>,
}

#[async_trait]
impl LogsService for MockLogsService {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        // Echo received request over channel
        self.echo_sender.send(request.into_inner()).await.unwrap();
        let response = ExportLogsServiceResponse {
            partial_success: None,
        };
        Ok(Response::new(response))
    }
}

impl MockLogsService {
    fn new(echo_sender: Sender<ExportLogsServiceRequest>) -> Self {
        Self { echo_sender }
    }
}

#[derive(Debug)]
struct MockMetricsService {
    echo_sender: Sender<ExportMetricsServiceRequest>,
}

#[async_trait]
impl MetricsService for MockMetricsService {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        // Echo received request over channel
        self.echo_sender.send(request.into_inner()).await.unwrap();
        let response = ExportMetricsServiceResponse {
            partial_success: None,
        };
        Ok(Response::new(response))
    }
}

impl MockMetricsService {
    fn new(echo_sender: Sender<ExportMetricsServiceRequest>) -> Self {
        Self { echo_sender }
    }
}
