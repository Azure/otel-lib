// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![warn(clippy::all, clippy::pedantic)]

use std::{
    fs::{remove_file, File},
    io::Write,
    net::SocketAddr,
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use openssl::ssl::{Ssl, SslAcceptor, SslFiletype, SslMethod};
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

use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::mpsc::{self, Receiver, Sender},
};
use tokio_openssl::SslStream;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{
    async_trait,
    transport::{server::Connected, Server},
    Request, Response, Status,
};
use uuid::Uuid;
pub struct TlsStream(pub SslStream<TcpStream>);
impl Connected for TlsStream {
    type ConnectInfo = std::net::SocketAddr;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.0.get_ref().peer_addr().unwrap()
    }
}

impl AsyncRead for TlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_read(cx, buf)
    }
}

impl AsyncWrite for TlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().0).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_shutdown(cx)
    }
}

pub async fn recv_wrapper(mut receiver: Receiver<()>) {
    if receiver.recv().await.is_none() {
        // Note: We need to use eprintln! and not the log macros here as the tests
        // create and assert on specific logs.
        eprintln!("shutdown channel closed unexpectedly");
    }
}

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
        let listener = TcpListener::bind(self.endpoint).await.unwrap();

        if let Some(self_signed_cert) = self.self_signed_cert {
            let mut ssl_builder = SslAcceptor::mozilla_modern(SslMethod::tls()).unwrap();
            ssl_builder
                .set_private_key_file(self_signed_cert.server_key.clone(), SslFiletype::PEM)
                .unwrap();
            ssl_builder
                .set_certificate_chain_file(self_signed_cert.server_cert.clone())
                .unwrap();
            let ssl_acceptor = Arc::new(ssl_builder.build());

            // Create async incoming TLS stream listener
            let incoming = async_stream::stream! {
                loop {
                    let (stream, _) = match listener.accept().await {
                        Ok(s) => s,
                        Err(e) => {
                            // Note: We need to use eprintln! and not the log macros here as the tests
                            // create and assert on specific logs.
                            eprintln!("failed to accept TCP connection: {e:?}");
                            continue;
                        }
                    };
                    let ssl = match Ssl::new(ssl_acceptor.context()) {
                        Ok(ssl) => ssl,
                        Err(e) => {
                            eprintln!("failed to create Ssl object: {e:?}");
                            continue;
                        }
                    };

                    let mut ssl_stream = match SslStream::new(ssl, stream) {
                        Ok(ssl_stream) => ssl_stream,
                        Err(e) => {
                            eprintln!("failed to create SslStream: {e:?}");
                            continue;
                        }
                    };

                    if let Err(e) = Pin::new(&mut ssl_stream).accept().await {
                        eprintln!("failed to accept TLS connection: {e:?}");
                        continue;
                    }
                    let tls_stream = TlsStream(ssl_stream);
                    yield Ok::<TlsStream, std::io::Error>(tls_stream);
                }
            };

            let () = server_builder
                .add_service(MetricsServiceServer::new(MockMetricsService::new(
                    self.echo_metric_tx,
                )))
                .add_service(LogsServiceServer::new(MockLogsService::new(
                    self.echo_logs_tx,
                )))
                .serve_with_incoming_shutdown(incoming, recv_wrapper(self.shutdown_rx))
                .await
                .unwrap();
        } else {
            // Create incoming TCP stream listener
            let incoming = TcpListenerStream::new(listener);

            // Start the server
            server_builder
                .add_service(MetricsServiceServer::new(MockMetricsService::new(
                    self.echo_metric_tx,
                )))
                .add_service(LogsServiceServer::new(MockLogsService::new(
                    self.echo_logs_tx,
                )))
                .serve_with_incoming_shutdown(incoming, recv_wrapper(self.shutdown_rx))
                .await
                .unwrap();
        }
    }
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
