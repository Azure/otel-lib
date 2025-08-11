// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![warn(clippy::all, clippy::pedantic)]

use std::{
    fs::{remove_file, File},
    io::{Read, Write},
    net::SocketAddr,
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use openssl::{
    asn1::Asn1Time,
    nid::Nid,
    pkey::PKey,
    rsa::Rsa,
    ssl::{Ssl, SslAcceptor, SslFiletype, SslMethod},
    x509::{extension::SubjectAlternativeName, X509Builder, X509NameBuilder},
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

use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::mpsc::{self, Receiver, Sender},
};
use tokio_openssl::SslStream;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{
    async_trait,
    service::interceptor,
    transport::{server::Connected, Server},
    Request, Response, Status,
};
use tower::ServiceBuilder;
use uuid::Uuid;

const BEARER_TOKEN_FILE: &str = "token.txt";

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
    pub fn new(port: u16, self_signed_cert: Option<SelfSignedCert>, auth_enabled: bool) -> Self {
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
            auth_enabled,
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
    auth_enabled: bool,
}

impl OtlpServer {
    fn new(
        endpoint: SocketAddr,
        shutdown_rx: Receiver<()>,
        echo_metric_tx: Sender<ExportMetricsServiceRequest>,
        echo_logs_tx: Sender<ExportLogsServiceRequest>,
        self_signed_cert: Option<SelfSignedCert>,
        auth_enabled: bool,
    ) -> Self {
        Self {
            endpoint,
            shutdown_rx,
            echo_metric_tx,
            echo_logs_tx,
            self_signed_cert,
            auth_enabled,
        }
    }

    /// Run the server
    ///
    /// # Panics
    /// Will panic if the port is already in use
    ///
    pub async fn run(self) {
        let listener = TcpListener::bind(self.endpoint).await.unwrap();

        let metrics_service =
            MetricsServiceServer::new(MockMetricsService::new(self.echo_metric_tx));
        let logs_service = LogsServiceServer::new(MockLogsService::new(self.echo_logs_tx));

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

            if self.auth_enabled {
                Server::builder()
                    .layer(ServiceBuilder::new().layer(interceptor(Self::auth_interceptor)))
                    .add_service(metrics_service)
                    .add_service(logs_service)
                    .serve_with_incoming_shutdown(incoming, recv_wrapper(self.shutdown_rx))
                    .await
                    .unwrap();
            } else {
                Server::builder()
                    .add_service(metrics_service)
                    .add_service(logs_service)
                    .serve_with_incoming_shutdown(incoming, recv_wrapper(self.shutdown_rx))
                    .await
                    .unwrap();
            }
        } else {
            // Create incoming TCP stream listener
            let incoming = TcpListenerStream::new(listener);
            if self.auth_enabled {
                // Start the server
                Server::builder()
                    .layer(ServiceBuilder::new().layer(interceptor(Self::auth_interceptor)))
                    .add_service(metrics_service)
                    .add_service(logs_service)
                    .serve_with_incoming_shutdown(incoming, recv_wrapper(self.shutdown_rx))
                    .await
                    .unwrap();
            } else {
                Server::builder()
                    .add_service(metrics_service)
                    .add_service(logs_service)
                    .serve_with_incoming_shutdown(incoming, recv_wrapper(self.shutdown_rx))
                    .await
                    .unwrap();
            }
        }
    }

    #[allow(clippy::unnecessary_wraps)]
    #[allow(clippy::result_large_err)]
    fn auth_interceptor(request: Request<()>) -> Result<Request<()>, Status> {
        let header_value = request
            .metadata()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(header_value, format!("Bearer {}", get_test_bearer_token()));
        Ok(request)
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
    // prefix for file name
    let prefix = Uuid::new_v4();

    // Generate RSA key
    let rsa = Rsa::generate(2048).unwrap();
    let pkey = PKey::from_rsa(rsa).unwrap();

    // Build subject name
    let mut name_builder = X509NameBuilder::new().unwrap();
    name_builder
        .append_entry_by_nid(Nid::COMMONNAME, "localhost")
        .unwrap();
    let name = name_builder.build();

    // Build certificate
    let mut builder = X509Builder::new().unwrap();
    builder.set_version(2).unwrap();
    builder.set_subject_name(&name).unwrap();
    builder.set_issuer_name(&name).unwrap();
    builder.set_pubkey(&pkey).unwrap();
    builder
        .set_not_before(&Asn1Time::days_from_now(0).unwrap())
        .unwrap();
    builder
        .set_not_after(&Asn1Time::days_from_now(365).unwrap())
        .unwrap();

    // Add SAN extension
    let san = SubjectAlternativeName::new()
        .dns("localhost")
        .dns("127.0.0.1")
        .build(&builder.x509v3_context(None, None))
        .unwrap();
    builder.append_extension(san).unwrap();

    // Sign and build
    builder
        .sign(&pkey, openssl::hash::MessageDigest::sha256())
        .unwrap();
    let cert = builder.build();

    let cert_pem = cert.to_pem().unwrap();
    let cert_path = PathBuf::from(format!("./{prefix}_cert.pem"));
    let mut cert_file = File::create(cert_path.clone()).unwrap();
    cert_file.write_all(&cert_pem).unwrap();

    let key_pem = pkey.private_key_to_pem_pkcs8().unwrap();
    let key_path = PathBuf::from(format!("./{prefix}_key.pem"));
    let mut key_file = File::create(key_path.clone()).unwrap();
    key_file.write_all(&key_pem).unwrap();

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

/// Create or update a bearer token
///
/// # Panics
///
/// Will panic if creating the file or the write of the token to the file fails
pub fn create_or_update_bearer_token() {
    let token = format!("{}", Uuid::new_v4());
    let mut file = File::create(BEARER_TOKEN_FILE).unwrap();
    file.write_all(token.as_bytes()).unwrap();
}

/// retrieve a bearer token
///
/// # Panics
///
/// Will panic if opening the file or reading from it fails.
pub(crate) fn get_test_bearer_token() -> String {
    let mut file = File::open(BEARER_TOKEN_FILE).unwrap();
    let mut buf = String::new();
    file.read_to_string(&mut buf).unwrap();
    buf
}

/// delete the bearer token file
///
/// # Panics
///
/// Will panic if removing the file fails.
pub fn clean_up_bearer_token() {
    remove_file(BEARER_TOKEN_FILE).unwrap();
}
