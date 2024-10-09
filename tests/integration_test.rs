// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![deny(rust_2018_idioms)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::match_wild_err_arm)]

use std::fs::OpenOptions;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use filetime::{set_file_mtime, FileTime};
use log::{debug, error, info, trace, warn};
use mocks::{generate_self_signed_cert, MockServer};
use opentelemetry::{global, logs::Severity, metrics::MeterProvider};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value::{self, StringValue};
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::AggregationTemporality;
use opentelemetry_sdk::metrics::data::Temporality;
use otel_lib::{
    config::{Attribute, Config, LogsExportTarget, MetricsExportTarget, Prometheus},
    Otel,
};
use port_check::free_local_port_in_range;
use tokio::sync::mpsc::Receiver;
use tokio::sync::Mutex;
use tokio::time::timeout;

mod mocks;

// This test does the following
// 1) setup four mock otlp servers, two for filtered logs (with TLS and without) and the other 2 for unfiltered logs
// 2) configure the otel-lib to
//    a) log at the `warn` level
//    b) export logs to the filtered server the `error` level.
//    c) export all logs to the unfiltered servers
//    d) export metrics to the filtered servers
//    e) setup up a prometheus endpoint
// 3) add a metric, and create some logs
// 4) validate that metrics are exported to both otlp servers and at avaialable at the prometheus endpoint
// 5) validate that `warn` and `error` logs are exported to the unfiltered server and only `error` logs are exported to the filtered server.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn end_to_end_test() {
    // Setup mock otlp servers for filtered logs
    let self_signed_cert = generate_self_signed_cert();

    let filtered_target = MockServer::new(free_local_port_in_range(10000..=10100).unwrap(), None);
    tokio::spawn(async {
        filtered_target.server.run().await;
    });
    let filtered_target_with_tls = MockServer::new(
        free_local_port_in_range(10100..=10200).unwrap(),
        Some(self_signed_cert.clone()),
    );
    tokio::spawn(async {
        filtered_target_with_tls.server.run().await;
    });

    // Setup mock otlp servers for unfiltered logs
    let unfiltered_target = MockServer::new(free_local_port_in_range(10200..=10300).unwrap(), None);
    tokio::spawn(async {
        unfiltered_target.server.run().await;
    });
    let unfiltered_target_with_tls = MockServer::new(
        free_local_port_in_range(10300..=10400).unwrap(),
        Some(self_signed_cert.clone()),
    );
    tokio::spawn(async {
        unfiltered_target_with_tls.server.run().await;
    });

    // Setup Otel-lib
    let prom_port = free_local_port_in_range(10400..=10500).unwrap();
    let prometheus_config = Some(Prometheus { port: prom_port });

    let metric_targets = vec![
        MetricsExportTarget {
            url: filtered_target.endpoint.clone(),
            interval_secs: 1,
            timeout: 5,
            temporality: Some(Temporality::Cumulative),
            ca_cert_path: None,
        },
        MetricsExportTarget {
            url: filtered_target_with_tls.endpoint.clone(),
            interval_secs: 1,
            timeout: 5,
            temporality: Some(Temporality::Cumulative),
            ca_cert_path: Some(self_signed_cert.get_ca_cert_path()),
        },
        MetricsExportTarget {
            url: unfiltered_target.endpoint.clone(),
            interval_secs: 1,
            timeout: 5,
            temporality: Some(Temporality::Delta),
            ca_cert_path: None,
        },
        MetricsExportTarget {
            url: unfiltered_target_with_tls.endpoint.clone(),
            interval_secs: 1,
            timeout: 5,
            temporality: Some(Temporality::Delta),
            ca_cert_path: Some(self_signed_cert.get_ca_cert_path()),
        },
    ];

    let logs_targets = vec![
        LogsExportTarget {
            url: filtered_target.endpoint.clone(),
            interval_secs: 1,
            timeout: 5,
            export_severity: Some(Severity::Error),
            ca_cert_path: None,
        },
        LogsExportTarget {
            url: filtered_target_with_tls.endpoint.clone(),
            interval_secs: 1,
            timeout: 5,
            export_severity: Some(Severity::Error),
            ca_cert_path: Some(self_signed_cert.get_ca_cert_path()),
        },
        LogsExportTarget {
            url: unfiltered_target.endpoint,
            interval_secs: 1,
            timeout: 5,
            export_severity: None,
            ca_cert_path: None,
        },
        LogsExportTarget {
            url: unfiltered_target_with_tls.endpoint,
            interval_secs: 1,
            timeout: 5,
            export_severity: None,
            ca_cert_path: Some(self_signed_cert.get_ca_cert_path()),
        },
    ];

    let sample_attribute = Attribute {
        key: "resource_key1".to_owned(),
        value: "1".to_owned(),
    };

    let config = Config {
        emit_metrics_to_stdout: false,
        metrics_export_targets: Some(metric_targets),
        log_export_targets: Some(logs_targets),
        level: "warn".to_owned(),
        service_name: "end_to_end_test".to_owned(),
        enterprise_number: Some("123".to_owned()),
        resource_attributes: Some(vec![sample_attribute.clone()]),
        prometheus_config,
        ..Config::default()
    };

    let otel_component = Arc::new(Mutex::new(Otel::new(config)));
    let otel_long_running_task =
        tokio::spawn(async move { otel_component.lock().await.run().await });
    let run_tests_task = run_tests(
        filtered_target.metrics_rx,
        filtered_target_with_tls.metrics_rx,
        filtered_target.logs_rx,
        filtered_target_with_tls.logs_rx,
        unfiltered_target.metrics_rx,
        unfiltered_target_with_tls.metrics_rx,
        unfiltered_target.logs_rx,
        unfiltered_target_with_tls.logs_rx,
        &sample_attribute,
        prom_port,
    );

    run_tests_task.await;

    // Make a change to the CA cert file
    touch_file(&PathBuf::from(self_signed_cert.get_ca_cert_path()));

    // Confirm otel task exits
    match timeout(Duration::from_secs(2), otel_long_running_task).await {
        Ok(_) => {}
        Err(e) => {
            panic!("Otel component did not exit on CA cert change: {e:?}");
        }
    }

    // TODO: troubleshoot why calling `otel_component.shutdown()` blocks test execution here.

    filtered_target.shutdown_tx.send(()).await.unwrap();
    unfiltered_target.shutdown_tx.send(()).await.unwrap();
    let () = self_signed_cert.cleanup();
}

#[allow(clippy::too_many_arguments)]
async fn run_tests(
    filtered_metrics_rx: Receiver<ExportMetricsServiceRequest>,
    filtered_metrics_with_tls_rx: Receiver<ExportMetricsServiceRequest>,
    filtered_logs_rx: Receiver<ExportLogsServiceRequest>,
    filtered_logs_with_tls_rx: Receiver<ExportLogsServiceRequest>,

    unfiltered_metrics_rx: Receiver<ExportMetricsServiceRequest>,
    unfiltered_metrics_with_tls_rx: Receiver<ExportMetricsServiceRequest>,
    unfiltered_logs_rx: Receiver<ExportLogsServiceRequest>,
    unfiltered_logs_with_tls_rx: Receiver<ExportLogsServiceRequest>,
    sample_attribute: &Attribute,
    prom_port: u16,
) {
    let meter = global::meter_provider().meter("end_to_end_test");
    let test_counter = meter.u64_counter("test_counter").init();
    test_counter.add(1, &[]);

    // validate that the metric is exported to the OTLP targets
    validate_test_counter(
        filtered_metrics_rx,
        sample_attribute,
        AggregationTemporality::Cumulative,
    )
    .await;
    validate_test_counter(
        filtered_metrics_with_tls_rx,
        sample_attribute,
        AggregationTemporality::Cumulative,
    )
    .await;

    validate_test_counter(
        unfiltered_metrics_rx,
        sample_attribute,
        AggregationTemporality::Delta,
    )
    .await;
    validate_test_counter(
        unfiltered_metrics_with_tls_rx,
        sample_attribute,
        AggregationTemporality::Delta,
    )
    .await;

    // validate the metric is available at the prom endpoint
    validate_test_counter_prometheus(prom_port).await;

    // test logs

    let trace_log = "this is a trace debug message";
    let debug_log = "this is a test debug message";
    let info_log = "this is a test info message";
    let warn_log = "this is a test warn message";
    let error_log = "this is a test error message";

    trace!("{trace_log}"); // shouldn't be logged by otel-lib
    debug!("{debug_log}"); // shouldn't be logged by otel-lib
    info!("{info_log}"); // shouldn't be logged by otel-lib
    warn!("{warn_log}"); // should be logged by otel-lib and exported to the unfiltered target
    error!("{error_log}"); // should be logged by otel-lib and exported to both OTLP targets

    // Check that the filtered target only receives the error log
    validate_filtered_logs(filtered_logs_rx, error_log.to_owned()).await;
    validate_filtered_logs(filtered_logs_with_tls_rx, error_log.to_owned()).await;

    // Check that the unfiltered target receives both warn and error log
    validate_unfiltered_logs(
        unfiltered_logs_rx,
        error_log.to_owned(),
        warn_log.to_owned(),
    )
    .await;
    validate_unfiltered_logs(
        unfiltered_logs_with_tls_rx,
        error_log.to_owned(),
        warn_log.to_owned(),
    )
    .await;
}

fn touch_file(path: &PathBuf) {
    OpenOptions::new().write(true).open(path).unwrap();
    let now = SystemTime::now();
    let mod_time = FileTime::from_system_time(now);
    set_file_mtime(path, mod_time).unwrap();
}

fn get_log_messages(logs_export_request: &ExportLogsServiceRequest) -> Vec<Value> {
    let mut log_messages = vec![];
    for resource_log in &logs_export_request.resource_logs {
        for scope_log in &resource_log.scope_logs {
            for log_record in &scope_log.log_records {
                let log_message = log_record.clone().body.unwrap().value.unwrap();
                log_messages.push(log_message);
            }
        }
    }
    log_messages
}

fn get_counter(
    metrics_export_request: &ExportMetricsServiceRequest,
) -> (String, i64, AggregationTemporality) {
    let resource_metric = &metrics_export_request.resource_metrics.first().unwrap();
    let scope_metric = &resource_metric.scope_metrics.first().unwrap();
    let metric = &scope_metric.metrics.first().unwrap();
    match &metric.data.clone().unwrap() {
        opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(sum) => {
            let num_value = sum.data_points.first();
            let num_value = num_value.unwrap().value.unwrap();
            match num_value {
                opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(_) => {
                    panic!("expected int value")
                }
                opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsInt(
                    intvalue,
                ) => (metric.name.clone(), intvalue, sum.aggregation_temporality()),
            }
        }
        _ => panic!("unexpected metric type"),
    }
}

fn get_resource_attributes(metrics_export_request: &ExportMetricsServiceRequest) -> Vec<KeyValue> {
    metrics_export_request
        .resource_metrics
        .first()
        .unwrap()
        .resource
        .clone()
        .unwrap()
        .attributes
}

async fn validate_test_counter(
    mut metrics_rx: Receiver<ExportMetricsServiceRequest>,
    sample_attribute: &Attribute,
    export_temporality: AggregationTemporality,
) {
    // validate that the metric is exported to the OTLP target
    let metrics_export_request = timeout(Duration::from_secs(2), metrics_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let (name, value, temporality) = get_counter(&metrics_export_request);
    assert_eq!(name, "test_counter");
    assert_eq!(value, 1);
    assert_eq!(export_temporality, temporality);

    let kv = KeyValue {
        key: sample_attribute.key.clone(),
        value: Some(AnyValue {
            value: Some(StringValue(sample_attribute.value.clone())),
        }),
    };
    //validate resource attribute
    assert!(get_resource_attributes(&metrics_export_request).contains(&kv));
}

async fn validate_test_counter_prometheus(prom_port: u16) {
    // validate the metric is available at the prom endpoint
    let body = reqwest::get(format!("http://127.0.0.1:{prom_port}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    assert!(
        body.contains("test_counter_total{otel_scope_name=\"end_to_end_test\"} 1"),
        "did not find expected metric test_counter in server response",
    );
}

async fn validate_filtered_logs(
    mut filtered_logs_rx: Receiver<ExportLogsServiceRequest>,
    error_log: String,
) {
    // Check that the filtered target only receives the error log
    let logs_export_request = timeout(Duration::from_secs(2), filtered_logs_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let mut log_messages = get_log_messages(&logs_export_request);
    assert_eq!(log_messages.len(), 1);
    assert_eq!(log_messages.pop().unwrap(), Value::StringValue(error_log));

    // check that no more messages are received.
    assert!(timeout(Duration::from_secs(2), filtered_logs_rx.recv())
        .await
        .is_err());
}

async fn validate_unfiltered_logs(
    mut logs_rx: Receiver<ExportLogsServiceRequest>,
    error_log: String,
    warn_log: String,
) {
    let logs_export_request = timeout(Duration::from_secs(2), logs_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let mut log_messages = get_log_messages(&logs_export_request);
    // If not all logs received, wait for the next export.
    if log_messages.len() == 1 {
        let logs_export_request = timeout(Duration::from_secs(2), logs_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let messages = get_log_messages(&logs_export_request);
        for message in messages {
            log_messages.push(message);
        }
    }
    assert_eq!(log_messages.len(), 2);
    assert_eq!(log_messages.pop().unwrap(), Value::StringValue(error_log));
    assert_eq!(log_messages.pop().unwrap(), Value::StringValue(warn_log));

    // check that no more messages are received.
    assert!(timeout(Duration::from_secs(2), logs_rx.recv())
        .await
        .is_err());
}
