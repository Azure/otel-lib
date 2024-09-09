// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![deny(rust_2018_idioms)]
#![warn(clippy::all, clippy::pedantic)]

use std::time::Duration;

use log::{debug, error, info, trace, warn};
use mocks::MockServer;
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

use tokio::sync::mpsc::Receiver;
use tokio::time::timeout;

mod mocks;

#[tokio::test]
// This test does the following
// 1) setup two mock otlp servers, one for filtered logs and the other for unfiltered logs
// 2) configure the otel-lib to
//    a) log at the `warn` level
//    b) export logs to the filtered server the `error` level.
//    c) export all logs to the unfiltered server
//    d) export metrics to the filtered server
//    e) setup up a prometheus endpoint
// 3) add a metric, and create some logs
// 4) validate that metrics are exported to both otlp servers and at avaialable at the prometheus endpoint
// 5) validate that `warn` and `error` logs are exported to the unfiltered server and only `error` logs are exported to the filtered server.

async fn end_to_end_test() {
    // Setup mock otlp server for filtered logs
    let filtered_target = MockServer::new(4317);
    tokio::spawn(async {
        filtered_target.server.run().await;
    });

    // Setup mock otlp server for unfiltered logs
    let unfiltered_target = MockServer::new(4318);
    tokio::spawn(async {
        unfiltered_target.server.run().await;
    });

    // Setup Otel-lib
    let prometheus_config = Some(Prometheus { port: 9090 });

    let metric_targets = vec![
        MetricsExportTarget {
            url: filtered_target.endpoint.clone(),
            interval_secs: 1,
            timeout: 5,
            temporality: Some(Temporality::Cumulative),
            ca_cert_path: None,
        },
        MetricsExportTarget {
            url: unfiltered_target.endpoint.clone(),
            interval_secs: 1,
            timeout: 5,
            temporality: Some(Temporality::Delta),
            ca_cert_path: None,
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
            url: unfiltered_target.endpoint,
            interval_secs: 1,
            timeout: 5,
            export_severity: None,
            ca_cert_path: None,
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

    let otel_component = Otel::new(config);
    let otel_long_running_task = otel_component.run();
    let run_tests_task = run_tests(
        filtered_target.metrics_rx,
        filtered_target.logs_rx,
        unfiltered_target.metrics_rx,
        unfiltered_target.logs_rx,
        &sample_attribute,
    );

    // run tests
    tokio::select! {
        () = otel_long_running_task => {
            panic!("Otel component ended unexpectedly");
        },
        () = run_tests_task => {
        }
    }

    // TODO: troubleshoot why calling `otel_component.shutdown()` blocks test execution here.

    filtered_target.shutdown_tx.send(()).await.unwrap();
    unfiltered_target.shutdown_tx.send(()).await.unwrap();
}

async fn run_tests(
    mut filtered_metrics_rx: Receiver<ExportMetricsServiceRequest>,
    mut filtered_logs_rx: Receiver<ExportLogsServiceRequest>,
    mut unfiltered_metrics_rx: Receiver<ExportMetricsServiceRequest>,
    mut unfiltered_logs_rx: Receiver<ExportLogsServiceRequest>,
    sample_attribute: &Attribute,
) {
    let meter = global::meter_provider().meter("end_to_end_test");
    let test_counter = meter.u64_counter("test_counter").init();
    test_counter.add(1, &[]);

    // validate that the metric is exported to the OTLP target
    let metrics_export_request = timeout(Duration::from_secs(2), filtered_metrics_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let (name, value, temporality) = get_counter(&metrics_export_request);
    assert_eq!(name, "test_counter");
    assert_eq!(value, 1);
    assert_eq!(AggregationTemporality::Cumulative, temporality);

    let kv = KeyValue {
        key: sample_attribute.key.clone(),
        value: Some(AnyValue {
            value: Some(StringValue(sample_attribute.value.clone())),
        }),
    };
    //validate resource attribute
    assert!(get_resource_attributes(&metrics_export_request).contains(&kv));

    // validate that the metric is exported to the unfiltered OTLP target
    let metrics_export_request = timeout(Duration::from_secs(2), unfiltered_metrics_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let (name, value, temporality) = get_counter(&metrics_export_request);
    assert_eq!(name, "test_counter");
    assert_eq!(value, 1);
    assert_eq!(AggregationTemporality::Delta, temporality);
    assert!(get_resource_attributes(&metrics_export_request).contains(&kv));

    // validate the metric is available at the prom endpoint
    let body = reqwest::get("http://127.0.0.1:9090/metrics")
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    assert!(
        body.contains("test_counter_total{otel_scope_name=\"end_to_end_test\"} 1"),
        "did not find expected metric test_counter in server response",
    );

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
    let logs_export_request = timeout(Duration::from_secs(2), filtered_logs_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let mut log_messages = get_log_messages(&logs_export_request);
    assert_eq!(log_messages.len(), 1);
    assert_eq!(
        log_messages.pop().unwrap(),
        Value::StringValue(error_log.to_owned())
    );

    // check that no more messages are received.
    assert!(timeout(Duration::from_secs(2), filtered_logs_rx.recv())
        .await
        .is_err());

    let logs_export_request = timeout(Duration::from_secs(2), unfiltered_logs_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let mut log_messages = get_log_messages(&logs_export_request);
    // If not all logs received, wait for the next export.
    if log_messages.len() == 1 {
        let logs_export_request = timeout(Duration::from_secs(2), unfiltered_logs_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let messages = get_log_messages(&logs_export_request);
        for message in messages {
            log_messages.push(message);
        }
    }
    assert_eq!(log_messages.len(), 2);
    assert_eq!(
        log_messages.pop().unwrap(),
        Value::StringValue(error_log.to_owned())
    );
    assert_eq!(
        log_messages.pop().unwrap(),
        Value::StringValue(warn_log.to_owned())
    );

    // check that no more messages are received.
    assert!(timeout(Duration::from_secs(2), unfiltered_logs_rx.recv())
        .await
        .is_err());
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
