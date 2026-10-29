// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::{
    io::{self, Write},
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use chrono::{DateTime, Utc};
use opentelemetry::{Array, KeyValue, Value as AttributeValue};
use opentelemetry_sdk::{
    error::{OTelSdkError, OTelSdkResult},
    metrics::{
        data::{AggregatedMetrics, Exemplar, ExponentialBucket, MetricData, ResourceMetrics},
        exporter::PushMetricExporter,
        Temporality,
    },
};
use serde::Serialize;
use serde_json::{json, Value};

pub(crate) struct JsonMetricExporter {
    writer: Mutex<Box<dyn Write + Send>>,
    shutdown: AtomicBool,
}

impl JsonMetricExporter {
    pub(crate) fn stdout() -> Self {
        Self {
            writer: Mutex::new(Box::new(io::stdout())),
            shutdown: AtomicBool::new(false),
        }
    }
}

impl PushMetricExporter for JsonMetricExporter {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(OTelSdkError::AlreadyShutdown);
        }
        let mut writer = self
            .writer
            .lock()
            .map_err(|err| OTelSdkError::InternalFailure(err.to_string()))?;
        serde_json::to_writer_pretty(&mut *writer, &metrics_json(metrics))
            .map_err(|err| OTelSdkError::InternalFailure(err.to_string()))?;
        writeln!(writer).map_err(|err| OTelSdkError::InternalFailure(err.to_string()))
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.writer
            .lock()
            .map_err(|err| OTelSdkError::InternalFailure(err.to_string()))?
            .flush()
            .map_err(|err| OTelSdkError::InternalFailure(err.to_string()))
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        self.shutdown.store(true, Ordering::Release);
        self.force_flush()
    }

    fn temporality(&self) -> Temporality {
        Temporality::Cumulative
    }
}

fn attribute_value(value: &AttributeValue) -> Value {
    match value {
        AttributeValue::Bool(value) => json!({ "boolValue": value }),
        AttributeValue::I64(value) => json!({ "intValue": value }),
        AttributeValue::F64(value) => json!({ "doubleValue": value }),
        AttributeValue::String(value) => json!({ "stringValue": value.as_str() }),
        AttributeValue::Array(value) => {
            let values = match value {
                Array::Bool(values) => values
                    .iter()
                    .map(|value| json!({"boolValue": value}))
                    .collect::<Vec<_>>(),
                Array::I64(values) => values
                    .iter()
                    .map(|value| json!({"intValue": value}))
                    .collect(),
                Array::F64(values) => values
                    .iter()
                    .map(|value| json!({"doubleValue": value}))
                    .collect(),
                Array::String(values) => values
                    .iter()
                    .map(|value| json!({"stringValue": value.as_str()}))
                    .collect(),
                _ => {
                    eprintln!("unsupported metric attribute array: {value:?}");
                    vec![json!({"stringValue": format!("{value:?}")})]
                }
            };
            json!({ "arrayValue": values })
        }
        _ => {
            eprintln!("unsupported metric attribute value: {value:?}");
            json!({ "stringValue": format!("{value:?}") })
        }
    }
}

fn attributes<'a>(values: impl Iterator<Item = &'a KeyValue>) -> Vec<Value> {
    values
        .map(|kv| json!({ "key": kv.key.as_str(), "value": attribute_value(&kv.value) }))
        .collect()
}

fn time_values(time: Option<SystemTime>) -> (Value, Value) {
    match time {
        Some(time) => {
            let duration = time.duration_since(UNIX_EPOCH).unwrap_or_default();
            let time = UNIX_EPOCH + duration;
            let date: DateTime<Utc> = time.into();
            (
                json!(date.format("%Y-%m-%d %H:%M:%S.%3f").to_string()),
                json!(duration.as_nanos()),
            )
        }
        None => (Value::Null, Value::Null),
    }
}

fn exemplars<T: Copy + Serialize>(
    items: impl Iterator<Item = impl std::borrow::Borrow<Exemplar<T>>>,
) -> Vec<Value> {
    items
        .map(|item| {
            let item = item.borrow();
            let (time, nanos) = time_values(Some(item.time()));
            json!({
                "filteredAttributes": attributes(item.filtered_attributes()),
                "timeUnixNano": nanos, "time": time, "value": item.value,
                "spanId": format!("{:016x}", u64::from_be_bytes(*item.span_id())),
                "traceId": format!("{:032x}", u128::from_be_bytes(*item.trace_id())),
            })
        })
        .collect()
}

fn number_point<T: Copy + Serialize>(
    attrs: impl Iterator<Item = impl std::borrow::Borrow<KeyValue>>,
    start: Option<SystemTime>,
    time: SystemTime,
    value: T,
    samples: &[Value],
) -> Value {
    let attrs: Vec<_> = attrs
        .map(|item| {
            let kv = item.borrow();
            json!({ "key": kv.key.as_str(), "value": attribute_value(&kv.value) })
        })
        .collect();
    let (start_time, start_nanos) = time_values(start);
    let (time, nanos) = time_values(Some(time));
    let mut point = json!({
        "attributes": attrs, "startTime": start_time, "time": time,
        "startTimeUnixNano": start_nanos, "timeUnixNano": nanos, "value": value,
    });
    if !samples.is_empty() {
        point["exemplars"] = json!(samples);
    }
    point
}

fn bucket(bucket: &ExponentialBucket) -> Value {
    json!({ "offset": bucket.offset(), "bucket_counts": bucket.counts().collect::<Vec<_>>() })
}

fn metric_data<T: Copy + Serialize>(data: &MetricData<T>) -> Value {
    match data {
        MetricData::Gauge(gauge) => json!({ "gauge": {
            "dataPoints": gauge.data_points().map(|p| number_point(
                p.attributes(), gauge.start_time(), gauge.time(), p.value(), &exemplars(p.exemplars())
            )).collect::<Vec<_>>()
        }}),
        MetricData::Sum(sum) => json!({ "sum": {
            "dataPoints": sum.data_points().map(|p| number_point(
                p.attributes(), Some(sum.start_time()), sum.time(), p.value(), &exemplars(p.exemplars())
            )).collect::<Vec<_>>(),
            "aggregationTemporality": format!("{:?}", sum.temporality()),
            "isMonotonic": sum.is_monotonic(),
        }}),
        MetricData::Histogram(histogram) => {
            let (start_time, start_nanos) = time_values(Some(histogram.start_time()));
            let (time, nanos) = time_values(Some(histogram.time()));
            json!({ "histogram": {
                "dataPoints": histogram.data_points().map(|p| json!({
                    "attributes": attributes(p.attributes()),
                    "startTimeUnixNano": start_nanos, "timeUnixNano": nanos,
                    "startTime": start_time, "time": time, "count": p.count(),
                    "explicitBounds": p.bounds().collect::<Vec<_>>(),
                    "bucketCounts": p.bucket_counts().collect::<Vec<_>>(),
                    "min": p.min(), "max": p.max(), "sum": p.sum(),
                    "exemplars": exemplars(p.exemplars()), "flags": 0,
                })).collect::<Vec<_>>(),
                "aggregationTemporality": format!("{:?}", histogram.temporality()),
            }})
        }
        MetricData::ExponentialHistogram(histogram) => {
            let (start_time, start_nanos) = time_values(Some(histogram.start_time()));
            let (time, nanos) = time_values(Some(histogram.time()));
            json!({ "exponentialHistogram": {
                "dataPoints": histogram.data_points().map(|p| json!({
                    "attributes": attributes(p.attributes()),
                    "startTimeUnixNano": start_nanos, "timeUnixNano": nanos,
                    "startTime": start_time, "time": time, "count": p.count(),
                    "min": p.min(), "max": p.max(), "sum": p.sum(),
                    "scale": p.scale(), "zeroCount": p.zero_count(),
                    "positive": bucket(p.positive_bucket()), "negative": bucket(p.negative_bucket()),
                    "zeroThreshold": p.zero_threshold(), "exemplars": exemplars(p.exemplars()), "flags": 0,
                })).collect::<Vec<_>>(),
                "aggregationTemporality": format!("{:?}", histogram.temporality()),
            }})
        }
    }
}

fn metrics_json(metrics: &ResourceMetrics) -> Value {
    let resource = metrics.resource();
    let resource_attributes = resource
        .iter()
        .map(|(key, value)| {
            json!({
                "key": key.as_str(), "value": attribute_value(value)
            })
        })
        .collect::<Vec<_>>();
    let scopes = metrics
        .scope_metrics()
        .map(|scope_metrics| {
            let scope = scope_metrics.scope();
            let mut scope_json = json!({});
            if !scope.name().is_empty() {
                scope_json["name"] = json!(scope.name());
            }
            if let Some(version) = scope.version() {
                scope_json["version"] = json!(version);
            }
            let attrs = attributes(scope.attributes());
            if !attrs.is_empty() {
                scope_json["attributes"] = json!(attrs);
            }
            let values = scope_metrics
                .metrics()
                .map(|metric| {
                    let mut value = json!({"name": metric.name()});
                    if !metric.description().is_empty() {
                        value["description"] = json!(metric.description());
                    }
                    if !metric.unit().is_empty() {
                        value["unit"] = json!(metric.unit());
                    }
                    let data = match metric.data() {
                        AggregatedMetrics::F64(data) => metric_data(data),
                        AggregatedMetrics::I64(data) => metric_data(data),
                        AggregatedMetrics::U64(data) => metric_data(data),
                    };
                    if let (Value::Object(target), Value::Object(data)) = (&mut value, data) {
                        target.extend(data);
                    }
                    value
                })
                .collect::<Vec<_>>();
            let mut value = json!({"scope": scope_json, "metrics": values});
            if let Some(schema_url) = scope.schema_url() {
                value["schemaUrl"] = json!(schema_url);
            }
            value
        })
        .collect::<Vec<_>>();
    let mut result =
        json!({ "resource": { "attributes": resource_attributes }, "scopeMetrics": scopes });
    if let Some(schema_url) = resource.schema_url() {
        result["schemaUrl"] = json!(schema_url);
    }
    json!({ "resourceMetrics": result })
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::{
        metrics::{PeriodicReader, SdkMeterProvider},
        Resource,
    };
    use std::sync::Arc;

    struct Buffer(Arc<Mutex<Vec<u8>>>);

    impl Write for Buffer {
        fn write(&mut self, data: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().write(data)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn exports_pretty_json_with_resource_and_metric_data() {
        let output = Arc::new(Mutex::new(Vec::new()));
        let exporter = JsonMetricExporter {
            writer: Mutex::new(Box::new(Buffer(output.clone()))),
            shutdown: AtomicBool::new(false),
        };
        let provider = SdkMeterProvider::builder()
            .with_resource(
                Resource::builder_empty()
                    .with_attribute(KeyValue::new("service.name", "test"))
                    .build(),
            )
            .with_reader(
                PeriodicReader::builder(exporter)
                    .with_interval(Duration::from_secs(3_599))
                    .build(),
            )
            .build();
        let meter = provider.meter("scope");
        meter
            .u64_counter("requests")
            .build()
            .add(7, &[KeyValue::new("route", "home")]);
        meter.u64_histogram("sizes").build().record(25, &[]);
        meter.u64_gauge("active").build().record(3, &[]);
        provider.force_flush().unwrap();

        let bytes = output.lock().unwrap().clone();
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(text.contains("\n  \"resourceMetrics\""));
        let data: Value = serde_json::from_slice(&bytes).unwrap();
        let resource = &data["resourceMetrics"];
        assert!(resource["resource"]["attributes"]
            .as_array()
            .unwrap()
            .contains(&json!({"key": "service.name", "value": {"stringValue": "test"}})));
        let scope = &resource["scopeMetrics"][0];
        assert_eq!(scope["scope"]["name"], "scope");
        let metrics = scope["metrics"].as_array().unwrap();
        let counter = metrics.iter().find(|m| m["name"] == "requests").unwrap();
        assert_eq!(counter["sum"]["aggregationTemporality"], "Cumulative");
        assert_eq!(counter["sum"]["dataPoints"][0]["value"], 7);
        assert_eq!(
            counter["sum"]["dataPoints"][0]["attributes"][0],
            json!({"key": "route", "value": {"stringValue": "home"}})
        );
        assert!(counter["sum"]["dataPoints"][0]["timeUnixNano"].is_number());
        let histogram = metrics.iter().find(|m| m["name"] == "sizes").unwrap();
        assert_eq!(histogram["histogram"]["dataPoints"][0]["count"], 1);
        assert_eq!(histogram["histogram"]["dataPoints"][0]["sum"], 25);
        let gauge = metrics.iter().find(|m| m["name"] == "active").unwrap();
        assert_eq!(gauge["gauge"]["dataPoints"][0]["value"], 3);
        provider.shutdown().unwrap();
    }

    #[test]
    fn reports_output_errors() {
        struct FailingWriter;

        impl Write for FailingWriter {
            fn write(&mut self, _data: &[u8]) -> io::Result<usize> {
                Err(io::Error::other("write failed"))
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let exporter = JsonMetricExporter {
            writer: Mutex::new(Box::new(FailingWriter)),
            shutdown: AtomicBool::new(false),
        };
        let result = futures::executor::block_on(exporter.export(&ResourceMetrics::default()));
        assert!(
            matches!(result, Err(OTelSdkError::InternalFailure(message)) if message.contains("write failed"))
        );
    }
}
