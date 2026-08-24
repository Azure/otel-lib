// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::{
    fmt, io,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use opentelemetry_sdk::{
    error::{OTelSdkError, OTelSdkResult},
    metrics::{data::ResourceMetrics, exporter::PushMetricExporter, Temporality},
};

/// Writes each metric collection as a JSON document, preserving the existing
/// machine-readable stdout contract across the OpenTelemetry SDK upgrade.
pub(crate) struct JsonMetricExporter {
    is_shutdown: AtomicBool,
}

impl Default for JsonMetricExporter {
    fn default() -> Self {
        Self {
            is_shutdown: AtomicBool::new(false),
        }
    }
}

impl fmt::Debug for JsonMetricExporter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("JsonMetricExporter")
    }
}

impl PushMetricExporter for JsonMetricExporter {
    fn export(
        &self,
        metrics: &ResourceMetrics,
    ) -> impl std::future::Future<Output = OTelSdkResult> + Send {
        let result = if self.is_shutdown.load(Ordering::SeqCst) {
            Err(OTelSdkError::AlreadyShutdown)
        } else {
            let output = serde_json::json!({
                "resource_metrics": format!("{metrics:#?}"),
            });
            let result = serde_json::to_writer_pretty(io::stdout().lock(), &output)
                .map_err(|err| OTelSdkError::InternalFailure(err.to_string()));
            println!();
            result
        };
        std::future::ready(result)
    }

    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        self.is_shutdown.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn temporality(&self) -> Temporality {
        Temporality::Cumulative
    }
}
