// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// This implementation is a slight adjustment of the code at
// [BatchLogProcessor](https://github.com/open-telemetry/opentelemetry-rust/blob/b933bdd82dadbadedb42a1f572caba1e0b8b9391/opentelemetry-sdk/src/logs/log_processor.rs#L154)
//
// I've opened an issue on the opentelemetry_rust SDK repo: [1881](https://github.com/open-telemetry/opentelemetry-rust/issues/1881).
// If that issue is accepted and addressed, this implementation will no longer be required.

use crate::runtime::{RuntimeChannel, TrySend};
use futures_channel::oneshot;
use futures_util::{
    future::{self, Either},
    {pin_mut, stream, StreamExt as _},
};

use opentelemetry::{
    global,
    logs::{AnyValue, LogError, LogResult, Severity},
};
use opentelemetry_sdk::{
    export::logs::{ExportResult, LogData, LogExporter},
    logs::LogProcessor,
    Resource,
};

use std::{
    borrow::Cow,
    fmt::{self, Debug, Formatter},
    sync::Arc,
    time::Duration,
};

/// Default delay interval between two consecutive exports.
const OTEL_BLRP_SCHEDULE_DELAY_DEFAULT: u64 = 1_000;
/// Maximum allowed time to export data.
const OTEL_BLRP_EXPORT_TIMEOUT_DEFAULT: u64 = 30_000;
/// Default maximum queue size.
const OTEL_BLRP_MAX_QUEUE_SIZE_DEFAULT: usize = 2_048;
/// Default maximum batch size.
const OTEL_BLRP_MAX_EXPORT_BATCH_SIZE_DEFAULT: usize = 512;

/// A [`LogProcessor`] that asynchronously buffers log records, applies a severity filter, and exports
/// them at a pre-configured interval.
pub struct FilteredBatchLogProcessor<R: RuntimeChannel> {
    message_sender: R::Sender<BatchMessage>,
}

impl<R: RuntimeChannel> Debug for FilteredBatchLogProcessor<R> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("FilteredBatchLogProcessor")
            .field("message_sender", &self.message_sender)
            .finish()
    }
}

impl<R: RuntimeChannel> LogProcessor for FilteredBatchLogProcessor<R> {
    fn emit(&self, data: &mut LogData) {
        let result = self
            .message_sender
            .try_send(BatchMessage::ExportLog(data.clone()));

        if let Err(err) = result {
            global::handle_error(LogError::Other(err.into()));
        }
    }

    fn force_flush(&self) -> LogResult<()> {
        let (res_sender, res_receiver) = oneshot::channel();
        self.message_sender
            .try_send(BatchMessage::Flush(Some(res_sender)))
            .map_err(|err| LogError::Other(err.into()))?;

        futures_executor::block_on(res_receiver)
            .map_err(|err| LogError::Other(err.into()))
            .and_then(std::convert::identity)
    }

    fn shutdown(&self) -> LogResult<()> {
        let (res_sender, res_receiver) = oneshot::channel();
        self.message_sender
            .try_send(BatchMessage::Shutdown(res_sender))
            .map_err(|err| LogError::Other(err.into()))?;

        futures_executor::block_on(res_receiver)
            .map_err(|err| LogError::Other(err.into()))
            .and_then(std::convert::identity)
    }

    fn set_resource(&self, resource: &Resource) {
        let resource = Arc::new(resource.clone());
        let _ = self
            .message_sender
            .try_send(BatchMessage::SetResource(resource));
    }

    fn event_enabled(
        &self,
        _level: opentelemetry::logs::Severity,
        _target: &str,
        _name: &str,
    ) -> bool {
        true
    }
}

impl<R: RuntimeChannel> FilteredBatchLogProcessor<R> {
    pub(crate) fn new(
        mut exporter: Box<dyn LogExporter>,
        config: FilteredBatchConfig,
        runtime: &R,
    ) -> Self {
        let (message_sender, message_receiver) =
            runtime.batch_message_channel(config.max_queue_size);
        let ticker = runtime
            .interval(config.scheduled_delay)
            .map(|_| BatchMessage::Flush(None));
        let timeout_runtime = runtime.clone();

        // Spawn worker process via user-defined spawn function.
        runtime.spawn(Box::pin(async move {
            let mut logs = Vec::new();
            let mut messages = Box::pin(stream::select(message_receiver, ticker));

            while let Some(message) = messages.next().await {
                match message {
                    BatchMessage::ExportLog(log) => {
                        // Apply filtering using the dedicated function
                        if !should_export_log(&log, &config) {
                            continue; // skip logs that do not match the filters
                        }

                        logs.push(Cow::Owned(log));

                        if logs.len() == config.max_export_batch_size {
                            let result = export_with_timeout(
                                config.max_export_timeout,
                                exporter.as_mut(),
                                &timeout_runtime,
                                logs.split_off(0),
                            )
                            .await;

                            if let Err(err) = result {
                                global::handle_error(err);
                            }
                        }
                    }
                    // Log batch interval time reached or a force flush has been invoked, export current spans.
                    BatchMessage::Flush(res_channel) => {
                        let result = export_with_timeout(
                            config.max_export_timeout,
                            exporter.as_mut(),
                            &timeout_runtime,
                            logs.split_off(0),
                        )
                        .await;

                        if let Some(channel) = res_channel {
                            if let Err(result) = channel.send(result) {
                                global::handle_error(LogError::from(format!(
                                    "failed to send flush result: {result:?}"
                                )));
                            }
                        } else if let Err(err) = result {
                            global::handle_error(err);
                        }
                    }
                    // Stream has terminated or processor is shutdown, return to finish execution.
                    BatchMessage::Shutdown(ch) => {
                        let result = export_with_timeout(
                            config.max_export_timeout,
                            exporter.as_mut(),
                            &timeout_runtime,
                            logs.split_off(0),
                        )
                        .await;

                        exporter.shutdown();

                        if let Err(result) = ch.send(result) {
                            global::handle_error(LogError::from(format!(
                                "failed to send batch processor shutdown result: {result:?}"
                            )));
                        }

                        break;
                    }

                    // propagate the resource
                    BatchMessage::SetResource(resource) => {
                        exporter.set_resource(&resource);
                    }
                }
            }
        }));

        // Return batch processor with link to worker
        FilteredBatchLogProcessor { message_sender }
    }

    /// Create a new batch processor builder
    pub(crate) fn builder<E>(exporter: E, runtime: R) -> FilteredBatchLogProcessorBuilder<E, R>
    where
        E: LogExporter,
    {
        FilteredBatchLogProcessorBuilder {
            exporter,
            batch_config: FilteredBatchConfig::default(),
            runtime,
        }
    }
}

/// Check if a log should be exported based on severity and target filtering rules
fn should_export_log(log: &LogData, config: &FilteredBatchConfig) -> bool {
    // Apply severity filtering
    let severity_matches = log
        .record
        .severity_number
        .is_some_and(|sev| sev >= config.export_severity);

    if !severity_matches {
        return false; // skip logs that do not match the severity filter
    }

    // Apply target filtering
    let target_matches = if let Some(ref target_filters) = config.target_filters {
        // Check if the log has a "target" attribute that matches any of our filters
        log.record.attributes.as_ref().is_some_and(|attrs| {
            attrs.iter().any(|(key, value)| {
                if key.as_str() == "target" {
                    // Extract string value from AnyValue by matching on the enum
                    if let AnyValue::String(target_value) = value {
                        let target_str = target_value.as_str();
                        // Check if target matches any of the configured filters (exact match only)
                        target_filters.iter().any(|filter| target_str == filter)
                    } else {
                        false
                    }
                } else {
                    false
                }
            })
        })
    } else {
        true // if no target filters specified, accept all logs
    };

    target_matches
}

async fn export_with_timeout<R, E>(
    time_out: Duration,
    exporter: &mut E,
    runtime: &R,
    batch: Vec<Cow<'_, LogData>>,
) -> ExportResult
where
    R: RuntimeChannel,
    E: LogExporter + ?Sized,
{
    if batch.is_empty() {
        return Ok(());
    }

    let export = exporter.export(batch);
    let delay = runtime.delay(time_out);
    pin_mut!(export);
    pin_mut!(delay);
    match future::select(export, delay).await {
        Either::Left((export_res, _)) => export_res,
        Either::Right((_, _)) => ExportResult::Err(LogError::ExportTimedOut(time_out)),
    }
}

#[derive(Debug, Clone)]
pub(crate) struct FilteredBatchConfig {
    /// The maximum queue size to buffer logs for delayed processing. If the
    /// queue gets full it drops the logs. The default value of is 2048.
    pub max_queue_size: usize,

    /// The delay interval in milliseconds between two consecutive processing
    /// of batches. The default value is 1 second.
    pub scheduled_delay: Duration,

    /// The maximum number of logs to process in a single batch. If there are
    /// more than one batch worth of logs then it processes multiple batches
    /// of logs one batch after the other without any delay. The default value
    /// is 512.
    pub max_export_batch_size: usize,

    /// The maximum duration to export a batch of data.
    pub max_export_timeout: Duration,

    /// export level - levels >= which to export
    pub export_severity: Severity,

    /// target filters - only export logs from targets matching these exact names. If None, exports all logs.
    pub target_filters: Option<Vec<String>>,
}

impl Default for FilteredBatchConfig {
    fn default() -> Self {
        Self {
            max_queue_size: OTEL_BLRP_MAX_QUEUE_SIZE_DEFAULT,
            scheduled_delay: Duration::from_millis(OTEL_BLRP_SCHEDULE_DELAY_DEFAULT),
            max_export_batch_size: OTEL_BLRP_MAX_EXPORT_BATCH_SIZE_DEFAULT,
            max_export_timeout: Duration::from_millis(OTEL_BLRP_EXPORT_TIMEOUT_DEFAULT),
            export_severity: Severity::Error,
            target_filters: None,
        }
    }
}

/// A builder for creating [`FilteredBatchLogProcessor`] instances.
///
#[derive(Debug)]
pub(crate) struct FilteredBatchLogProcessorBuilder<E, R> {
    exporter: E,
    batch_config: FilteredBatchConfig,
    runtime: R,
}

impl<E, R> FilteredBatchLogProcessorBuilder<E, R>
where
    E: LogExporter + 'static,
    R: RuntimeChannel,
{
    /// Set the `FilteredBatchConfig` for [`BatchLogProcessorBuilder`]
    pub(crate) fn with_batch_config(self, config: FilteredBatchConfig) -> Self {
        FilteredBatchLogProcessorBuilder {
            batch_config: config,
            ..self
        }
    }

    /// Build a batch processor
    pub(crate) fn build(self) -> FilteredBatchLogProcessor<R> {
        FilteredBatchLogProcessor::new(Box::new(self.exporter), self.batch_config, &self.runtime)
    }
}

/// Messages sent between application thread and batch log processor's work thread.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum BatchMessage {
    /// Export logs, usually called when the log is emitted.
    ExportLog(LogData),
    /// Flush the current buffer to the backend, it can be triggered by
    /// pre configured interval or a call to `force_push` function.
    Flush(Option<oneshot::Sender<ExportResult>>),
    /// Shut down the worker thread, push all logs in buffer to the backend.
    Shutdown(oneshot::Sender<ExportResult>),
    /// Set the resource for the exporter.
    SetResource(Arc<Resource>),
}

#[cfg(test)]
#[allow(clippy::default_trait_access)]
mod tests {
    use super::*;
    use opentelemetry::{logs::AnyValue, logs::LogRecord};
    use opentelemetry_sdk::logs::LogRecord as SdkLogRecord;

    /// Helper function to create a `LogData` with specified severity and target
    fn create_log_data(severity: Option<Severity>, target: Option<&str>) -> LogData {
        let mut log_record = SdkLogRecord::default();

        if let Some(sev) = severity {
            log_record.set_severity_number(sev);
        }

        if let Some(target_str) = target {
            log_record.add_attribute("target", AnyValue::from(target_str.to_string()));
        }

        LogData {
            record: log_record,
            instrumentation: Default::default(),
        }
    }

    /// Helper function to create `FilteredBatchConfig` for testing
    fn create_test_config(
        export_severity: Severity,
        target_filters: Option<Vec<String>>,
    ) -> FilteredBatchConfig {
        FilteredBatchConfig {
            max_queue_size: 100,
            scheduled_delay: Duration::from_millis(100),
            max_export_batch_size: 10,
            max_export_timeout: Duration::from_millis(1000),
            export_severity,
            target_filters,
        }
    }

    #[test]
    fn test_should_export_log_severity_filtering_basic() {
        let config = create_test_config(Severity::Error, None);

        // Test logs with different severities
        let error_log = create_log_data(Some(Severity::Error), None);
        let warn_log = create_log_data(Some(Severity::Warn), None);
        let info_log = create_log_data(Some(Severity::Info), None);
        let debug_log = create_log_data(Some(Severity::Debug), None);

        // Only Error and above should pass
        assert!(should_export_log(&error_log, &config));
        assert!(!should_export_log(&warn_log, &config));
        assert!(!should_export_log(&info_log, &config));
        assert!(!should_export_log(&debug_log, &config));
    }

    #[test]
    fn test_should_export_log_severity_filtering_warn_level() {
        let config = create_test_config(Severity::Warn, None);

        let error_log = create_log_data(Some(Severity::Error), None);
        let warn_log = create_log_data(Some(Severity::Warn), None);
        let info_log = create_log_data(Some(Severity::Info), None);

        // Warn and above should pass
        assert!(should_export_log(&error_log, &config));
        assert!(should_export_log(&warn_log, &config));
        assert!(!should_export_log(&info_log, &config));
    }

    #[test]
    fn test_should_export_log_severity_filtering_no_severity() {
        let config = create_test_config(Severity::Error, None);
        let log_without_severity = create_log_data(None, None);

        // Logs without severity should not pass
        assert!(!should_export_log(&log_without_severity, &config));
    }

    #[test]
    fn test_should_export_log_target_filtering_exact_match() {
        let config = create_test_config(
            Severity::Info,
            Some(vec![
                "critical_service".to_string(),
                "payment_processor".to_string(),
            ]),
        );

        let matching_log1 = create_log_data(Some(Severity::Info), Some("critical_service"));
        let matching_log2 = create_log_data(Some(Severity::Info), Some("payment_processor"));
        let non_matching_log = create_log_data(Some(Severity::Info), Some("other_service"));

        assert!(should_export_log(&matching_log1, &config));
        assert!(should_export_log(&matching_log2, &config));
        assert!(!should_export_log(&non_matching_log, &config));
    }

    #[test]
    fn test_should_export_log_target_filtering_no_target_attribute() {
        let config = create_test_config(Severity::Info, Some(vec!["critical_service".to_string()]));

        let log_without_target = create_log_data(Some(Severity::Info), None);

        // Logs without target attribute should not pass when target filters are configured
        assert!(!should_export_log(&log_without_target, &config));
    }

    #[test]
    fn test_should_export_log_target_filtering_no_filters_configured() {
        let config = create_test_config(Severity::Info, None);

        let log_with_target = create_log_data(Some(Severity::Info), Some("any_target"));
        let log_without_target = create_log_data(Some(Severity::Info), None);

        // When no target filters are configured, all logs should pass target filtering
        assert!(should_export_log(&log_with_target, &config));
        assert!(should_export_log(&log_without_target, &config));
    }

    #[test]
    fn test_should_export_log_target_filtering_case_sensitive() {
        let config = create_test_config(Severity::Info, Some(vec!["CriticalService".to_string()]));

        let matching_log = create_log_data(Some(Severity::Info), Some("CriticalService"));
        let non_matching_log = create_log_data(Some(Severity::Info), Some("criticalservice"));

        // Target matching should be case-sensitive
        assert!(should_export_log(&matching_log, &config));
        assert!(!should_export_log(&non_matching_log, &config));
    }

    #[test]
    fn test_should_export_log_combined_severity_and_target_filtering() {
        let config =
            create_test_config(Severity::Error, Some(vec!["critical_service".to_string()]));

        // Both severity and target match
        let matching_log = create_log_data(Some(Severity::Error), Some("critical_service"));

        // Severity matches but target doesn't
        let severity_only_log = create_log_data(Some(Severity::Error), Some("other_service"));

        // Target matches but severity doesn't
        let target_only_log = create_log_data(Some(Severity::Warn), Some("critical_service"));

        // Neither matches
        let no_match_log = create_log_data(Some(Severity::Warn), Some("other_service"));

        assert!(should_export_log(&matching_log, &config));
        assert!(!should_export_log(&severity_only_log, &config));
        assert!(!should_export_log(&target_only_log, &config));
        assert!(!should_export_log(&no_match_log, &config));
    }

    #[test]
    fn test_should_export_log_target_filtering_empty_filter_list() {
        let config = create_test_config(Severity::Info, Some(vec![]));

        let log_with_target = create_log_data(Some(Severity::Info), Some("any_target"));

        // Empty filter list should reject all logs (no targets match)
        assert!(!should_export_log(&log_with_target, &config));
    }

    #[test]
    fn test_should_export_log_target_filtering_special_characters() {
        let config = create_test_config(
            Severity::Info,
            Some(vec![
                "service-with-dashes".to_string(),
                "service_with_underscores".to_string(),
                "service::with::colons".to_string(),
            ]),
        );

        let dash_log = create_log_data(Some(Severity::Info), Some("service-with-dashes"));
        let underscore_log =
            create_log_data(Some(Severity::Info), Some("service_with_underscores"));
        let colon_log = create_log_data(Some(Severity::Info), Some("service::with::colons"));

        assert!(should_export_log(&dash_log, &config));
        assert!(should_export_log(&underscore_log, &config));
        assert!(should_export_log(&colon_log, &config));
    }

    #[test]
    fn test_should_export_log_target_filtering_non_string_attribute() {
        let config = create_test_config(Severity::Info, Some(vec!["critical_service".to_string()]));

        // Create a log with target attribute that's not a string
        let mut log_record = SdkLogRecord::default();
        log_record.set_severity_number(Severity::Info);
        log_record.add_attribute("target", AnyValue::Int(123)); // Non-string value

        let log_data = LogData {
            record: log_record,
            instrumentation: Default::default(),
        };

        // Non-string target attributes should not match
        assert!(!should_export_log(&log_data, &config));
    }

    #[test]
    fn test_should_export_log_multiple_attributes_with_target() {
        let config = create_test_config(Severity::Info, Some(vec!["critical_service".to_string()]));

        let mut log_record = SdkLogRecord::default();
        log_record.set_severity_number(Severity::Info);
        log_record.add_attribute("service", AnyValue::from("some_service".to_string()));
        log_record.add_attribute("target", AnyValue::from("critical_service".to_string()));
        log_record.add_attribute("component", AnyValue::from("auth".to_string()));

        let log_data = LogData {
            record: log_record,
            instrumentation: Default::default(),
        };

        // Should find the target attribute among multiple attributes
        assert!(should_export_log(&log_data, &config));
    }

    #[test]
    fn test_should_export_log_severity_levels_comprehensive() {
        // Test all severity levels with different thresholds
        let trace_config = create_test_config(Severity::Trace, None);
        let debug_config = create_test_config(Severity::Debug, None);
        let info_config = create_test_config(Severity::Info, None);
        let warn_config = create_test_config(Severity::Warn, None);
        let error_config = create_test_config(Severity::Error, None);

        let trace_log = create_log_data(Some(Severity::Trace), None);
        let debug_log = create_log_data(Some(Severity::Debug), None);
        let info_log = create_log_data(Some(Severity::Info), None);
        let warn_log = create_log_data(Some(Severity::Warn), None);
        let error_log = create_log_data(Some(Severity::Error), None);

        // Trace threshold - should accept all
        assert!(should_export_log(&trace_log, &trace_config));
        assert!(should_export_log(&debug_log, &trace_config));
        assert!(should_export_log(&info_log, &trace_config));
        assert!(should_export_log(&warn_log, &trace_config));
        assert!(should_export_log(&error_log, &trace_config));

        // Debug threshold - should accept debug and above
        assert!(!should_export_log(&trace_log, &debug_config));
        assert!(should_export_log(&debug_log, &debug_config));
        assert!(should_export_log(&info_log, &debug_config));
        assert!(should_export_log(&warn_log, &debug_config));
        assert!(should_export_log(&error_log, &debug_config));

        // Info threshold - should accept info and above
        assert!(!should_export_log(&trace_log, &info_config));
        assert!(!should_export_log(&debug_log, &info_config));
        assert!(should_export_log(&info_log, &info_config));
        assert!(should_export_log(&warn_log, &info_config));
        assert!(should_export_log(&error_log, &info_config));

        // Warn threshold - should accept warn and above
        assert!(!should_export_log(&trace_log, &warn_config));
        assert!(!should_export_log(&debug_log, &warn_config));
        assert!(!should_export_log(&info_log, &warn_config));
        assert!(should_export_log(&warn_log, &warn_config));
        assert!(should_export_log(&error_log, &warn_config));

        // Error threshold - should accept only error
        assert!(!should_export_log(&trace_log, &error_config));
        assert!(!should_export_log(&debug_log, &error_config));
        assert!(!should_export_log(&info_log, &error_config));
        assert!(!should_export_log(&warn_log, &error_config));
        assert!(should_export_log(&error_log, &error_config));
    }

    #[test]
    fn test_should_export_log_edge_cases() {
        // Test various edge cases

        // Empty string target
        let config = create_test_config(Severity::Info, Some(vec![String::new()]));
        let empty_target_log = create_log_data(Some(Severity::Info), Some(""));
        assert!(should_export_log(&empty_target_log, &config));

        // Very long target name
        let long_target = "a".repeat(1000);
        let long_config = create_test_config(Severity::Info, Some(vec![long_target.clone()]));
        let long_target_log = create_log_data(Some(Severity::Info), Some(&long_target));
        assert!(should_export_log(&long_target_log, &long_config));

        // Unicode characters in target
        let unicode_config = create_test_config(
            Severity::Info,
            Some(vec!["服务器".to_string(), "🚀rocket".to_string()]),
        );
        let unicode_log1 = create_log_data(Some(Severity::Info), Some("服务器"));
        let unicode_log2 = create_log_data(Some(Severity::Info), Some("🚀rocket"));
        assert!(should_export_log(&unicode_log1, &unicode_config));
        assert!(should_export_log(&unicode_log2, &unicode_config));
    }

    #[test]
    fn test_should_export_log_multiple_target_attributes() {
        // Test log with multiple target-related attributes (only "target" should match)
        let config = create_test_config(Severity::Info, Some(vec!["service1".to_string()]));

        let mut log_record = SdkLogRecord::default();
        log_record.set_severity_number(Severity::Info);
        log_record.add_attribute("target", AnyValue::from("service1".to_string()));
        log_record.add_attribute("target_service", AnyValue::from("service2".to_string()));
        log_record.add_attribute("service_target", AnyValue::from("service3".to_string()));

        let log_data = LogData {
            record: log_record,
            instrumentation: Default::default(),
        };

        // Should match because "target" attribute has "service1"
        assert!(should_export_log(&log_data, &config));
    }
}
