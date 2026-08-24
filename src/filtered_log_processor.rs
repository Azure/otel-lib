// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::{fmt::Debug, time::Duration};

use opentelemetry::{
    logs::{AnyValue, Severity},
    InstrumentationScope,
};
use opentelemetry_sdk::{
    error::OTelSdkResult,
    logs::{LogProcessor, SdkLogRecord},
    Resource,
};

#[derive(Debug, Clone)]
pub(crate) struct FilteredBatchConfig {
    /// export level - levels >= which to export
    pub export_severity: Severity,

    /// target filters - only export logs from targets matching these exact names. If None, exports all logs.
    pub target_filters: Option<Vec<String>>,
}

impl Default for FilteredBatchConfig {
    fn default() -> Self {
        Self {
            export_severity: Severity::Error,
            target_filters: None,
        }
    }
}

#[derive(Debug)]
pub(crate) struct FilteredLogProcessor<P> {
    inner: P,
    config: FilteredBatchConfig,
}

impl<P> FilteredLogProcessor<P>
where
    P: LogProcessor,
{
    pub(crate) fn new(inner: P, config: FilteredBatchConfig) -> Self {
        Self { inner, config }
    }
}

impl<P> LogProcessor for FilteredLogProcessor<P>
where
    P: LogProcessor,
{
    fn emit(&self, record: &mut SdkLogRecord, instrumentation: &InstrumentationScope) {
        if should_export_log(record, &self.config) {
            self.inner.emit(record, instrumentation);
        }
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn event_enabled(&self, level: Severity, target: &str, name: Option<&str>) -> bool {
        level >= self.config.export_severity
            && target_matches(target, self.config.target_filters.as_deref())
            && self.inner.event_enabled(level, target, name)
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

fn should_export_log(log: &SdkLogRecord, config: &FilteredBatchConfig) -> bool {
    let severity_matches = log
        .severity_number()
        .is_some_and(|severity| severity >= config.export_severity);

    if !severity_matches {
        return false;
    }

    match config.target_filters.as_deref() {
        Some(target_filters) => log_target_matches(log, target_filters),
        None => true,
    }
}

fn log_target_matches(log: &SdkLogRecord, target_filters: &[String]) -> bool {
    log.target()
        .is_some_and(|target| target_matches(target.as_ref(), Some(target_filters)))
        || log.attributes_iter().any(|(key, value)| {
            key.as_str() == "target"
                && matches!(
                    value,
                    AnyValue::String(target) if target_matches(target.as_str(), Some(target_filters))
                )
        })
}

fn target_matches(target: &str, target_filters: Option<&[String]>) -> bool {
    match target_filters {
        Some(target_filters) => target_filters.iter().any(|filter| filter == target),
        None => true,
    }
}

#[cfg(test)]
#[allow(clippy::default_trait_access)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use opentelemetry::logs::{LogRecord, Logger, LoggerProvider as _};
    use opentelemetry_sdk::logs::{LogProcessor, SdkLoggerProvider};

    fn create_log_record(severity: Option<Severity>, target: Option<&str>) -> SdkLogRecord {
        let logger = SdkLoggerProvider::builder().build().logger("test-logger");
        let mut log_record = logger.create_log_record();

        if let Some(severity) = severity {
            log_record.set_severity_number(severity);
        }

        if let Some(target) = target {
            log_record.set_target(target.to_owned());
            log_record.add_attribute("target", target.to_owned());
        }

        log_record
    }

    fn create_test_config(
        export_severity: Severity,
        target_filters: Option<Vec<String>>,
    ) -> FilteredBatchConfig {
        FilteredBatchConfig {
            export_severity,
            target_filters,
        }
    }

    #[derive(Debug, Default, Clone)]
    struct RecordingLogProcessor {
        targets: Arc<Mutex<Vec<Option<String>>>>,
    }

    impl LogProcessor for RecordingLogProcessor {
        fn emit(&self, record: &mut SdkLogRecord, _instrumentation: &InstrumentationScope) {
            self.targets
                .lock()
                .unwrap()
                .push(record.target().map(std::string::ToString::to_string));
        }

        fn force_flush(&self) -> OTelSdkResult {
            Ok(())
        }
    }

    #[test]
    fn test_should_export_log_severity_filtering_basic() {
        let config = create_test_config(Severity::Error, None);

        let error_log = create_log_record(Some(Severity::Error), None);
        let warn_log = create_log_record(Some(Severity::Warn), None);
        let info_log = create_log_record(Some(Severity::Info), None);
        let debug_log = create_log_record(Some(Severity::Debug), None);

        assert!(should_export_log(&error_log, &config));
        assert!(!should_export_log(&warn_log, &config));
        assert!(!should_export_log(&info_log, &config));
        assert!(!should_export_log(&debug_log, &config));
    }

    #[test]
    fn test_should_export_log_severity_filtering_warn_level() {
        let config = create_test_config(Severity::Warn, None);

        let error_log = create_log_record(Some(Severity::Error), None);
        let warn_log = create_log_record(Some(Severity::Warn), None);
        let info_log = create_log_record(Some(Severity::Info), None);

        assert!(should_export_log(&error_log, &config));
        assert!(should_export_log(&warn_log, &config));
        assert!(!should_export_log(&info_log, &config));
    }

    #[test]
    fn test_should_export_log_severity_filtering_no_severity() {
        let config = create_test_config(Severity::Error, None);
        let log_without_severity = create_log_record(None, None);

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

        let matching_log1 = create_log_record(Some(Severity::Info), Some("critical_service"));
        let matching_log2 = create_log_record(Some(Severity::Info), Some("payment_processor"));
        let non_matching_log = create_log_record(Some(Severity::Info), Some("other_service"));

        assert!(should_export_log(&matching_log1, &config));
        assert!(should_export_log(&matching_log2, &config));
        assert!(!should_export_log(&non_matching_log, &config));
    }

    #[test]
    fn test_should_export_log_target_filtering_no_target_attribute() {
        let config = create_test_config(Severity::Info, Some(vec!["critical_service".to_string()]));

        let log_without_target = create_log_record(Some(Severity::Info), None);

        assert!(!should_export_log(&log_without_target, &config));
    }

    #[test]
    fn test_should_export_log_target_filtering_no_filters_configured() {
        let config = create_test_config(Severity::Info, None);

        let log_with_target = create_log_record(Some(Severity::Info), Some("any_target"));
        let log_without_target = create_log_record(Some(Severity::Info), None);

        assert!(should_export_log(&log_with_target, &config));
        assert!(should_export_log(&log_without_target, &config));
    }

    #[test]
    fn test_should_export_log_target_filtering_case_sensitive() {
        let config = create_test_config(Severity::Info, Some(vec!["CriticalService".to_string()]));

        let matching_log = create_log_record(Some(Severity::Info), Some("CriticalService"));
        let non_matching_log = create_log_record(Some(Severity::Info), Some("criticalservice"));

        assert!(should_export_log(&matching_log, &config));
        assert!(!should_export_log(&non_matching_log, &config));
    }

    #[test]
    fn test_should_export_log_combined_severity_and_target_filtering() {
        let config =
            create_test_config(Severity::Error, Some(vec!["critical_service".to_string()]));

        let matching_log = create_log_record(Some(Severity::Error), Some("critical_service"));
        let severity_only_log = create_log_record(Some(Severity::Error), Some("other_service"));
        let target_only_log = create_log_record(Some(Severity::Warn), Some("critical_service"));
        let no_match_log = create_log_record(Some(Severity::Warn), Some("other_service"));

        assert!(should_export_log(&matching_log, &config));
        assert!(!should_export_log(&severity_only_log, &config));
        assert!(!should_export_log(&target_only_log, &config));
        assert!(!should_export_log(&no_match_log, &config));
    }

    #[test]
    fn test_should_export_log_target_filtering_empty_filter_list() {
        let config = create_test_config(Severity::Info, Some(vec![]));

        let log_with_target = create_log_record(Some(Severity::Info), Some("any_target"));

        assert!(!should_export_log(&log_with_target, &config));
    }

    #[test]
    fn test_filtered_processor_only_forwards_matching_logs() {
        let inner = RecordingLogProcessor::default();
        let captured_targets = inner.targets.clone();
        let filtered_processor = FilteredLogProcessor::new(
            inner,
            create_test_config(Severity::Error, Some(vec!["critical_service".to_string()])),
        );

        let scope = InstrumentationScope::builder("test-scope").build();

        let mut matching_log = create_log_record(Some(Severity::Error), Some("critical_service"));
        filtered_processor.emit(&mut matching_log, &scope);

        let mut wrong_severity = create_log_record(Some(Severity::Warn), Some("critical_service"));
        filtered_processor.emit(&mut wrong_severity, &scope);

        let mut wrong_target = create_log_record(Some(Severity::Error), Some("other_service"));
        filtered_processor.emit(&mut wrong_target, &scope);

        assert_eq!(
            captured_targets.lock().unwrap().as_slice(),
            &[Some("critical_service".to_owned())]
        );
    }
}
