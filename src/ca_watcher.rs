// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::{path::Path, time::Duration};

use log::{debug, error, warn};
use notify_debouncer_mini::{
    new_debouncer_opt,
    notify::{
        self,
        event::{AccessKind, AccessMode},
        Event, EventHandler, EventKind, RecommendedWatcher, RecursiveMode, Watcher, WatcherKind,
    },
    Config, DebounceEventResult, Debouncer,
};
use tokio::sync::mpsc;

/// Watches CA files and reports when any of them changes.
pub(crate) struct CaWatcher {
    // The watch stops when the debouncer is dropped, so it must live as long as the receiver.
    _debouncer: Debouncer<ChangeWatcher>,
    rx: mpsc::Receiver<DebounceEventResult>,
}

impl CaWatcher {
    /// Registers watches for the configured paths before returning.
    ///
    /// Logs individual registration failures and continues with the remaining paths.
    ///
    /// # Errors
    /// Returns an error if the watcher cannot start.
    pub(crate) fn start<'a>(
        paths: impl IntoIterator<Item = &'a str>,
    ) -> Result<Self, notify::Error> {
        let (tx, rx) = mpsc::channel(1);

        let config = Config::default().with_timeout(Duration::from_secs(1));
        let mut debouncer = new_debouncer_opt(config, move |res| {
            // This runs on the debouncer's own thread, not on an async executor thread.
            futures::executor::block_on(async {
                if let Err(e) = tx.send(res).await {
                    error!("error sending watcher notification: {e}");
                }
            });
        })?;

        for path in paths {
            match debouncer
                .watcher()
                .watch(Path::new(path), RecursiveMode::NonRecursive)
            {
                Ok(()) => debug!("watching for changes to {path}"),
                Err(e) => warn!("unable to watch CA file {path}: {e}"),
            }
        }

        Ok(Self {
            _debouncer: debouncer,
            rx,
        })
    }

    /// Waits for the next batch of changes or watcher error.
    ///
    /// Returns `None` if the watcher stopped.
    pub(crate) async fn recv(&mut self) -> Option<DebounceEventResult> {
        self.rx.recv().await
    }
}

/// The platform watcher, minus events that only mean a file was read.
///
/// The debouncer gives every event the same kind, so it cannot tell a read from a write. On
/// Linux, notify reports each time a watched file is opened. Without this filter, any process
/// that only reads a CA file would look like a CA rotation.
struct ChangeWatcher(RecommendedWatcher);

impl Watcher for ChangeWatcher {
    fn new<F: EventHandler>(mut event_handler: F, config: notify::Config) -> notify::Result<Self> {
        let watcher = RecommendedWatcher::new(
            move |res: notify::Result<Event>| {
                if !matches!(&res, Ok(event) if is_read_access(event.kind)) {
                    event_handler.handle_event(res);
                }
            },
            config,
        )?;

        Ok(Self(watcher))
    }

    fn watch(&mut self, path: &Path, recursive_mode: RecursiveMode) -> notify::Result<()> {
        self.0.watch(path, recursive_mode)
    }

    fn unwatch(&mut self, path: &Path) -> notify::Result<()> {
        self.0.unwatch(path)
    }

    fn kind() -> WatcherKind {
        RecommendedWatcher::kind()
    }
}

/// Returns whether `kind` is an access event that cannot mean the file changed.
///
/// A close after the file was opened for writing is kept as a change. On Linux, a write through
/// a memory map sends no other event.
fn is_read_access(kind: EventKind) -> bool {
    matches!(kind, EventKind::Access(access) if access != AccessKind::Close(AccessMode::Write))
}

#[cfg(test)]
mod tests {
    use notify_debouncer_mini::notify::{
        event::{AccessKind, AccessMode, DataChange, MetadataKind, ModifyKind, RemoveKind},
        EventKind,
    };

    use super::is_read_access;

    #[test]
    fn filters_read_access_but_keeps_change_signals() {
        for (kind, ignored) in [
            (EventKind::Access(AccessKind::Open(AccessMode::Any)), true),
            (EventKind::Access(AccessKind::Read), true),
            (EventKind::Access(AccessKind::Close(AccessMode::Read)), true),
            (
                EventKind::Access(AccessKind::Close(AccessMode::Write)),
                false,
            ),
            (EventKind::Modify(ModifyKind::Data(DataChange::Any)), false),
            (
                EventKind::Modify(ModifyKind::Metadata(MetadataKind::Any)),
                false,
            ),
            (EventKind::Remove(RemoveKind::Any), false),
            (EventKind::Other, false),
        ] {
            assert_eq!(is_read_access(kind), ignored, "{kind:?}");
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn failed_registration_preserves_read_filtering_and_write_close() {
        use std::{collections::HashSet, fs, fs::OpenOptions, path::PathBuf, time::Duration};

        use tokio::time::timeout;
        use uuid::Uuid;

        use super::CaWatcher;

        struct TestDir(PathBuf);
        impl Drop for TestDir {
            fn drop(&mut self) {
                if let Err(e) = fs::remove_dir_all(&self.0) {
                    eprintln!("unable to clean watcher test directory: {e}");
                }
            }
        }

        let dir = TestDir(std::env::temp_dir().join(format!("otel_ca_{}", Uuid::new_v4())));
        fs::create_dir(&dir.0).unwrap();
        let missing = dir.0.join("missing.crt");
        let read_only = dir.0.join("read_only.crt");
        let changed = dir.0.join("changed.crt");
        fs::write(&read_only, "unchanged").unwrap();
        fs::write(&changed, "unchanged").unwrap();
        let mut watcher = CaWatcher::start(
            [&missing, &read_only, &changed]
                .into_iter()
                .map(|path| path.to_str().unwrap()),
        )
        .unwrap();

        fs::read(&read_only).unwrap();
        fs::read(&changed).unwrap();
        drop(OpenOptions::new().write(true).open(&changed).unwrap());

        let events = timeout(Duration::from_secs(10), watcher.recv())
            .await
            .expect("write-close event did not arrive")
            .expect("watcher stopped")
            .expect("watcher reported an error");
        let paths: HashSet<_> = events.into_iter().map(|event| event.path).collect();
        assert_eq!(paths, HashSet::from([changed]));
    }
}
