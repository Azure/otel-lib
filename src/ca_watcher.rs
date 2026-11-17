// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::{path::Path, time::Duration};

use log::{debug, error};
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
    /// Starts watching every path in `paths`.
    ///
    /// Each watch is registered before this returns, so changes made afterwards are reported.
    ///
    /// # Errors
    /// Returns an error if the watcher cannot start or if any path cannot be watched, for
    /// example because it does not exist.
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
            debouncer
                .watcher()
                .watch(Path::new(path), RecursiveMode::NonRecursive)?;
            debug!("watching for changes to {path}");
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

// The expected events and the symlink layout are specific to inotify on Linux.
#[cfg(all(test, target_os = "linux"))]
mod tests {
    use std::{
        collections::HashSet,
        fs::{self, OpenOptions},
        path::{Path, PathBuf},
        time::{Duration, SystemTime},
    };

    use filetime::{set_file_mtime, FileTime};
    use notify_debouncer_mini::notify::ErrorKind;
    use tokio::time::timeout;
    use uuid::Uuid;

    use super::CaWatcher;

    const EVENT_TIMEOUT: Duration = Duration::from_secs(10);

    /// Directory that is removed when the test ends, even if it fails.
    struct TestDir(PathBuf);

    impl TestDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("otel_lib_ca_watcher_{}", Uuid::new_v4()));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum Change {
        Write,
        CloseAfterWriteOpen,
        Touch,
        Remove,
        Rename,
        AtomicReplace,
        ProjectedVolumeRotation,
    }

    /// Creates the CA file to watch and returns the path to watch.
    fn create_ca(dir: &Path, change: Change) -> PathBuf {
        match change {
            Change::ProjectedVolumeRotation => {
                // Kubernetes mounts each key as a symlink through `..data` to a timestamped
                // directory.
                fs::create_dir(dir.join("..2024_01")).unwrap();
                fs::write(dir.join("..2024_01/ca.crt"), "old").unwrap();
                std::os::unix::fs::symlink("..2024_01", dir.join("..data")).unwrap();
                std::os::unix::fs::symlink("..data/ca.crt", dir.join("ca.crt")).unwrap();
            }
            _ => fs::write(dir.join("ca.crt"), "old").unwrap(),
        }
        dir.join("ca.crt")
    }

    fn apply(dir: &Path, change: Change) {
        let ca = dir.join("ca.crt");
        match change {
            Change::Write => fs::write(&ca, "new").unwrap(),
            Change::CloseAfterWriteOpen => {
                // Stands in for a write through a memory map, which only sends a close event.
                drop(OpenOptions::new().write(true).open(&ca).unwrap());
            }
            Change::Touch => {
                set_file_mtime(&ca, FileTime::from_system_time(SystemTime::now())).unwrap();
            }
            Change::Remove => fs::remove_file(&ca).unwrap(),
            Change::Rename => fs::rename(&ca, dir.join("old.crt")).unwrap(),
            Change::AtomicReplace => {
                fs::write(dir.join("ca.crt.tmp"), "new").unwrap();
                fs::rename(dir.join("ca.crt.tmp"), &ca).unwrap();
            }
            Change::ProjectedVolumeRotation => {
                // Same steps as the kubelet atomic writer.
                fs::create_dir(dir.join("..2024_02")).unwrap();
                fs::write(dir.join("..2024_02/ca.crt"), "new").unwrap();
                std::os::unix::fs::symlink("..2024_02", dir.join("..data_tmp")).unwrap();
                fs::rename(dir.join("..data_tmp"), dir.join("..data")).unwrap();
                fs::remove_dir_all(dir.join("..2024_01")).unwrap();
            }
        }
    }

    /// Reads one watched CA, then changes another, and checks that only the change is reported.
    ///
    /// Events reach the debouncer in order, so a reported read would arrive in the same batch
    /// as the change or earlier.
    async fn assert_only_change_reported(change: Change) {
        let dir = TestDir::new();
        let read_only = dir.0.join("read_only.crt");
        fs::write(&read_only, "unchanged").unwrap();
        let changed = create_ca(&dir.0, change);

        let mut watcher = CaWatcher::start(
            [&read_only, &changed]
                .into_iter()
                .map(|path| path.to_str().unwrap()),
        )
        .unwrap();

        fs::read_to_string(&read_only).unwrap();
        fs::read_to_string(&changed).unwrap();
        apply(&dir.0, change);

        let events = timeout(EVENT_TIMEOUT, watcher.recv())
            .await
            .unwrap_or_else(|_| panic!("{change:?}: no event within {EVENT_TIMEOUT:?}"))
            .expect("watcher stopped")
            .unwrap_or_else(|e| panic!("{change:?}: watcher error: {e:?}"));
        let paths: HashSet<_> = events.into_iter().map(|event| event.path).collect();
        assert_eq!(paths, HashSet::from([changed]), "{change:?}");
    }

    #[tokio::test]
    async fn write_is_reported() {
        assert_only_change_reported(Change::Write).await;
    }

    #[tokio::test]
    async fn close_after_write_open_is_reported() {
        assert_only_change_reported(Change::CloseAfterWriteOpen).await;
    }

    #[tokio::test]
    async fn touch_is_reported() {
        assert_only_change_reported(Change::Touch).await;
    }

    #[tokio::test]
    async fn remove_is_reported() {
        assert_only_change_reported(Change::Remove).await;
    }

    #[tokio::test]
    async fn rename_is_reported() {
        assert_only_change_reported(Change::Rename).await;
    }

    #[tokio::test]
    async fn atomic_replace_is_reported() {
        assert_only_change_reported(Change::AtomicReplace).await;
    }

    #[tokio::test]
    async fn projected_volume_rotation_is_reported() {
        assert_only_change_reported(Change::ProjectedVolumeRotation).await;
    }

    #[test]
    fn missing_path_is_an_error() {
        let dir = TestDir::new();
        let missing = dir.0.join("missing.crt");

        let Err(e) = CaWatcher::start([missing.to_str().unwrap()]) else {
            panic!("watching {missing:?} should fail");
        };
        assert!(matches!(e.kind, ErrorKind::PathNotFound), "{e:?}");
    }
}
