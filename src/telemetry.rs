use std::path::Path;

use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

use crate::config::{LogRotation, LoggingConfig};

/// Initialises tracing to stdout and (when enabled) to rotating files.
///
/// The returned guard keeps the non-blocking file writer's background thread alive;
/// bind it in `main` for the lifetime of the process.
///
/// Security notes (public, no-auth deployments):
/// - File logging is refused (and the process continues on stdout only) when the
///   configured log directory or a would-be log file is a symlink. `tracing-appender`
///   opens log files with `O_CREAT|O_APPEND` and no `O_NOFOLLOW`, so following a
///   planted symlink would append attacker log data to an arbitrary file. We cannot
///   pass `O_NOFOLLOW` through the builder, so we fail closed instead.
/// - The stdout layer is JSON with ANSI disabled, so control characters are escaped
///   by the JSON serializer and cannot be interpreted as terminal escape sequences.
/// - `rotation = "never"` is unbounded (the `max_files` cap only applies to rotated
///   files) and is refused while file logging is enabled.
pub fn init(config: &LoggingConfig) -> Option<WorkerGuard> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("providarr=debug,tower_http=info,sqlx=warn"));

    if !config.enabled {
        tracing_subscriber::registry()
            .with(filter)
            .with(
                config
                    .stdout
                    .then(|| fmt::layer().json().with_target(true).with_ansi(false)),
            )
            .init();
        return None;
    }

    let appender = match build_appender(config) {
        Ok(appender) => appender,
        Err(err) => {
            eprintln!("providarr: file logging disabled ({err}); logging to stdout only");
            tracing_subscriber::registry()
                .with(filter)
                .with(
                    config
                        .stdout
                        .then(|| fmt::layer().json().with_target(true).with_ansi(false)),
                )
                .init();
            return None;
        }
    };

    let (writer, guard) = tracing_appender::non_blocking(appender);

    tracing_subscriber::registry()
        .with(filter)
        .with(
            config
                .stdout
                .then(|| fmt::layer().json().with_target(true).with_ansi(false)),
        )
        .with(
            fmt::layer()
                .json()
                .with_target(true)
                .with_ansi(false)
                .with_writer(writer),
        )
        .init();

    Some(guard)
}

fn build_appender(config: &LoggingConfig) -> anyhow::Result<RollingFileAppender> {
    // `Rotation::NEVER` produces a single, ever-growing file; `max_files` cannot
    // bound it because nothing is ever rotated. Refuse rather than silently allow
    // unbounded disk usage on an internet-facing service.
    if matches!(config.rotation, LogRotation::Never) {
        anyhow::bail!(
            "logging.rotation = \"never\" is unbounded (max_files only caps rotated \
             files); use minutely/hourly/daily or set logging.enabled = false"
        );
    }

    let dir = Path::new(&config.dir);
    create_log_dir(dir)?;

    // Refuse a symlinked log file and restrict existing log files before the
    // appender opens anything.
    secure_log_files(dir, &config.filename_prefix, "log")?;

    let mut builder = RollingFileAppender::builder()
        .rotation(map_rotation(config.rotation))
        .filename_prefix(&config.filename_prefix)
        .filename_suffix("log");

    if config.max_files > 0 {
        builder = builder.max_log_files(config.max_files);
    }

    let appender = builder.build(dir)?;

    // The appender just created (or opened) the active log file; tighten it too.
    // Best effort: a failure here must not stop the process, and the directory is
    // already 0o700 so the file is not reachable by other users regardless.
    let _ = secure_log_files(dir, &config.filename_prefix, "log");

    Ok(appender)
}

/// Ensures `dir` exists as a real (non-symlink) directory restricted to `0o700`.
///
/// A symlinked log directory is rejected before any `set_permissions` call, which
/// would otherwise follow the link and chmod an arbitrary target.
fn create_log_dir(dir: &Path) -> std::io::Result<()> {
    if let Ok(meta) = std::fs::symlink_metadata(dir) {
        if meta.file_type().is_symlink() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!("log directory {} is a symlink", dir.display()),
            ));
        }
        if !meta.is_dir() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotADirectory,
                format!("log path {} exists but is not a directory", dir.display()),
            ));
        }
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;

        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true).mode(0o700);
        match builder.create(dir) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(err) => return Err(err),
        }
    }

    #[cfg(not(unix))]
    {
        std::fs::create_dir_all(dir)?;
    }

    // Re-verify after creation before touching permissions (create_dir_all could
    // have raced with a symlink swap) and re-secure pre-existing directories.
    let meta = std::fs::symlink_metadata(dir)?;
    if meta.file_type().is_symlink() || !meta.is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("log directory {} is not a real directory", dir.display()),
        ));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }

    Ok(())
}

/// Rejects symlinked log files in `dir` matching the appender's naming and, on
/// Unix, restricts matching regular files to `0o600`.
///
/// Entries are matched on the prefix/suffix only (not the exact rotation date) so
/// that this also covers the file the appender is about to create regardless of
/// the current rotation window.
fn secure_log_files(dir: &Path, prefix: &str, suffix: &str) -> std::io::Result<()> {
    let prefix = (!prefix.is_empty()).then_some(prefix);
    let suffix = (!suffix.is_empty()).then_some(suffix);

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };

        let matches = match (prefix, suffix) {
            (Some(prefix), Some(suffix)) => name.starts_with(prefix) && name.ends_with(suffix),
            (Some(prefix), None) => name.starts_with(prefix),
            (None, Some(suffix)) => name.ends_with(suffix),
            (None, None) => true,
        };
        if !matches {
            continue;
        }

        // `DirEntry::file_type` uses lstat semantics and does not follow links.
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!("log file {} is a symlink", entry.path().display()),
            ));
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if file_type.is_file() {
                std::fs::set_permissions(entry.path(), std::fs::Permissions::from_mode(0o600))?;
            }
        }
    }

    Ok(())
}

fn map_rotation(rotation: LogRotation) -> Rotation {
    match rotation {
        LogRotation::Never => Rotation::NEVER,
        LogRotation::Minutely => Rotation::MINUTELY,
        LogRotation::Hourly => Rotation::HOURLY,
        LogRotation::Daily => Rotation::DAILY,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn logging_config(dir: String) -> LoggingConfig {
        LoggingConfig {
            enabled: true,
            dir,
            filename_prefix: "providarr".to_string(),
            rotation: LogRotation::Daily,
            max_files: 3,
            stdout: false,
        }
    }

    #[test]
    fn build_appender_creates_the_log_directory() {
        let root = tempfile::tempdir().unwrap();
        let logging = logging_config(
            root.path()
                .join("nested/logs")
                .to_string_lossy()
                .into_owned(),
        );

        let appender = build_appender(&logging).expect("appender builds");
        drop(appender);

        assert!(std::path::Path::new(&logging.dir).is_dir());
    }

    #[test]
    fn build_appender_refuses_never_rotation() {
        let root = tempfile::tempdir().unwrap();
        let mut logging = logging_config(root.path().to_string_lossy().into_owned());
        logging.rotation = LogRotation::Never;

        let err = build_appender(&logging).expect_err("never rotation is refused");
        assert!(err.to_string().contains("never"));
    }

    #[cfg(unix)]
    #[test]
    fn build_appender_rejects_symlinked_log_directory() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("target");
        std::fs::create_dir(&target).unwrap();
        let link = root.path().join("logs");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let logging = logging_config(link.to_string_lossy().into_owned());
        let err = build_appender(&logging).expect_err("symlinked dir is refused");
        assert!(err.to_string().contains("symlink"));
    }

    #[cfg(unix)]
    #[test]
    fn build_appender_rejects_symlinked_log_file() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("logs");
        std::fs::create_dir(&dir).unwrap();
        let target = root.path().join("victim");
        std::fs::write(&target, b"do not touch").unwrap();
        std::os::unix::fs::symlink(&target, dir.join("providarr.2026-01-01.log")).unwrap();

        let logging = logging_config(dir.to_string_lossy().into_owned());
        let err = build_appender(&logging).expect_err("symlinked log file is refused");
        assert!(err.to_string().contains("symlink"));
        assert_eq!(std::fs::read(&target).unwrap(), b"do not touch");
    }
}
