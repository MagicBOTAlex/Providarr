use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

use crate::config::{LogRotation, LoggingConfig};

/// Initialises tracing to stdout and (when enabled) to rotating files.
///
/// The returned guard keeps the non-blocking file writer's background thread alive;
/// bind it in `main` for the lifetime of the process.
pub fn init(config: &LoggingConfig) -> Option<WorkerGuard> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("providarr=debug,tower_http=info,sqlx=warn"));

    if !config.enabled {
        tracing_subscriber::registry()
            .with(filter)
            .with(
                config
                    .stdout
                    .then(|| fmt::layer().with_target(true).compact()),
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
                        .then(|| fmt::layer().with_target(true).compact()),
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
                .then(|| fmt::layer().with_target(true).compact()),
        )
        .with(
            fmt::layer()
                .with_target(true)
                .with_ansi(false)
                .with_writer(writer)
                .compact(),
        )
        .init();

    Some(guard)
}

fn build_appender(config: &LoggingConfig) -> anyhow::Result<RollingFileAppender> {
    std::fs::create_dir_all(&config.dir)?;

    let mut builder = RollingFileAppender::builder()
        .rotation(map_rotation(config.rotation))
        .filename_prefix(&config.filename_prefix)
        .filename_suffix("log");

    if config.max_files > 0 {
        builder = builder.max_log_files(config.max_files);
    }

    Ok(builder.build(&config.dir)?)
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

    #[test]
    fn build_appender_creates_the_log_directory() {
        let root = tempfile::tempdir().unwrap();
        let logging = LoggingConfig {
            enabled: true,
            dir: root
                .path()
                .join("nested/logs")
                .to_string_lossy()
                .to_string(),
            filename_prefix: "providarr".to_string(),
            rotation: LogRotation::Daily,
            max_files: 3,
            stdout: false,
        };

        let appender = build_appender(&logging).expect("appender builds");
        drop(appender);

        assert!(std::path::Path::new(&logging.dir).is_dir());
    }
}
