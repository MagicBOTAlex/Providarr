use std::{
    collections::HashMap,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use bytes::Bytes;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Largest fixture or recorded file we are willing to read from disk.
const MAX_FIXTURE_BYTES: u64 = 16 * 1024 * 1024;

/// Largest number of `recorded/*.json` entries processed during a load.
const MAX_RECORDED_ENTRIES: usize = 10_000;

/// A canned upstream response used while developing offline.
#[derive(Debug, Clone, Deserialize)]
pub struct FixtureSpec {
    pub provider: String,
    #[serde(default = "default_method")]
    pub method: String,
    /// Full path and query string, e.g. `/movie/550?language=en-US`.
    ///
    /// Manifest entries are usually query-less, which is treated as a wildcard
    /// for any query string; recorded responses are keyed on the exact value so
    /// distinct query variants never collide.
    pub path: String,
    pub file: String,
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default = "default_status")]
    pub status: u16,
}

fn default_method() -> String {
    "GET".to_string()
}

fn default_status() -> u16 {
    200
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetadataSpec {
    pub id: String,
    pub file: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct MetadataManifest {
    #[serde(default)]
    pub movie: Vec<MetadataSpec>,
    #[serde(default)]
    pub series: Vec<MetadataSpec>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Manifest {
    #[serde(default)]
    pub upstream: Vec<FixtureSpec>,
    #[serde(default)]
    pub metadata: MetadataManifest,
}

/// A response captured from a live provider, persisted under `<dir>/recorded/`
/// so a later run can replay it without calling upstream.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RecordedResponse {
    provider: String,
    method: String,
    path: String,
    #[serde(default)]
    content_type: Option<String>,
    status: u16,
    body: String,
}

#[derive(Debug, Clone)]
pub struct ReplayResponse {
    pub status: u16,
    pub content_type: Option<String>,
    pub body: Bytes,
}

/// Loads canned responses from a caller-supplied fixtures directory.
///
/// The directory defaults to `fixtures/` (config `replay.dir`) and may contain a
/// `manifest.json` plus a `recorded/` folder. With `replay.record` enabled, every
/// successful live provider response is written under `recorded/` (gitignored), so
/// a later run can replay it offline. The repository ships no captured provider
/// data; the test suite uses synthetic fixtures under `tests/fixtures/`.
pub struct ReplayStore {
    pub root: PathBuf,
    record: bool,
    upstream: RwLock<Vec<(FixtureSpec, Bytes)>>,
    movies: HashMap<String, Bytes>,
    series: HashMap<String, Bytes>,
}

impl ReplayStore {
    pub fn load(root: impl Into<PathBuf>, record: bool) -> Self {
        let root = root.into();
        // Canonicalize once so every fixture path can be confined to the root,
        // even when the configured root is relative or contains symlinks.
        let canonical_root = root.canonicalize().unwrap_or_else(|_| root.clone());

        let manifest_path = canonical_root.join("manifest.json");
        let manifest: Manifest = match std::fs::metadata(&manifest_path) {
            Ok(metadata) if metadata.len() > MAX_FIXTURE_BYTES => {
                tracing::warn!(
                    path = %manifest_path.display(),
                    size = metadata.len(),
                    max = MAX_FIXTURE_BYTES,
                    "fixtures manifest too large; ignoring"
                );
                Manifest::default()
            }
            Ok(_) => match std::fs::read_to_string(&manifest_path) {
                Ok(raw) => match serde_json::from_str(&raw) {
                    Ok(manifest) => manifest,
                    Err(err) => {
                        tracing::warn!(error = %err, path = %manifest_path.display(), "failed to parse fixtures manifest");
                        Manifest::default()
                    }
                },
                Err(err) => {
                    tracing::warn!(error = %err, path = %manifest_path.display(), "failed to read fixtures manifest");
                    Manifest::default()
                }
            },
            Err(_) => {
                tracing::debug!(path = %manifest_path.display(), "fixtures manifest not found; replay cache empty");
                Manifest::default()
            }
        };

        let read = |file: &str| -> Option<Bytes> {
            let path = resolve_fixture(&canonical_root, file)?;
            read_fixture(&path)
        };

        let collect = |specs: Vec<MetadataSpec>| -> HashMap<String, Bytes> {
            specs
                .into_iter()
                .filter_map(|spec| read(&spec.file).map(|body| (spec.id, body)))
                .collect()
        };

        let mut upstream: Vec<(FixtureSpec, Bytes)> = manifest
            .upstream
            .into_iter()
            .filter_map(|spec| read(&spec.file).map(|body| (spec, body)))
            .collect();

        // Recorded responses (from a previous `replay.record` run) override any
        // manifest entry for the same provider/method/path and query.
        Self::load_recorded(&canonical_root, &mut upstream);

        let movies = collect(manifest.metadata.movie);
        let series = collect(manifest.metadata.series);

        let store = Self {
            root,
            record,
            upstream: RwLock::new(upstream),
            movies,
            series,
        };
        tracing::info!(
            upstream = store.upstream.read().len(),
            movies = store.movies.len(),
            series = store.series.len(),
            record = store.record,
            root = %store.root.display(),
            "loaded replay fixtures"
        );
        store
    }

    pub fn load_arc(root: impl Into<PathBuf>, record: bool) -> Arc<Self> {
        Arc::new(Self::load(root, record))
    }

    fn load_recorded(root: &Path, upstream: &mut Vec<(FixtureSpec, Bytes)>) {
        let dir = root.join("recorded");
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(_) => return,
        };

        for (index, entry) in entries.flatten().enumerate() {
            if index >= MAX_RECORDED_ENTRIES {
                tracing::warn!(
                    max = MAX_RECORDED_ENTRIES,
                    "ignoring recorded responses beyond entry limit"
                );
                break;
            }

            let path = entry.path();

            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }

            // Re-resolve through the confinement check so a symlinked entry
            // cannot point outside the fixture root.
            let Ok(name) = entry.file_name().into_string() else {
                tracing::warn!(file = %path.display(), "skipping recorded response with non-UTF-8 name");
                continue;
            };
            let relative = format!("recorded/{name}");
            let Some(path) = resolve_fixture(root, &relative) else {
                continue;
            };

            let Some(raw) = read_fixture(&path) else {
                continue;
            };

            let record: RecordedResponse = match serde_json::from_slice(&raw) {
                Ok(record) => record,
                Err(err) => {
                    tracing::warn!(error = %err, file = %path.display(), "failed to parse recorded response");
                    continue;
                }
            };

            let spec = FixtureSpec {
                provider: record.provider,
                method: record.method,
                path: record.path,
                file: relative,
                content_type: record.content_type,
                status: record.status,
            };

            upstream.retain(|(existing, _)| !same_key(existing, &spec));
            upstream.push((spec, Bytes::from(record.body.into_bytes())));
        }
    }

    pub fn record_enabled(&self) -> bool {
        self.record
    }

    /// Captures a live upstream response to disk (and the in-memory index) so it
    /// can be replayed later. No-op unless `replay.record` is enabled.
    pub fn record(
        &self,
        provider: &str,
        method: &str,
        path_and_query: &str,
        status: u16,
        content_type: Option<String>,
        body: &Bytes,
    ) {
        if !self.record {
            return;
        }

        // Key on the full path and query so `/movie/1?language=en` and
        // `/movie/1?language=fr` are stored (and hashed) independently.
        let spec = FixtureSpec {
            provider: provider.to_string(),
            method: method.to_ascii_uppercase(),
            path: path_and_query.to_string(),
            file: String::new(),
            content_type: content_type.clone(),
            status,
        };

        {
            let mut upstream = self.upstream.write();
            upstream.retain(|(existing, _)| !same_key(existing, &spec));
            upstream.push((spec.clone(), body.clone()));
        }

        self.persist(&spec, body);
    }

    fn persist(&self, spec: &FixtureSpec, body: &Bytes) {
        let dir = self.root.join("recorded");

        if let Err(err) = std::fs::create_dir_all(&dir) {
            tracing::warn!(error = %err, dir = %dir.display(), "failed to create replay record directory");
            return;
        }

        let record = RecordedResponse {
            provider: spec.provider.clone(),
            method: spec.method.clone(),
            path: spec.path.clone(),
            content_type: spec.content_type.clone(),
            status: spec.status,
            body: String::from_utf8_lossy(body).into_owned(),
        };

        let json = match serde_json::to_vec_pretty(&record) {
            Ok(json) => json,
            Err(err) => {
                tracing::warn!(error = %err, "failed to serialize recorded response");
                return;
            }
        };

        let file = dir.join(record_file_name(&spec.provider, &spec.method, &spec.path));

        if let Err(err) = std::fs::write(&file, json) {
            tracing::warn!(error = %err, file = %file.display(), "failed to write recorded response");
        }
    }

    pub fn upstream(
        &self,
        provider: &str,
        method: &str,
        path_and_query: &str,
    ) -> Option<ReplayResponse> {
        self.upstream
            .read()
            .iter()
            .find(|(spec, _)| matches_request(spec, provider, method, path_and_query))
            .map(|(spec, body)| ReplayResponse {
                status: spec.status,
                content_type: spec
                    .content_type
                    .clone()
                    .or_else(|| Some("application/json".to_string())),
                body: body.clone(),
            })
    }

    pub fn movie(&self, id: &str) -> Option<Bytes> {
        self.movies.get(id).cloned()
    }

    pub fn series(&self, id: &str) -> Option<Bytes> {
        self.series.get(id).cloned()
    }

    pub fn is_empty(&self) -> bool {
        self.upstream.read().is_empty() && self.movies.is_empty() && self.series.is_empty()
    }
}

fn same_key(a: &FixtureSpec, b: &FixtureSpec) -> bool {
    a.provider.eq_ignore_ascii_case(&b.provider)
        && a.method.eq_ignore_ascii_case(&b.method)
        && a.path == b.path
}

fn strip_query(path_and_query: &str) -> &str {
    path_and_query.split('?').next().unwrap_or(path_and_query)
}

/// Matches a stored spec against an incoming request.
///
/// An exact path+query match always wins. Query-less entries (manifest fixtures
/// and legacy recordings) act as a wildcard for any query, which keeps offline
/// fixtures usable; entries that carry a query are only returned for that exact
/// query, so variants such as `?language=en` and `?language=fr` never collide.
fn matches_request(spec: &FixtureSpec, provider: &str, method: &str, path_and_query: &str) -> bool {
    if !spec.provider.eq_ignore_ascii_case(provider) || !spec.method.eq_ignore_ascii_case(method) {
        return false;
    }

    spec.path == path_and_query
        || (!spec.path.contains('?') && spec.path == strip_query(path_and_query))
}

/// Resolves a manifest/recorded `file` within `root`, rejecting absolute paths,
/// traversal components, and symlinks that escape the root.
fn resolve_fixture(root: &Path, file: &str) -> Option<PathBuf> {
    let relative = Path::new(file);
    if relative.is_absolute()
        || relative.components().any(|component| {
            matches!(
                component,
                Component::ParentDir
                    | Component::CurDir
                    | Component::RootDir
                    | Component::Prefix(_)
            )
        })
    {
        tracing::warn!(file, "refusing fixture path with traversal");
        return None;
    }

    let resolved = match root.join(relative).canonicalize() {
        Ok(resolved) => resolved,
        Err(err) => {
            tracing::warn!(error = %err, file, "failed to resolve fixture path");
            return None;
        }
    };

    if !resolved.starts_with(root) {
        tracing::warn!(
            file,
            resolved = %resolved.display(),
            "refusing fixture path escaping root"
        );
        return None;
    }

    Some(resolved)
}

/// Reads a fixture file, refusing anything larger than [`MAX_FIXTURE_BYTES`].
fn read_fixture(path: &Path) -> Option<Bytes> {
    match std::fs::metadata(path) {
        Ok(metadata) if metadata.len() > MAX_FIXTURE_BYTES => {
            tracing::warn!(
                file = %path.display(),
                size = metadata.len(),
                max = MAX_FIXTURE_BYTES,
                "skipping oversized fixture"
            );
            return None;
        }
        _ => {}
    }

    match std::fs::read(path) {
        Ok(bytes) => Some(Bytes::from(bytes)),
        Err(err) => {
            tracing::warn!(error = %err, file = %path.display(), "failed to read fixture");
            None
        }
    }
}

/// A deterministic, filesystem-safe name so re-recording the same request
/// overwrites its file instead of accumulating duplicates. The provider is
/// sanitized so it can never inject path separators or traversal segments.
fn record_file_name(provider: &str, method: &str, path: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(provider.as_bytes());
    hasher.update([0]);
    hasher.update(method.as_bytes());
    hasher.update([0]);
    hasher.update(path.as_bytes());
    let digest = hasher.finalize();
    let suffix = hex::encode(&digest[..8]);

    let sanitize = |value: &str, limit: usize| -> String {
        value
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .take(limit)
            .collect()
    };

    let safe_provider = sanitize(provider, 32);
    let safe_path = sanitize(path, 96);

    format!("{safe_provider}-{safe_path}-{suffix}.json")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_upstream_ignoring_query() {
        let store = ReplayStore::load("tests/fixtures", false);
        let hit = store
            .upstream(
                "tmdb",
                "GET",
                "/movie/1?language=en-US&append_to_response=credits",
            )
            .expect("tmdb movie fixture");
        assert_eq!(hit.status, 200);
        assert!(!hit.body.is_empty());

        assert!(store.upstream("tmdb", "GET", "/movie/999").is_none());
        assert!(store.movie("1").is_some());
        assert!(store.series("1").is_some());
        assert!(store.series("2").is_some());
    }

    #[tokio::test]
    async fn records_and_replays_a_response() {
        let dir = tempfile::tempdir().unwrap();
        let store = ReplayStore::load(dir.path(), true);
        assert!(store.record_enabled());
        assert!(store.upstream("tmdb", "GET", "/movie/1").is_none());

        store.record(
            "tmdb",
            "GET",
            "/movie/1?language=en-US",
            200,
            Some("application/json".to_string()),
            &Bytes::from_static(br#"{"id":1}"#),
        );

        // Served immediately, and again after a fresh load from disk. The
        // query is part of the key, so the same request replays exactly.
        assert_eq!(
            store
                .upstream("tmdb", "GET", "/movie/1?language=en-US")
                .unwrap()
                .body,
            Bytes::from_static(br#"{"id":1}"#)
        );

        let reloaded = ReplayStore::load(dir.path(), false);
        let hit = reloaded
            .upstream("tmdb", "GET", "/movie/1?language=en-US")
            .expect("recorded response reloads");
        assert_eq!(hit.status, 200);
        assert_eq!(hit.body, Bytes::from_static(br#"{"id":1}"#));
    }

    #[tokio::test]
    async fn keeps_query_variants_distinct() {
        let dir = tempfile::tempdir().unwrap();
        let store = ReplayStore::load(dir.path(), true);

        store.record(
            "tmdb",
            "GET",
            "/movie/1?language=en",
            200,
            None,
            &Bytes::from_static(b"en"),
        );
        store.record(
            "tmdb",
            "GET",
            "/movie/1?language=fr",
            200,
            None,
            &Bytes::from_static(b"fr"),
        );

        assert_eq!(
            store
                .upstream("tmdb", "GET", "/movie/1?language=en")
                .unwrap()
                .body,
            Bytes::from_static(b"en")
        );
        assert_eq!(
            store
                .upstream("tmdb", "GET", "/movie/1?language=fr")
                .unwrap()
                .body,
            Bytes::from_static(b"fr")
        );
        assert!(
            store
                .upstream("tmdb", "GET", "/movie/1?language=de")
                .is_none(),
            "a different query must not match a recorded variant"
        );
        assert_eq!(
            std::fs::read_dir(dir.path().join("recorded"))
                .unwrap()
                .count(),
            2,
            "each query variant gets its own file"
        );
    }

    #[test]
    fn rejects_manifest_paths_escaping_root() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        std::fs::create_dir(&root).unwrap();
        std::fs::write(tmp.path().join("secret.json"), br#"{"secret":true}"#).unwrap();

        let absolute = tmp.path().join("secret.json");
        let manifest = serde_json::json!({
            "upstream": [
                {
                    "provider": "tmdb",
                    "method": "GET",
                    "path": "/escape",
                    "file": "../secret.json"
                },
                {
                    "provider": "tmdb",
                    "method": "GET",
                    "path": "/absolute",
                    "file": absolute.to_string_lossy()
                }
            ],
            "metadata": {
                "movie": [{ "id": "dot", "file": "./secret.json" }]
            }
        });
        std::fs::write(root.join("manifest.json"), manifest.to_string()).unwrap();

        let store = ReplayStore::load(&root, false);
        assert!(store.upstream("tmdb", "GET", "/escape").is_none());
        assert!(store.upstream("tmdb", "GET", "/absolute").is_none());
        assert!(store.movie("dot").is_none());
        assert!(store.is_empty());
    }

    #[test]
    fn record_file_name_sanitizes_provider() {
        let name = record_file_name("../../evil/provider", "GET", "/movie/1?language=en");
        assert!(!name.contains('/'), "unsafe name: {name}");
        assert!(!name.contains(".."), "unsafe name: {name}");
        assert!(name.ends_with(".json"), "unsafe name: {name}");
    }
}
