use std::{
    collections::HashMap,
    io::Read,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use bytes::Bytes;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Largest fixture or recorded file we are willing to read from disk.
const MAX_FIXTURE_BYTES: u64 = 16 * 1024 * 1024;

/// Largest single response body served from or written to the replay cache.
///
/// The live cache cap (`cache.max_body_bytes`) is not available to this module,
/// so replay bodies are bounded by the same constant as fixture files instead.
/// This keeps recorded/fixture responses from bypassing the cache body cap by
/// more than the fixture allowance.
const MAX_REPLAY_BODY_BYTES: usize = MAX_FIXTURE_BYTES as usize;

/// Largest number of `recorded/*.json` entries processed during a load.
const MAX_RECORDED_ENTRIES: usize = 10_000;

/// Total recorded body bytes kept by the replay cache. Once the quota is
/// reached new responses are refused, so a public instance cannot grow the
/// replay cache without bound.
const MAX_RECORDED_BYTES: usize = 64 * 1024 * 1024;

/// Recorded entries are identified by this `file` prefix so the quota can
/// distinguish them from manifest fixtures loaded from the caller's manifest.
const RECORDED_PREFIX: &str = "recorded/";

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
            load_fixture(&canonical_root, file).map(|(_, body)| body)
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

        let mut total_bytes: usize = upstream
            .iter()
            .filter(|(spec, _)| spec.file.starts_with(RECORDED_PREFIX))
            .map(|(_, body)| body.len())
            .sum();

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

            let Ok(name) = entry.file_name().into_string() else {
                tracing::warn!(file = %path.display(), "skipping recorded response with non-UTF-8 name");
                continue;
            };
            let relative = format!("{RECORDED_PREFIX}{name}");
            // Open and read in one step so the confinement check and the read
            // operate on the same handle.
            let Some((path, raw)) = load_fixture(root, &relative) else {
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
            let body = Bytes::from(record.body.into_bytes());

            let existing = upstream.iter().position(|(e, _)| same_key(e, &spec));
            let replaced_bytes = existing
                .filter(|&i| upstream[i].0.file.starts_with(RECORDED_PREFIX))
                .map(|i| upstream[i].1.len())
                .unwrap_or_default();
            if total_bytes
                .saturating_sub(replaced_bytes)
                .saturating_add(body.len())
                > MAX_RECORDED_BYTES
            {
                tracing::warn!(
                    bytes = total_bytes,
                    incoming = body.len(),
                    max = MAX_RECORDED_BYTES,
                    "ignoring recorded responses beyond byte quota"
                );
                continue;
            }

            if let Some(i) = existing {
                upstream.remove(i);
            }
            total_bytes = total_bytes.saturating_sub(replaced_bytes) + body.len();
            upstream.push((spec, body));
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
        // `/movie/1?language=fr` are stored (and hashed) independently. The
        // `recorded/` prefix marks the entry as quota-accounted.
        let method = method.to_ascii_uppercase();
        let file = format!(
            "{RECORDED_PREFIX}{}",
            record_file_name(provider, &method, path_and_query)
        );
        let spec = FixtureSpec {
            provider: provider.to_string(),
            method,
            path: path_and_query.to_string(),
            file,
            content_type: content_type.clone(),
            status,
        };

        {
            let mut upstream = self.upstream.write();
            let existing = upstream.iter().position(|(e, _)| same_key(e, &spec));
            let replacing = existing
                .map(|i| upstream[i].0.file.starts_with(RECORDED_PREFIX))
                .unwrap_or(false);
            let replaced_bytes = if replacing {
                existing.map(|i| upstream[i].1.len()).unwrap_or_default()
            } else {
                0
            };
            let recorded_count = upstream
                .iter()
                .filter(|(spec, _)| spec.file.starts_with(RECORDED_PREFIX))
                .count();
            let recorded_bytes: usize = upstream
                .iter()
                .filter(|(spec, _)| spec.file.starts_with(RECORDED_PREFIX))
                .map(|(_, body)| body.len())
                .sum();

            if !within_record_quota(
                recorded_count,
                recorded_bytes,
                replacing,
                replaced_bytes,
                body.len(),
            ) {
                tracing::warn!(
                    count = recorded_count,
                    bytes = recorded_bytes,
                    incoming = body.len(),
                    "replay recording quota reached; refusing to record"
                );
                return;
            }

            if let Some(i) = existing {
                upstream.remove(i);
            }
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
        let upstream = self.upstream.read();
        // An exact path+query match always wins over a query-less wildcard,
        // regardless of insertion order. Otherwise a query-less manifest fixture
        // would shadow an exact recorded entry for the same path.
        let hit = upstream
            .iter()
            .find(|(spec, _)| exact_match(spec, provider, method, path_and_query))
            .or_else(|| {
                upstream
                    .iter()
                    .find(|(spec, _)| wildcard_match(spec, provider, method, path_and_query))
            })?;
        Some(ReplayResponse {
            status: hit.0.status,
            content_type: hit
                .0
                .content_type
                .clone()
                .or_else(|| Some("application/json".to_string())),
            body: hit.1.clone(),
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

/// An exact `path == path_and_query` match for the same provider and method.
fn exact_match(spec: &FixtureSpec, provider: &str, method: &str, path_and_query: &str) -> bool {
    spec.provider.eq_ignore_ascii_case(provider)
        && spec.method.eq_ignore_ascii_case(method)
        && spec.path == path_and_query
}

/// A query-less entry (manifest fixture or legacy recording) matching any query
/// for the same provider/method/path. This keeps offline fixtures usable.
fn wildcard_match(spec: &FixtureSpec, provider: &str, method: &str, path_and_query: &str) -> bool {
    spec.provider.eq_ignore_ascii_case(provider)
        && spec.method.eq_ignore_ascii_case(method)
        && !spec.path.contains('?')
        && spec.path == strip_query(path_and_query)
}

/// Returns true when a new recorded body may be admitted under the count and
/// total-byte quotas. `replacing` means the same key already exists, in which
/// case its `replaced_bytes` do not count against the quota.
fn within_record_quota(
    recorded_count: usize,
    recorded_bytes: usize,
    replacing: bool,
    replaced_bytes: usize,
    incoming_bytes: usize,
) -> bool {
    if incoming_bytes > MAX_REPLAY_BODY_BYTES {
        return false;
    }
    if !replacing && recorded_count >= MAX_RECORDED_ENTRIES {
        return false;
    }
    recorded_bytes
        .saturating_sub(replaced_bytes)
        .saturating_add(incoming_bytes)
        <= MAX_RECORDED_BYTES
}

/// Resolves, opens, and reads a manifest/recorded `file` within `root` in one
/// step.
///
/// The path is canonicalized and confined to `root`, then opened once; the body
/// is read from that handle rather than re-resolving the name, so a path swap
/// between the confinement check and the read cannot escape the root. `..` is
/// rejected, while `.` is harmless after canonicalization and is allowed so
/// `./`-prefixed fixtures work. Symlinked directories that escape the root are
/// caught by the `starts_with(root)` check; a symlinked final component swapped
/// in after canonicalization is rejected on a best-effort basis.
fn load_fixture(root: &Path, file: &str) -> Option<(PathBuf, Bytes)> {
    let relative = Path::new(file);
    if relative.is_absolute()
        || relative.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
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

    match std::fs::symlink_metadata(&resolved) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            tracing::warn!(
                file,
                resolved = %resolved.display(),
                "refusing symlinked fixture"
            );
            return None;
        }
        Ok(_) => {}
        Err(err) => {
            tracing::warn!(error = %err, file, "failed to stat fixture path");
            return None;
        }
    }

    let mut handle = match std::fs::File::open(&resolved) {
        Ok(handle) => handle,
        Err(err) => {
            tracing::warn!(error = %err, file, "failed to open fixture");
            return None;
        }
    };

    // Read from the opened handle, capped so a file that grows past the limit
    // between the metadata check and the read is still refused.
    let mut bytes = Vec::new();
    match (&mut handle)
        .take(MAX_FIXTURE_BYTES + 1)
        .read_to_end(&mut bytes)
    {
        Ok(_) if bytes.len() as u64 > MAX_FIXTURE_BYTES => {
            tracing::warn!(
                file,
                size = bytes.len(),
                max = MAX_FIXTURE_BYTES,
                "skipping oversized fixture"
            );
            return None;
        }
        Ok(_) => {}
        Err(err) => {
            tracing::warn!(error = %err, file, "failed to read fixture");
            return None;
        }
    }

    Some((resolved, Bytes::from(bytes)))
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

    #[test]
    fn exact_recorded_entry_beats_queryless_manifest_fixture() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::write(root.join("wild.json"), b"wild").unwrap();
        let manifest = serde_json::json!({
            "upstream": [
                { "provider": "tmdb", "method": "GET", "path": "/movie/1", "file": "wild.json" }
            ]
        });
        std::fs::write(root.join("manifest.json"), manifest.to_string()).unwrap();

        std::fs::create_dir(root.join("recorded")).unwrap();
        let recorded = RecordedResponse {
            provider: "tmdb".to_string(),
            method: "GET".to_string(),
            path: "/movie/1?language=en".to_string(),
            content_type: None,
            status: 200,
            body: "exact".to_string(),
        };
        std::fs::write(
            root.join("recorded/exact.json"),
            serde_json::to_vec(&recorded).unwrap(),
        )
        .unwrap();

        let store = ReplayStore::load(root, false);

        // The exact recorded entry wins even though the query-less manifest
        // fixture was inserted first and wildcard-matches the query.
        assert_eq!(
            store
                .upstream("tmdb", "GET", "/movie/1?language=en")
                .unwrap()
                .body,
            Bytes::from_static(b"exact")
        );
        // A query with no exact recording falls back to the wildcard fixture.
        assert_eq!(
            store
                .upstream("tmdb", "GET", "/movie/1?language=fr")
                .unwrap()
                .body,
            Bytes::from_static(b"wild")
        );
    }

    #[test]
    fn record_quota_admits_and_rejects() {
        assert!(within_record_quota(0, 0, false, 0, 10));
        assert!(within_record_quota(
            MAX_RECORDED_ENTRIES - 1,
            0,
            false,
            0,
            10
        ));
        assert!(!within_record_quota(MAX_RECORDED_ENTRIES, 0, false, 0, 10));
        assert!(within_record_quota(MAX_RECORDED_ENTRIES, 0, true, 0, 10));
        assert!(!within_record_quota(0, MAX_RECORDED_BYTES, false, 0, 1));
        assert!(within_record_quota(
            0,
            MAX_RECORDED_BYTES,
            true,
            MAX_RECORDED_BYTES,
            1
        ));
        assert!(!within_record_quota(
            0,
            0,
            false,
            0,
            MAX_REPLAY_BODY_BYTES + 1
        ));
    }

    #[tokio::test]
    async fn record_refuses_oversized_body() {
        let dir = tempfile::tempdir().unwrap();
        let store = ReplayStore::load(dir.path(), true);

        let big = Bytes::from(vec![b'x'; MAX_REPLAY_BODY_BYTES + 1]);
        store.record("tmdb", "GET", "/movie/big", 200, None, &big);

        assert!(store.upstream("tmdb", "GET", "/movie/big").is_none());
        let recorded = dir.path().join("recorded");
        assert!(
            !recorded.exists() || std::fs::read_dir(&recorded).unwrap().count() == 0,
            "an oversized body must not be persisted"
        );
    }

    #[test]
    fn allows_curdir_prefixed_fixture() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("data.json"), br#"{"ok":true}"#).unwrap();

        let manifest = serde_json::json!({
            "metadata": { "movie": [{ "id": "dot", "file": "./data.json" }] }
        });
        std::fs::write(root.join("manifest.json"), manifest.to_string()).unwrap();

        let store = ReplayStore::load(&root, false);
        assert_eq!(
            store.movie("dot"),
            Some(Bytes::from_static(br#"{"ok":true}"#))
        );
    }
}
