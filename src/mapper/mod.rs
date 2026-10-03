//! Maps TMDb data into the Radarr `MovieResource` shape that Theoriarr consumes.
//!
//! Kept deliberately close to Radarr's schema so Theoriarr's existing mapper keeps
//! working while Providarr takes over from `api.radarr.video`.

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{error::AppError, state::AppState};

pub mod series;

const TMDB_IMAGE_BASE: &str = "https://image.tmdb.org/t/p/original";
const APPEND: &str = "credits,images,external_ids,release_dates,alternative_titles,translations,collection,videos,recommendations,keywords";

/// Upper bound on `/movie/changes` pages fetched in a single call. TMDb allows
/// far more, but the mapper only needs a bounded sample to stay responsive.
const MAX_CHANGE_PAGES: i64 = 20;

/// Upper bound on the `parts` embedded in a collection. A collection can list
/// thousands of entries; only a bounded sample is mapped per request.
const MAX_COLLECTION_PARTS: usize = 250;

// ---------------------------------------------------------------------------
// Output resources (Radarr shape)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct RatingItem {
    pub count: i64,
    pub value: f64,
    pub origin: Option<String>,
    #[serde(rename = "Type")]
    pub rating_type: String,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct MovieRatings {
    pub tmdb: Option<RatingItem>,
    pub imdb: Option<RatingItem>,
    pub metacritic: Option<RatingItem>,
    pub rotten_tomatoes: Option<RatingItem>,
    pub trakt: Option<RatingItem>,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ImageResource {
    pub cover_type: String,
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct AlternativeTitleResource {
    pub title: String,
    #[serde(rename = "Type")]
    pub kind: String,
    pub language: String,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct TranslationResource {
    pub title: String,
    pub overview: String,
    pub language: String,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct RecommendationResource {
    pub tmdb_id: i64,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct CertificationResource {
    pub country: String,
    pub certification: String,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct CastResource {
    pub name: String,
    pub order: i64,
    pub character: String,
    pub tmdb_id: i64,
    pub credit_id: String,
    pub images: Vec<ImageResource>,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct CrewResource {
    pub name: String,
    pub order: i64,
    pub job: String,
    pub department: String,
    pub tmdb_id: i64,
    pub credit_id: String,
    pub images: Vec<ImageResource>,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct Credits {
    pub cast: Vec<CastResource>,
    pub crew: Vec<CrewResource>,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct CollectionResource {
    pub name: String,
    pub overview: Option<String>,
    pub tmdb_id: i64,
    pub images: Vec<ImageResource>,
    pub translations: Vec<TranslationResource>,
    pub parts: Vec<MovieResource>,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct MovieResource {
    pub tmdb_id: i64,
    pub imdb_id: Option<String>,
    pub overview: Option<String>,
    pub title: String,
    pub original_title: Option<String>,
    pub title_slug: String,
    pub ratings: Vec<RatingItem>,
    pub movie_ratings: MovieRatings,
    pub runtime: Option<i64>,
    pub images: Vec<ImageResource>,
    pub genres: Vec<String>,
    pub keywords: Vec<String>,
    pub year: i64,
    pub premier: Option<String>,
    pub in_cinema: Option<String>,
    pub physical_release: Option<String>,
    pub digital_release: Option<String>,
    pub alternative_titles: Vec<AlternativeTitleResource>,
    pub translations: Vec<TranslationResource>,
    pub credits: Credits,
    pub studio: Option<String>,
    pub youtube_trailer_id: Option<String>,
    pub certifications: Vec<CertificationResource>,
    pub status: Option<String>,
    pub collection: Option<CollectionResource>,
    pub original_language: Option<String>,
    pub homepage: Option<String>,
    pub recommendations: Vec<RecommendationResource>,
    pub popularity: Option<f64>,
}

// ---------------------------------------------------------------------------
// TMDb input
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize, Default)]
struct TmdbMovie {
    id: i64,
    imdb_id: Option<String>,
    title: Option<String>,
    original_title: Option<String>,
    overview: Option<String>,
    original_language: Option<String>,
    runtime: Option<i64>,
    popularity: Option<f64>,
    release_date: Option<String>,
    homepage: Option<String>,
    poster_path: Option<String>,
    backdrop_path: Option<String>,
    vote_average: Option<f64>,
    vote_count: Option<i64>,
    status: Option<String>,
    #[serde(default)]
    production_companies: Vec<TmdbNamed>,
    #[serde(default)]
    genres: Vec<TmdbNamed>,
    belongs_to_collection: Option<TmdbCollectionRef>,
    credits: Option<TmdbCredits>,
    external_ids: Option<TmdbExternalIds>,
    #[serde(default)]
    release_dates: TmdbReleaseDates,
    #[serde(default)]
    alternative_titles: TmdbAlternativeTitles,
    #[serde(default)]
    translations: TmdbTranslations,
    #[serde(default)]
    recommendations: TmdbPaged<TmdbRecommendation>,
    #[serde(default)]
    videos: TmdbVideos,
    keywords: Option<TmdbKeywords>,
}

#[derive(Debug, Deserialize)]
struct TmdbNamed {
    name: String,
}

#[derive(Debug, Deserialize)]
struct TmdbExternalIds {
    imdb_id: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct TmdbCollectionRef {
    id: i64,
    name: String,
}

#[derive(Debug, Deserialize, Default)]
struct TmdbCredits {
    #[serde(default)]
    cast: Vec<TmdbCast>,
    #[serde(default)]
    crew: Vec<TmdbCrew>,
}

#[derive(Debug, Deserialize)]
struct TmdbCast {
    id: i64,
    name: String,
    #[serde(default)]
    order: i64,
    character: Option<String>,
    credit_id: Option<String>,
    profile_path: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TmdbCrew {
    id: i64,
    name: String,
    #[serde(default)]
    order: i64,
    job: Option<String>,
    department: Option<String>,
    credit_id: Option<String>,
    profile_path: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct TmdbReleaseDates {
    #[serde(default)]
    results: Vec<TmdbReleaseCountry>,
}

#[derive(Debug, Deserialize)]
struct TmdbReleaseCountry {
    iso_3166_1: String,
    #[serde(default)]
    release_dates: Vec<TmdbReleaseDate>,
}

#[derive(Debug, Deserialize)]
struct TmdbReleaseDate {
    #[serde(default)]
    certification: String,
    #[serde(rename = "type", default)]
    release_type: i64,
    release_date: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct TmdbAlternativeTitles {
    #[serde(default)]
    titles: Vec<TmdbAltTitle>,
}

#[derive(Debug, Deserialize)]
struct TmdbAltTitle {
    title: String,
    #[serde(rename = "type", default)]
    kind: String,
    #[serde(default)]
    iso_639_1: String,
    #[serde(default)]
    #[allow(dead_code)]
    iso_3166_1: String,
}

#[derive(Debug, Deserialize, Default)]
struct TmdbTranslations {
    #[serde(default)]
    translations: Vec<TmdbTranslation>,
}

#[derive(Debug, Deserialize)]
struct TmdbTranslation {
    #[serde(default)]
    iso_639_1: String,
    data: TmdbTranslationData,
}

#[derive(Debug, Deserialize, Default)]
struct TmdbTranslationData {
    #[serde(default)]
    title: String,
    #[serde(default)]
    overview: String,
}

#[derive(Debug, Deserialize)]
struct TmdbPaged<T> {
    results: Vec<T>,
    #[serde(default)]
    page: i64,
    #[serde(default)]
    total_pages: i64,
}

impl<T> Default for TmdbPaged<T> {
    fn default() -> Self {
        Self {
            results: Vec::new(),
            page: 0,
            total_pages: 0,
        }
    }
}

#[derive(Debug, Deserialize)]
struct TmdbRecommendation {
    id: i64,
    title: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct TmdbVideos {
    #[serde(default)]
    results: Vec<TmdbVideo>,
}

#[derive(Debug, Deserialize)]
struct TmdbVideo {
    key: String,
    site: Option<String>,
    #[serde(rename = "type")]
    kind: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct TmdbKeywords {
    #[serde(default)]
    keywords: Vec<TmdbNamed>,
}

#[derive(Debug, Deserialize)]
struct TmdbSearchResult {
    id: i64,
}

#[derive(Debug, Deserialize, Default)]
struct TmdbFind {
    #[serde(default)]
    movie_results: Vec<TmdbSearchResult>,
}

#[derive(Debug, Deserialize, Default)]
struct TmdbCollection {
    id: i64,
    name: String,
    overview: Option<String>,
    poster_path: Option<String>,
    backdrop_path: Option<String>,
    #[serde(default)]
    parts: Vec<TmdbMovie>,
}

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

pub async fn movie(state: &AppState, tmdb_id: i64) -> Result<MovieResource, AppError> {
    let path = format!("/movie/{tmdb_id}?language=en-US&append_to_response={APPEND}");
    let tmdb: TmdbMovie = fetch_json(state, "tmdb", &path).await?;
    Ok(map_movie(&tmdb))
}

pub async fn movie_by_imdb(
    state: &AppState,
    imdb_id: &str,
) -> Result<Vec<MovieResource>, AppError> {
    // Reject anything that is not a canonical IMDb id before interpolating it
    // into the provider path (`tt` followed by one or more ASCII digits).
    if !is_valid_imdb_id(imdb_id) {
        return Err(AppError::NotFound);
    }
    let path = format!("/find/{imdb_id}?external_source=imdb_id");
    let found: TmdbFind = fetch_json(state, "tmdb", &path).await?;
    let mut out = Vec::new();
    for result in found.movie_results.into_iter().take(1) {
        out.push(movie(state, result.id).await?);
    }
    Ok(out)
}

pub async fn search(
    state: &AppState,
    query: &str,
    year: Option<&str>,
) -> Result<Vec<MovieResource>, AppError> {
    let encoded = encode_query_value(query);
    let mut path = format!("/search/movie?language=en-US&include_adult=false&query={encoded}");
    if let Some(year) = year
        && is_valid_year(year)
    {
        path.push_str(&format!("&year={year}"));
    }

    let page: TmdbPaged<TmdbSearchResult> = fetch_json(state, "tmdb", &path).await?;
    hydrate(state, page.results).await
}

pub async fn list(state: &AppState, kind: &str) -> Result<Vec<MovieResource>, AppError> {
    let path = match kind {
        "trending" => "/trending/movie/week?language=en-US",
        "popular" => "/movie/popular?language=en-US",
        _ => return Err(AppError::NotFound),
    };
    let page: TmdbPaged<TmdbSearchResult> = fetch_json(state, "tmdb", path).await?;
    hydrate(state, page.results).await
}

pub async fn bulk(state: &AppState, ids: &[i64]) -> Result<Vec<MovieResource>, AppError> {
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        out.push(movie(state, *id).await?);
    }
    Ok(out)
}

/// TMDb `/movie/changes` — movie ids changed since `since` (capped to the 14-day window).
pub async fn changed_movies(state: &AppState, since: Option<&str>) -> Result<Vec<i64>, AppError> {
    use chrono::{Duration, Utc};

    let today = Utc::now().date_naive();
    let mut start = since
        .and_then(parse_since)
        .unwrap_or_else(|| today - Duration::days(7));
    if (today - start).num_days() > 14 {
        start = today - Duration::days(14);
    }
    if start > today {
        start = today;
    }

    let start_date = start.format("%Y-%m-%d");
    let end_date = today.format("%Y-%m-%d");

    let mut ids = Vec::new();
    let mut page_number: i64 = 1;
    loop {
        let path = format!(
            "/movie/changes?start_date={start_date}&end_date={end_date}&page={page_number}"
        );
        let page: TmdbPaged<TmdbChange> = fetch_json(state, "tmdb", &path).await?;

        let current = if page.page <= 0 {
            page_number
        } else {
            page.page
        };
        let total = page.total_pages;
        let empty = page.results.is_empty();
        ids.extend(page.results.into_iter().map(|change| change.id));

        // Stop on the last page, an empty page, when the response omits
        // pagination metadata entirely, or once our own page budget is spent.
        if empty || total <= 0 || current >= total || page_number >= MAX_CHANGE_PAGES {
            break;
        }
        page_number += 1;
    }
    Ok(ids)
}

fn parse_since(value: &str) -> Option<chrono::NaiveDate> {
    if let Ok(datetime) = chrono::DateTime::parse_from_rfc3339(value) {
        return Some(datetime.date_naive());
    }
    if let Ok(datetime) = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S") {
        return Some(datetime.date());
    }
    // `get` returns `None` instead of panicking when the slice end is not on a
    // UTF-8 char boundary (e.g. a non-ASCII `since` value).
    let date = value.get(..10)?;
    chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d").ok()
}

/// The incoming search term may already contain `+` standing in for a space
/// (form encoding). Normalize those back before re-encoding so we don't emit a
/// literal `%2B` and turn a multi-word title into one long word for TMDb.
fn encode_query_value(value: &str) -> String {
    let normalized = value.replace('+', " ");
    url::form_urlencoded::byte_serialize(normalized.as_bytes()).collect()
}

/// A canonical IMDb id: `tt` followed by one to ten ASCII digits. Request
/// values are percent-decoded upstream, so reject anything with path/query
/// metacharacters before it reaches the provider path. The digit-length cap
/// also keeps upstream-supplied ids from being reflected unbounded.
fn is_valid_imdb_id(value: &str) -> bool {
    value.strip_prefix("tt").is_some_and(|digits| {
        !digits.is_empty() && digits.len() <= 10 && digits.bytes().all(|b| b.is_ascii_digit())
    })
}

/// A search `year` is only usable when it is exactly four ASCII digits.
fn is_valid_year(value: &str) -> bool {
    value.len() == 4 && value.bytes().all(|b| b.is_ascii_digit())
}

#[derive(Debug, Deserialize)]
struct TmdbChange {
    id: i64,
}

pub async fn collection(state: &AppState, tmdb_id: i64) -> Result<CollectionResource, AppError> {
    let path =
        format!("/collection/{tmdb_id}?language=en-US&append_to_response=images,translations");
    let tmdb: TmdbCollection = fetch_json(state, "tmdb", &path).await?;
    Ok(map_collection(&tmdb))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Search/list results are partial; fetch full details (cached) so `MapMovie`
/// has everything it expects. Capped to avoid a stampede.
async fn hydrate(
    state: &AppState,
    results: Vec<TmdbSearchResult>,
) -> Result<Vec<MovieResource>, AppError> {
    let limit = state.config.search.hydrate_limit;
    let mut out = Vec::new();
    for result in results.into_iter().take(limit) {
        match movie(state, result.id).await {
            Ok(movie) => out.push(movie),
            Err(AppError::NotFound) => {}
            Err(err) => return Err(err),
        }
    }
    Ok(out)
}

async fn fetch_json<T: DeserializeOwned>(
    state: &AppState,
    provider: &str,
    path: &str,
) -> Result<T, AppError> {
    let response = state.registry.fetch(provider, "GET", path).await?;
    crate::cache_report::record(
        response.cached,
        response.stale,
        response.replayed,
        response.cache_created_at,
        response.cache_expires_at,
    );
    if response.status == 404 {
        return Err(AppError::NotFound);
    }
    if !(200..300).contains(&response.status) {
        return Err(AppError::Upstream {
            status: response.status,
            body: String::from_utf8_lossy(&response.body)
                .chars()
                .take(300)
                .collect(),
        });
    }
    serde_json::from_slice(&response.body).map_err(AppError::Serde)
}

fn map_movie(t: &TmdbMovie) -> MovieResource {
    let release = t.release_date.as_deref().unwrap_or("");
    let year = release
        .get(0..4)
        .and_then(|y| y.parse::<i64>().ok())
        .unwrap_or_default();
    let in_cinema = iso_datetime(release);

    let (certifications, digital, physical) = release_info(t);
    let images = movie_images(t.poster_path.as_deref(), t.backdrop_path.as_deref());

    let vote_value = t.vote_average.unwrap_or_default();
    let vote_count = t.vote_count.unwrap_or_default();
    let tmdb_rating = RatingItem {
        count: vote_count,
        value: vote_value,
        origin: Some("Tmdb".to_string()),
        rating_type: "User".to_string(),
    };

    let credits = map_credits(t.credits.as_ref());

    // Theoriarr dereferences these unconditionally, so never emit null. Fall
    // back to the localized title / `en` when TMDb omits the original fields.
    let original_language = non_empty(t.original_language.as_deref())
        .unwrap_or("en")
        .to_string();
    let original_title = non_empty(t.original_title.as_deref())
        .or_else(|| non_empty(t.title.as_deref()))
        .unwrap_or("")
        .to_string();

    MovieResource {
        tmdb_id: t.id,
        imdb_id: t
            .imdb_id
            .clone()
            .or_else(|| t.external_ids.as_ref().and_then(|e| e.imdb_id.clone()))
            .filter(|id| is_valid_imdb_id(id)),
        overview: t.overview.clone(),
        title: t.title.clone().unwrap_or_default(),
        original_title: Some(original_title),
        title_slug: t.id.to_string(),
        ratings: vec![tmdb_rating.clone()],
        movie_ratings: MovieRatings {
            tmdb: Some(tmdb_rating),
            ..Default::default()
        },
        runtime: t.runtime,
        images,
        genres: t.genres.iter().map(|g| g.name.clone()).collect(),
        keywords: t
            .keywords
            .as_ref()
            .map(|k| k.keywords.iter().map(|w| w.name.clone()).collect())
            .unwrap_or_default(),
        year,
        premier: in_cinema.clone(),
        in_cinema,
        physical_release: physical,
        digital_release: digital,
        alternative_titles: t
            .alternative_titles
            .titles
            .iter()
            .map(|a| AlternativeTitleResource {
                title: a.title.clone(),
                kind: a.kind.clone(),
                // TMDb keys alternative titles by country; prefer the actual
                // language code when present and fall back to the movie's
                // original language rather than leaking a country code.
                language: non_empty(Some(a.iso_639_1.as_str()))
                    .or_else(|| non_empty(t.original_language.as_deref()))
                    .unwrap_or("en")
                    .to_string(),
            })
            .collect(),
        translations: t
            .translations
            .translations
            .iter()
            // Theoriarr overrides the title from the first translation matching the
            // configured language; TMDb often ships an empty title for the original
            // language, which would blank it out. Drop those.
            .filter(|tr| !tr.data.title.trim().is_empty())
            .map(|tr| TranslationResource {
                title: tr.data.title.clone(),
                overview: tr.data.overview.clone(),
                language: tr.iso_639_1.clone(),
            })
            .collect(),
        credits,
        studio: t.production_companies.first().map(|c| c.name.clone()),
        youtube_trailer_id: trailer_id(t),
        certifications,
        status: t.status.clone(),
        collection: t
            .belongs_to_collection
            .as_ref()
            .map(|c| CollectionResource {
                name: c.name.clone(),
                overview: None,
                tmdb_id: c.id,
                images: Vec::new(),
                translations: Vec::new(),
                parts: Vec::new(),
            }),
        original_language: Some(original_language),
        homepage: t.homepage.clone().filter(|h| !h.is_empty()),
        recommendations: t
            .recommendations
            .results
            .iter()
            .map(|r| RecommendationResource {
                tmdb_id: r.id,
                name: r.title.clone().unwrap_or_default(),
            })
            .collect(),
        popularity: t.popularity,
    }
}

fn map_collection(c: &TmdbCollection) -> CollectionResource {
    CollectionResource {
        name: c.name.clone(),
        overview: c.overview.clone(),
        tmdb_id: c.id,
        images: movie_images(c.poster_path.as_deref(), c.backdrop_path.as_deref()),
        translations: Vec::new(),
        parts: c
            .parts
            .iter()
            .take(MAX_COLLECTION_PARTS)
            .map(map_movie)
            .collect(),
    }
}

fn map_credits(credits: Option<&TmdbCredits>) -> Credits {
    let Some(credits) = credits else {
        return Credits::default();
    };

    Credits {
        cast: credits
            .cast
            .iter()
            .map(|c| CastResource {
                name: c.name.clone(),
                order: c.order,
                character: c.character.clone().unwrap_or_default(),
                tmdb_id: c.id,
                credit_id: c.credit_id.clone().unwrap_or_default(),
                images: person_image(c.profile_path.as_deref()),
            })
            .collect(),
        crew: credits
            .crew
            .iter()
            .map(|c| CrewResource {
                name: c.name.clone(),
                order: c.order,
                job: c.job.clone().unwrap_or_default(),
                department: c.department.clone().unwrap_or_default(),
                tmdb_id: c.id,
                credit_id: c.credit_id.clone().unwrap_or_default(),
                images: person_image(c.profile_path.as_deref()),
            })
            .collect(),
    }
}

fn release_info(t: &TmdbMovie) -> (Vec<CertificationResource>, Option<String>, Option<String>) {
    let mut certifications = Vec::new();
    let mut digital: Option<String> = None;
    let mut physical: Option<String> = None;

    for country in &t.release_dates.results {
        for release in &country.release_dates {
            if !release.certification.trim().is_empty() {
                certifications.push(CertificationResource {
                    country: country.iso_3166_1.clone(),
                    certification: release.certification.trim().to_string(),
                });
            }

            if let Some(date) = release.release_date.as_deref().and_then(iso_datetime) {
                match release.release_type {
                    4 => digital = Some(earliest(digital, date)),
                    5 => physical = Some(earliest(physical, date)),
                    _ => {}
                }
            }
        }
    }

    (certifications, digital, physical)
}

fn earliest(current: Option<String>, candidate: String) -> String {
    match current {
        Some(existing) if existing <= candidate => existing,
        _ => candidate,
    }
}

fn trailer_id(t: &TmdbMovie) -> Option<String> {
    t.videos
        .results
        .iter()
        .find(|v| v.site.as_deref() == Some("YouTube") && v.kind.as_deref() == Some("Trailer"))
        .or_else(|| {
            t.videos
                .results
                .iter()
                .find(|v| v.site.as_deref() == Some("YouTube"))
        })
        .map(|v| v.key.clone())
}

fn iso_datetime(date: &str) -> Option<String> {
    date.get(..10).map(|d| format!("{d}T00:00:00Z"))
}

/// Returns `Some(trimmed)` only when the value has non-whitespace content.
fn non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|v| !v.is_empty())
}

fn image_url(path: &str) -> String {
    format!("{TMDB_IMAGE_BASE}{path}")
}

fn image(cover_type: &str, path: &str) -> ImageResource {
    ImageResource {
        cover_type: cover_type.to_string(),
        url: image_url(path),
    }
}

fn person_image(profile_path: Option<&str>) -> Vec<ImageResource> {
    profile_path
        .filter(|p| !p.is_empty())
        .map(|p| vec![image("Headshot", p)])
        .unwrap_or_default()
}

fn movie_images(poster: Option<&str>, backdrop: Option<&str>) -> Vec<ImageResource> {
    let mut images = Vec::new();
    if let Some(poster) = poster.filter(|p| !p.is_empty()) {
        images.push(image("Poster", poster));
    }
    if let Some(backdrop) = backdrop.filter(|b| !b.is_empty()) {
        images.push(image("Fanart", backdrop));
    }
    images
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> TmdbMovie {
        let raw = std::fs::read_to_string("tests/fixtures/raw/tmdb-movie-1.json")
            .expect("raw tmdb fixture");
        serde_json::from_str(&raw).expect("parse tmdb fixture")
    }

    #[test]
    fn maps_core_movie_fields() {
        let movie = map_movie(&fixture());

        assert_eq!(movie.tmdb_id, 1);
        assert_eq!(movie.title, "Synthetic Feature");
        assert_eq!(movie.original_title.as_deref(), Some("Synthetic Original"));
        assert_eq!(movie.year, 2020);
        assert_eq!(movie.runtime, Some(120));
        assert_eq!(movie.imdb_id.as_deref(), Some("tt0000001"));

        // Lists must never be null (Theoriarr calls .Select on them).
        assert!(!movie.images.is_empty());
        assert!(!movie.certifications.is_empty());
        assert!(!movie.alternative_titles.is_empty());
        assert!(!movie.translations.is_empty());
        assert!(
            movie
                .translations
                .iter()
                .all(|t| !t.title.trim().is_empty()),
            "empty-title translations must be dropped"
        );
        assert!(!movie.credits.cast.is_empty());

        // Ratings + genres present
        assert!(movie.movie_ratings.tmdb.is_some());
        assert!(!movie.genres.is_empty());
    }

    #[test]
    fn encodes_with_pascal_case_keys() {
        let value = serde_json::to_value(map_movie(&fixture())).unwrap();
        assert_eq!(value["TmdbId"], 1);
        assert_eq!(value["Title"], "Synthetic Feature");
        assert!(value["MovieRatings"]["Tmdb"]["Value"].is_number());
        assert!(value["Credits"]["Cast"].is_array());
        assert!(value["Recommendations"].is_array());
        assert!(value["Images"].is_array());
        assert_eq!(value["Ratings"][0]["Type"], "User");
    }

    #[test]
    fn parses_since_formats() {
        let expected = chrono::NaiveDate::from_ymd_opt(2026, 10, 1).unwrap();
        assert_eq!(parse_since("2026-10-01T12:00:00Z"), Some(expected));
        assert_eq!(parse_since("2026-10-01"), Some(expected));
        assert_eq!(parse_since("2026-10-01T00:00:00"), Some(expected));
        assert_eq!(parse_since("2026-10-01junk"), Some(expected));
        assert_eq!(parse_since("garbage"), None);
    }

    #[test]
    fn parse_since_does_not_panic_on_non_ascii() {
        // `&value[..10]` would panic: byte 10 lands inside a multi-byte char.
        assert_eq!(parse_since("日本語日本語日本語"), None);
        assert_eq!(parse_since("2026-10-🦀"), None);
        assert_eq!(parse_since(""), None);
    }

    #[test]
    fn search_query_normalises_plus_to_space() {
        assert_eq!(encode_query_value("Synthetic Feature"), "Synthetic+Feature");
        // A pre-encoded `+` must not survive as a literal `%2B`.
        assert_eq!(encode_query_value("Synthetic+Feature"), "Synthetic+Feature");
        assert_eq!(encode_query_value("a&b=c"), "a%26b%3Dc");
    }

    #[test]
    fn validates_imdb_ids() {
        assert!(is_valid_imdb_id("tt0000001"));
        assert!(is_valid_imdb_id("tt1"));
        // Ten digits is the cap; longer ids are rejected.
        assert!(is_valid_imdb_id("tt1234567890"));
        assert!(!is_valid_imdb_id("tt"));
        assert!(!is_valid_imdb_id("0000001"));
        assert!(!is_valid_imdb_id("tt12ab"));
        assert!(!is_valid_imdb_id("tt12345678901"));
        assert!(!is_valid_imdb_id("tt1/../../evil?x=1"));
        assert!(!is_valid_imdb_id("tt1%2F.."));
    }

    #[test]
    fn movie_drops_invalid_upstream_imdb_id() {
        let invalid = map_movie(
            &serde_json::from_str::<TmdbMovie>(r#"{"id":1,"imdb_id":"tt12345678901"}"#).unwrap(),
        );
        assert_eq!(invalid.imdb_id, None);

        let valid = map_movie(
            &serde_json::from_str::<TmdbMovie>(r#"{"id":1,"imdb_id":"tt0000001"}"#).unwrap(),
        );
        assert_eq!(valid.imdb_id.as_deref(), Some("tt0000001"));
    }

    #[test]
    fn collection_parts_are_capped() {
        let parts: Vec<String> = (0..(MAX_COLLECTION_PARTS + 10))
            .map(|i| format!(r#"{{"id":{i},"title":"Part {i}"}}"#))
            .collect();
        let raw = format!(
            r#"{{"id":1,"name":"Collection","parts":[{}]}}"#,
            parts.join(",")
        );
        let collection: TmdbCollection = serde_json::from_str(&raw).unwrap();
        assert_eq!(
            map_collection(&collection).parts.len(),
            MAX_COLLECTION_PARTS
        );
    }

    #[test]
    fn validates_search_year() {
        assert!(is_valid_year("2020"));
        assert!(!is_valid_year(""));
        assert!(!is_valid_year("20"));
        assert!(!is_valid_year("20200"));
        assert!(!is_valid_year("202a"));
        assert!(!is_valid_year("2020&x=1"));
    }

    #[test]
    fn movie_defaults_non_null_original_fields() {
        let raw =
            r#"{"id":1,"title":"Title","original_title":"","original_language":"","imdb_id":""}"#;
        let movie = map_movie(&serde_json::from_str::<TmdbMovie>(raw).unwrap());

        assert_eq!(movie.original_title.as_deref(), Some("Title"));
        assert_eq!(movie.original_language.as_deref(), Some("en"));
        assert_eq!(movie.imdb_id, None);
    }

    #[test]
    fn alternative_titles_use_language_not_country() {
        let raw = r#"{"id":1,"title":"Title","original_language":"zh","alternative_titles":{"titles":[{"title":"A","type":"","iso_639_1":"fr","iso_3166_1":"FR"},{"title":"B","type":"","iso_3166_1":"CN"}]}}"#;
        let movie = map_movie(&serde_json::from_str::<TmdbMovie>(raw).unwrap());

        assert_eq!(movie.alternative_titles[0].language, "fr");
        assert_eq!(movie.alternative_titles[1].language, "zh");
    }

    #[test]
    fn iso_datetime_handles_short_and_non_ascii() {
        assert_eq!(
            iso_datetime("2019-02-05"),
            Some("2019-02-05T00:00:00Z".into())
        );
        assert_eq!(iso_datetime("2019"), None);
        assert_eq!(iso_datetime("日本語日本語"), None);
    }
}
