//! Maps TVDB data into the SkyHook `ShowResource` shape that Theoriarr consumes.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{error::AppError, state::AppState};

use super::fetch_json;

// ---------------------------------------------------------------------------
// Output resources (SkyHook shape, camelCase)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SeriesImage {
    pub cover_type: String,
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Actor {
    pub name: String,
    pub character: String,
    pub image: Option<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Rating {
    pub count: i64,
    pub value: f64,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TimeOfDay {
    pub hours: i64,
    pub minutes: i64,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Season {
    pub season_number: i64,
    pub images: Vec<SeriesImage>,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Episode {
    pub tvdb_id: i64,
    pub season_number: i64,
    pub episode_number: i64,
    pub absolute_episode_number: Option<i64>,
    pub aired_after_season_number: Option<i64>,
    pub aired_before_season_number: Option<i64>,
    pub aired_before_episode_number: Option<i64>,
    pub title: Option<String>,
    pub air_date: Option<String>,
    pub air_date_utc: Option<String>,
    pub runtime: i64,
    pub finale_type: Option<String>,
    pub rating: Option<Rating>,
    pub overview: Option<String>,
    pub image: Option<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct AlternativeTitle {
    pub title: String,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ShowResource {
    pub tvdb_id: i64,
    pub title: String,
    pub overview: Option<String>,
    pub slug: Option<String>,
    pub first_aired: Option<String>,
    pub last_aired: Option<String>,
    pub tv_rage_id: Option<i64>,
    pub tv_maze_id: Option<i64>,
    pub tmdb_id: Option<i64>,
    pub mal_ids: Vec<i64>,
    pub ani_list_ids: Vec<i64>,
    pub anidb_ids: Vec<i64>,
    pub status: Option<String>,
    pub runtime: Option<i64>,
    pub time_of_day: Option<TimeOfDay>,
    pub network: Option<String>,
    pub imdb_id: Option<String>,
    pub original_language: Option<String>,
    pub original_country: Option<String>,
    pub daily: bool,
    pub actors: Vec<Actor>,
    pub genres: Vec<String>,
    pub content_rating: Option<String>,
    pub rating: Option<Rating>,
    pub images: Vec<SeriesImage>,
    pub seasons: Vec<Season>,
    pub episodes: Vec<Episode>,
    pub alternative_titles: Vec<AlternativeTitle>,
}

// ---------------------------------------------------------------------------
// TVDB input
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct Envelope<T> {
    data: T,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct SeriesExtended {
    #[allow(dead_code)]
    id: i64,
    name: Option<String>,
    slug: Option<String>,
    image: Option<String>,
    first_aired: Option<String>,
    last_aired: Option<String>,
    original_language: Option<String>,
    original_country: Option<String>,
    average_runtime: Option<i64>,
    overview: Option<String>,
    status: Option<Named>,
    genres: Option<Vec<Named>>,
    remote_ids: Option<Vec<RemoteId>>,
    content_ratings: Option<Vec<ContentRating>>,
    aliases: Option<Vec<Alias>>,
    characters: Option<Vec<Character>>,
    artworks: Option<Vec<Artwork>>,
    seasons: Option<Vec<TvdbSeason>>,
    airs_time: Option<String>,
    airs_days: Option<AirsDays>,
    latest_network: Option<Named>,
}

/// TVDB weekly air schedule. A show airing five or more days a week is treated as
/// "daily" (soaps, talk shows) which Theoriarr maps to `SeriesTypes.Daily`.
#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct AirsDays {
    monday: Option<bool>,
    tuesday: Option<bool>,
    wednesday: Option<bool>,
    thursday: Option<bool>,
    friday: Option<bool>,
    saturday: Option<bool>,
    sunday: Option<bool>,
}

impl AirsDays {
    fn days_per_week(&self) -> usize {
        [
            self.monday,
            self.tuesday,
            self.wednesday,
            self.thursday,
            self.friday,
            self.saturday,
            self.sunday,
        ]
        .iter()
        .filter(|day| **day == Some(true))
        .count()
    }
}

#[derive(Debug, Deserialize)]
struct Named {
    #[serde(default)]
    name: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RemoteId {
    id: String,
    source_name: Option<String>,
    #[allow(dead_code)]
    #[serde(rename = "type")]
    kind: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct ContentRating {
    name: String,
    country: String,
}

#[derive(Debug, Deserialize)]
struct Alias {
    name: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Character {
    name: Option<String>,
    person_name: Option<String>,
    #[serde(rename = "personImgURL")]
    person_img_url: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Artwork {
    image: String,
    #[serde(rename = "type")]
    kind: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TvdbSeason {
    number: i64,
    image: Option<String>,
    #[serde(rename = "type", default)]
    kind: Option<SeasonTypeRef>,
}

/// TVDB season order metadata (`Aired Order`, `DVD Order`, `Absolute Order`, …).
#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct SeasonTypeRef {
    #[serde(default)]
    id: Option<i64>,
    #[serde(default)]
    name: Option<String>,
    #[serde(rename = "type", default)]
    kind: Option<String>,
}

impl SeasonTypeRef {
    fn is_aired_order(&self) -> bool {
        self.id == Some(1)
            || self
                .name
                .as_deref()
                .is_some_and(|n| n.eq_ignore_ascii_case("Aired Order"))
            || self
                .kind
                .as_deref()
                .is_some_and(|k| k.eq_ignore_ascii_case("official"))
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TvdbEpisode {
    id: i64,
    name: Option<String>,
    aired: Option<String>,
    season_number: Option<i64>,
    number: Option<i64>,
    absolute_number: Option<i64>,
    runtime: Option<i64>,
    overview: Option<String>,
    image: Option<String>,
    finale_type: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct Translation {
    name: Option<String>,
    overview: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct SearchItem {
    name: Option<String>,
    overview: Option<String>,
    #[serde(rename = "image_url")]
    image_url: Option<String>,
    #[serde(rename = "first_air_time")]
    first_air_time: Option<String>,
    year: Option<String>,
    network: Option<String>,
    country: Option<String>,
    #[serde(rename = "primary_language")]
    primary_language: Option<String>,
    #[serde(rename = "tvdb_id")]
    tvdb_id: Option<String>,
    #[serde(rename = "remote_ids")]
    remote_ids: Option<Vec<RemoteId>>,
}

// ---------------------------------------------------------------------------
// Entry points
// ---------------------------------------------------------------------------

pub async fn show(
    state: &AppState,
    tvdb_id: i64,
    language: &str,
) -> Result<ShowResource, AppError> {
    let lang = tvdb_language(language);

    let extended: Envelope<SeriesExtended> =
        fetch_json(state, "tvdb", &format!("/series/{tvdb_id}/extended")).await?;
    let extended = extended.data;

    let translation: Option<Translation> = match fetch_json::<Envelope<Translation>>(
        state,
        "tvdb",
        &format!("/series/{tvdb_id}/translations/{lang}"),
    )
    .await
    {
        Ok(envelope) => Some(envelope.data),
        Err(AppError::NotFound) => None,
        Err(_) => None,
    };

    let episodes = fetch_episodes(state, tvdb_id, &lang).await?;

    let tmdb_id = remote_id(&extended, "TheMovieDB.com").and_then(|v| v.parse::<i64>().ok());
    let rating = match tmdb_id {
        Some(id) => fetch_tv_rating(state, id).await,
        None => None,
    };

    // Episode ratings only exist on TMDb (TVDB's episode list has no score), so
    // fetch them one cached call per season. Skip entirely when there is no TMDb id.
    let episode_ratings = match tmdb_id {
        Some(id) => fetch_episode_ratings(state, id, &distinct_seasons(&episodes)).await,
        None => BTreeMap::new(),
    };

    Ok(map_show(
        tvdb_id,
        &extended,
        translation.as_ref(),
        episodes,
        rating,
        &episode_ratings,
    ))
}

async fn fetch_tv_rating(state: &AppState, tmdb_id: i64) -> Option<Rating> {
    let value: serde_json::Value =
        fetch_json(state, "tmdb", &format!("/tv/{tmdb_id}?language=en-US"))
            .await
            .ok()?;
    let count = value
        .get("vote_count")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let average = value
        .get("vote_average")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    if count == 0 && average == 0.0 {
        return None;
    }
    Some(Rating {
        count,
        value: average,
    })
}

#[derive(Debug, Deserialize, Default)]
struct TmdbSeason {
    #[serde(default)]
    episodes: Vec<TmdbSeasonEpisode>,
}

#[derive(Debug, Deserialize, Default)]
struct TmdbSeasonEpisode {
    episode_number: Option<i64>,
    vote_average: Option<f64>,
    vote_count: Option<i64>,
}

/// The distinct season numbers present in the TVDB episode list.
fn distinct_seasons(episodes: &[TvdbEpisode]) -> Vec<i64> {
    let mut seasons: Vec<i64> = episodes.iter().filter_map(|e| e.season_number).collect();
    seasons.sort_unstable();
    seasons.dedup();
    seasons
}

/// Episode ratings come from TMDb (TVDB's episode list has no score): one cached
/// call per season, keyed by (season, episode).
async fn fetch_episode_ratings(
    state: &AppState,
    tmdb_id: i64,
    season_numbers: &[i64],
) -> BTreeMap<(i64, i64), Rating> {
    let mut ratings = BTreeMap::new();

    for &season in season_numbers {
        let data: TmdbSeason = match fetch_json(
            state,
            "tmdb",
            &format!("/tv/{tmdb_id}/season/{season}?language=en-US"),
        )
        .await
        {
            Ok(data) => data,
            // A season TMDb does not know about (e.g. specials) is not fatal.
            Err(_) => continue,
        };

        for episode in data.episodes {
            let Some(number) = episode.episode_number else {
                continue;
            };
            let count = episode.vote_count.unwrap_or(0);
            let average = episode.vote_average.unwrap_or(0.0);

            if count != 0 || average != 0.0 {
                ratings.insert(
                    (season, number),
                    Rating {
                        count,
                        value: average,
                    },
                );
            }
        }
    }

    ratings
}

fn remote_id(extended: &SeriesExtended, source: &str) -> Option<String> {
    extended.remote_ids.as_ref()?.iter().find_map(|r| {
        r.source_name
            .as_deref()
            .filter(|s| s.eq_ignore_ascii_case(source))
            .map(|_| r.id.clone())
    })
}

pub async fn search(
    state: &AppState,
    term: &str,
    _language: &str,
) -> Result<Vec<ShowResource>, AppError> {
    let encoded: String = url::form_urlencoded::byte_serialize(term.as_bytes()).collect();
    let page: Envelope<Vec<SearchItem>> = fetch_json(
        state,
        "tvdb",
        &format!("/search?query={encoded}&type=series&limit=20"),
    )
    .await?;

    // Search results carry enough to build a lightweight ShowResource, so we avoid
    // three TVDB calls per hit.
    Ok(page
        .data
        .iter()
        .take(20)
        .filter_map(map_search_item)
        .collect())
}

fn map_search_item(item: &SearchItem) -> Option<ShowResource> {
    let tvdb_id: i64 = item.tvdb_id.as_deref()?.parse().ok()?;

    let remote = |source: &str| -> Option<String> {
        item.remote_ids.as_ref()?.iter().find_map(|r| {
            r.source_name
                .as_deref()
                .filter(|s| s.eq_ignore_ascii_case(source))
                .map(|_| r.id.clone())
        })
    };

    let first_aired = date_only(item.first_air_time.as_deref()).or_else(|| {
        item.year
            .as_deref()
            .filter(|y| y.len() == 4)
            .map(|y| format!("{y}-01-01"))
    });

    let images = item
        .image_url
        .clone()
        .filter(|u| !u.is_empty())
        .map(|url| {
            vec![SeriesImage {
                cover_type: "Poster".to_string(),
                url,
            }]
        })
        .unwrap_or_default();

    Some(ShowResource {
        tvdb_id,
        title: item.name.clone().unwrap_or_default(),
        overview: item.overview.clone(),
        first_aired,
        tv_maze_id: remote("TV Maze").and_then(|v| v.parse().ok()),
        tmdb_id: remote("TheMovieDB.com").and_then(|v| v.parse().ok()),
        imdb_id: remote("IMDB").filter(|id| !id.trim().is_empty()),
        status: Some(String::new()),
        network: item.network.clone(),
        original_language: item.primary_language.clone(),
        original_country: item.country.clone(),
        images,
        ..Default::default()
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn fetch_episodes(
    state: &AppState,
    tvdb_id: i64,
    lang: &str,
) -> Result<Vec<TvdbEpisode>, AppError> {
    let mut episodes = Vec::new();

    for page in 0..20 {
        let value: serde_json::Value = fetch_json(
            state,
            "tvdb",
            &format!("/series/{tvdb_id}/episodes/default/{lang}?page={page}"),
        )
        .await?;

        let items = value
            .get("data")
            .and_then(|data| {
                data.get("episodes")
                    .and_then(|e| e.as_array())
                    .or_else(|| data.as_array())
            })
            .cloned()
            .unwrap_or_default();

        let count = items.len();
        for item in items {
            if let Ok(episode) = serde_json::from_value::<TvdbEpisode>(item) {
                episodes.push(episode);
            }
        }

        let has_next = value
            .get("links")
            .and_then(|l| l.get("next"))
            .map(|n| !n.is_null())
            .unwrap_or(false);

        if count == 0 || !has_next {
            break;
        }
    }

    Ok(episodes)
}

fn map_show(
    tvdb_id: i64,
    extended: &SeriesExtended,
    translation: Option<&Translation>,
    episodes: Vec<TvdbEpisode>,
    rating: Option<Rating>,
    episode_ratings: &BTreeMap<(i64, i64), Rating>,
) -> ShowResource {
    let base_name = extended.name.clone().unwrap_or_default();
    let title = translation
        .and_then(|t| t.name.clone())
        .filter(|n| !n.trim().is_empty())
        .unwrap_or(base_name);
    let overview = translation
        .and_then(|t| t.overview.clone())
        .filter(|o| !o.trim().is_empty())
        .or_else(|| extended.overview.clone());

    let remote = |source: &str| -> Option<String> {
        extended
            .remote_ids
            .as_ref()?
            .iter()
            .find(|r| {
                r.source_name
                    .as_deref()
                    .is_some_and(|s| s.eq_ignore_ascii_case(source))
            })
            .map(|r| r.id.clone())
    };

    // TVDB exposes anime ids (and sometimes several per source) via `remoteIds`.
    // The id field can be a comma/space separated list, so parse each token.
    let remote_all = |sources: &[&str]| -> Vec<i64> {
        let Some(ids) = extended.remote_ids.as_ref() else {
            return Vec::new();
        };

        let mut parsed: Vec<i64> = ids
            .iter()
            .filter(|r| {
                r.source_name.as_deref().is_some_and(|name| {
                    sources
                        .iter()
                        .any(|source| name.eq_ignore_ascii_case(source))
                })
            })
            .flat_map(|r| r.id.split([',', ' ']))
            .filter_map(|token| token.trim().parse::<i64>().ok())
            .collect();
        parsed.sort_unstable();
        parsed.dedup();
        parsed
    };

    // TVDB returns one entry per (season, order) pair. Keying only on the
    // season number lets an arbitrary order (DVD/Absolute) win, so take
    // Aired-order artwork first and only fall back to other orders.
    let season_images: BTreeMap<i64, String> = {
        let mut images = BTreeMap::new();
        if let Some(seasons) = extended.seasons.as_ref() {
            for season in seasons
                .iter()
                .filter(|s| s.kind.as_ref().is_some_and(SeasonTypeRef::is_aired_order))
            {
                if let Some(image) = season.image.clone() {
                    images.insert(season.number, image);
                }
            }
            for season in seasons {
                if let Some(image) = season.image.clone() {
                    images.entry(season.number).or_insert(image);
                }
            }
        }
        images
    };

    let mut mapped_episodes: Vec<Episode> = episodes.iter().map(map_episode).collect();
    for episode in &mut mapped_episodes {
        episode.rating = episode_ratings
            .get(&(episode.season_number, episode.episode_number))
            .cloned();
    }
    mapped_episodes.sort_by_key(|e| (e.season_number, e.episode_number));

    // Seasons derive from the episodes we actually have.
    let mut season_numbers: Vec<i64> = mapped_episodes.iter().map(|e| e.season_number).collect();
    season_numbers.dedup();
    let seasons = season_numbers
        .into_iter()
        .map(|number| Season {
            season_number: number,
            images: season_images
                .get(&number)
                .map(|url| {
                    vec![SeriesImage {
                        cover_type: "Poster".to_string(),
                        url: url.clone(),
                    }]
                })
                .unwrap_or_default(),
        })
        .collect();

    ShowResource {
        tvdb_id,
        title,
        overview,
        slug: extended.slug.clone(),
        first_aired: date_only(extended.first_aired.as_deref()),
        last_aired: date_only(extended.last_aired.as_deref()),
        tv_rage_id: None,
        tv_maze_id: remote("TV Maze").and_then(|v| v.parse().ok()),
        tmdb_id: remote("TheMovieDB.com").and_then(|v| v.parse().ok()),
        mal_ids: remote_all(&["MyAnimeList", "MAL", "My Anime List"]),
        ani_list_ids: remote_all(&["AniList", "Ani-List", "Ani List"]),
        anidb_ids: remote_all(&["AniDB", "Ani DB"]),
        // Theoriarr's MapSeriesStatus NREs on a null status, so always emit a
        // non-empty string even when TVDB omits the status object.
        status: Some(
            non_empty(extended.status.as_ref().and_then(|s| s.name.as_deref()))
                .unwrap_or("Continuing")
                .to_string(),
        ),
        runtime: extended.average_runtime,
        time_of_day: parse_air_time(extended.airs_time.as_deref()),
        network: extended
            .latest_network
            .as_ref()
            .and_then(|n| n.name.clone()),
        imdb_id: remote("IMDB").filter(|id| !id.trim().is_empty()),
        original_language: extended.original_language.clone(),
        original_country: extended.original_country.clone(),
        daily: extended
            .airs_days
            .as_ref()
            .is_some_and(|days| days.days_per_week() >= 5),
        actors: extended
            .characters
            .as_ref()
            .map(|chars| {
                chars
                    .iter()
                    .filter_map(|c| {
                        c.person_name.as_ref().map(|name| Actor {
                            name: name.clone(),
                            character: c.name.clone().unwrap_or_default(),
                            image: c.person_img_url.clone(),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default(),
        genres: extended
            .genres
            .as_ref()
            .map(|g| g.iter().filter_map(|n| n.name.clone()).collect())
            .unwrap_or_default(),
        content_rating: pick_content_rating(extended),
        rating,
        images: map_artworks(extended),
        seasons,
        episodes: mapped_episodes,
        alternative_titles: extended
            .aliases
            .as_ref()
            .map(|aliases| {
                let mut titles: Vec<AlternativeTitle> = aliases
                    .iter()
                    .filter(|a| !a.name.trim().is_empty())
                    .map(|a| AlternativeTitle {
                        title: a.name.clone(),
                    })
                    .collect();
                titles.dedup_by(|a, b| a.title == b.title);
                titles
            })
            .unwrap_or_default(),
    }
}

fn map_episode(e: &TvdbEpisode) -> Episode {
    let air_date = date_only(e.aired.as_deref());
    Episode {
        tvdb_id: e.id,
        season_number: e.season_number.unwrap_or_default(),
        episode_number: e.number.unwrap_or_default(),
        absolute_episode_number: e.absolute_number,
        aired_after_season_number: None,
        aired_before_season_number: None,
        aired_before_episode_number: None,
        title: e.name.clone(),
        air_date: air_date.clone(),
        air_date_utc: air_date.map(|d| format!("{d}T00:00:00Z")),
        runtime: e.runtime.unwrap_or_default(),
        finale_type: e.finale_type.clone(),
        rating: None,
        overview: e.overview.clone(),
        image: e.image.clone(),
    }
}

fn map_artworks(extended: &SeriesExtended) -> Vec<SeriesImage> {
    let cover_type = |kind: i64| -> Option<&'static str> {
        match kind {
            1 => Some("Banner"),
            2 => Some("Poster"),
            3 => Some("Fanart"),
            22 => Some("Clearlogo"),
            _ => None,
        }
    };

    let mut images = Vec::new();
    if let Some(artworks) = &extended.artworks {
        for artwork in artworks.iter().filter_map(|a| {
            cover_type(a.kind).map(|ct| SeriesImage {
                cover_type: ct.to_string(),
                url: a.image.clone(),
            })
        }) {
            images.push(artwork);
        }
    }

    // Fall back to the base poster if no artwork endpoint data.
    if images.is_empty()
        && let Some(image) = extended.image.clone().filter(|i| !i.is_empty())
    {
        images.push(SeriesImage {
            cover_type: "Poster".to_string(),
            url: image,
        });
    }

    images.truncate(12);
    images
}

fn pick_content_rating(extended: &SeriesExtended) -> Option<String> {
    let ratings = extended.content_ratings.as_ref()?;
    let original = extended.original_country.as_deref();
    ratings
        .iter()
        .find(|r| original.is_some_and(|country| r.country.eq_ignore_ascii_case(country)))
        .or_else(|| {
            ratings
                .iter()
                .find(|r| r.country.eq_ignore_ascii_case("usa"))
        })
        .map(|r| r.name.clone())
        .filter(|n| !n.is_empty())
}

fn date_only(value: Option<&str>) -> Option<String> {
    // `get` avoids panicking when byte 10 is not a UTF-8 char boundary.
    value?.get(..10).map(str::to_string)
}

/// Returns `Some(trimmed)` only when the value has non-whitespace content.
fn non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|v| !v.is_empty())
}

fn parse_air_time(value: Option<&str>) -> Option<TimeOfDay> {
    let value = value?;
    let (hours, minutes) = value.split_once(':')?;
    Some(TimeOfDay {
        hours: hours.trim().parse().ok()?,
        minutes: minutes.trim().parse().ok()?,
    })
}

/// Map a 2-letter request language to TVDB's 3-letter code.
fn tvdb_language(language: &str) -> String {
    match language.to_ascii_lowercase().as_str() {
        "en" | "eng" => "eng",
        "ja" | "jpn" => "jpn",
        "es" | "spa" => "spa",
        "fr" | "fra" => "fra",
        "de" | "deu" => "deu",
        "pt" | "por" => "por",
        "it" | "ita" => "ita",
        "ru" | "rus" => "rus",
        "ko" | "kor" => "kor",
        "zh" | "zho" => "zho",
        _ => "eng",
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_show_basics() {
        let extended: SeriesExtended = serde_json::from_str(include_str!(
            "../../tests/fixtures/raw/tvdb-series-1-extended.json"
        ))
        .map(|e: Envelope<SeriesExtended>| e.data)
        .expect("parse extended");

        let raw: serde_json::Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/raw/tvdb-series-1-episodes.json"
        ))
        .expect("parse episodes");
        let episodes: Vec<TvdbEpisode> = raw["data"]["episodes"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|v| serde_json::from_value(v).ok())
            .collect();

        let show = map_show(
            1,
            &extended,
            Some(&Translation {
                name: Some("Synthetic Show (English)".to_string()),
                overview: Some("English overview".to_string()),
            }),
            episodes,
            Some(Rating {
                count: 100,
                value: 7.0,
            }),
            &BTreeMap::new(),
        );

        assert_eq!(show.tvdb_id, 1);
        assert_eq!(show.rating.as_ref().map(|r| r.count), Some(100));
        assert_eq!(show.title, "Synthetic Show (English)");
        assert_eq!(show.first_aired.as_deref(), Some("2020-01-02"));
        assert_eq!(show.imdb_id.as_deref(), Some("tt0000002"));
        assert_eq!(show.tmdb_id, Some(200));
        assert_eq!(show.content_rating.as_deref(), Some("TV-14"));
        assert!(!show.actors.is_empty());
        assert!(!show.images.is_empty());
        assert!(!show.episodes.is_empty());
        assert!(!show.seasons.is_empty());
        assert_eq!(show.episodes[0].season_number, 1);
        assert_eq!(show.episodes[0].episode_number, 1);
    }

    #[test]
    fn serialises_with_camel_case_keys() {
        let value = serde_json::to_value(ShowResource {
            tvdb_id: 1,
            title: "T".to_string(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(value["tvdbId"], 1);
        assert_eq!(value["title"], "T");
    }

    #[test]
    fn maps_episode_ratings_from_lookup() {
        let extended = serde_json::from_str::<Envelope<SeriesExtended>>(include_str!(
            "../../tests/fixtures/raw/tvdb-series-1-extended.json"
        ))
        .unwrap()
        .data;

        let raw: serde_json::Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/raw/tvdb-series-1-episodes.json"
        ))
        .unwrap();
        let episodes: Vec<TvdbEpisode> = raw["data"]["episodes"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|v| serde_json::from_value(v).ok())
            .collect();

        let mut ratings = BTreeMap::new();
        ratings.insert(
            (1, 1),
            Rating {
                count: 42,
                value: 8.1,
            },
        );

        let show = map_show(1, &extended, None, episodes, None, &ratings);

        let rated = show
            .episodes
            .iter()
            .find(|e| e.season_number == 1 && e.episode_number == 1)
            .expect("episode 1");
        assert_eq!(rated.rating.as_ref().map(|r| r.count), Some(42));
        assert_eq!(rated.rating.as_ref().map(|r| r.value), Some(8.1));

        let unrated = show
            .episodes
            .iter()
            .find(|e| e.season_number == 1 && e.episode_number == 2)
            .expect("episode 2");
        assert!(unrated.rating.is_none());
    }

    #[test]
    fn maps_anime_remote_ids() {
        let raw = r#"{"data":{"id":1,"name":"Anime","remoteIds":[{"sourceName":"MyAnimeList","id":"123,456"},{"sourceName":"AniList","id":"789"},{"sourceName":"AniDB","id":"42"},{"sourceName":"IMDB","id":"tt1"}]}}"#;
        let extended = serde_json::from_str::<Envelope<SeriesExtended>>(raw)
            .unwrap()
            .data;

        let show = map_show(1, &extended, None, Vec::new(), None, &BTreeMap::new());

        assert_eq!(show.mal_ids, vec![123, 456]);
        assert_eq!(show.ani_list_ids, vec![789]);
        assert_eq!(show.anidb_ids, vec![42]);
        assert_eq!(show.imdb_id.as_deref(), Some("tt1"));
    }

    #[test]
    fn maps_daily_from_air_days() {
        let weekdays = r#"{"data":{"id":1,"name":"Talk Show","airsDays":{"monday":true,"tuesday":true,"wednesday":true,"thursday":true,"friday":true,"saturday":false,"sunday":false}}}"#;
        let daily = serde_json::from_str::<Envelope<SeriesExtended>>(weekdays)
            .unwrap()
            .data;
        assert!(map_show(1, &daily, None, Vec::new(), None, &BTreeMap::new()).daily);

        let weekly = r#"{"data":{"id":2,"name":"Weekly","airsDays":{"monday":true,"tuesday":false,"wednesday":false,"thursday":false,"friday":false,"saturday":false,"sunday":false}}}"#;
        let weekly = serde_json::from_str::<Envelope<SeriesExtended>>(weekly)
            .unwrap()
            .data;
        assert!(!map_show(2, &weekly, None, Vec::new(), None, &BTreeMap::new()).daily);
    }

    #[test]
    fn maps_search_item_without_extra_calls() {
        let raw = r#"{"data":[{"name":"Synthetic Show","overview":"x","image_url":"https://example.com/synth-poster.jpg","first_air_time":"2020-01-02","network":"Synthetic Network","country":"usa","primary_language":"eng","tvdb_id":"1","remote_ids":[{"sourceName":"TV Maze","id":"100"},{"sourceName":"IMDB","id":"tt0000002"}]}]}"#;
        let page: Envelope<Vec<SearchItem>> = serde_json::from_str(raw).unwrap();
        let shows: Vec<ShowResource> = page.data.iter().filter_map(map_search_item).collect();

        assert_eq!(shows.len(), 1);
        assert_eq!(shows[0].tvdb_id, 1);
        assert_eq!(shows[0].tv_maze_id, Some(100));
        assert_eq!(shows[0].imdb_id.as_deref(), Some("tt0000002"));
        assert_eq!(shows[0].first_aired.as_deref(), Some("2020-01-02"));
        assert_eq!(shows[0].images.len(), 1);
    }

    #[test]
    fn date_only_does_not_panic_on_non_ascii() {
        assert_eq!(date_only(Some("2011-04-17")), Some("2011-04-17".into()));
        assert_eq!(
            date_only(Some("2011-04-17T00:00:00Z")),
            Some("2011-04-17".into())
        );
        assert_eq!(date_only(Some("日本語日本語")), None);
        assert_eq!(date_only(Some("short")), None);
        assert_eq!(date_only(None), None);
    }

    #[test]
    fn status_is_never_null() {
        let raw = r#"{"data":{"id":1,"name":"No Status"}}"#;
        let extended = serde_json::from_str::<Envelope<SeriesExtended>>(raw)
            .unwrap()
            .data;
        assert_eq!(
            map_show(1, &extended, None, Vec::new(), None, &BTreeMap::new())
                .status
                .as_deref(),
            Some("Continuing")
        );

        let ended = r#"{"data":{"id":1,"name":"Ended Show","status":{"id":2,"name":"Ended"}}}"#;
        let extended = serde_json::from_str::<Envelope<SeriesExtended>>(ended)
            .unwrap()
            .data;
        assert_eq!(
            map_show(1, &extended, None, Vec::new(), None, &BTreeMap::new())
                .status
                .as_deref(),
            Some("Ended")
        );
    }

    #[test]
    fn content_rating_prefers_original_then_us_then_none() {
        let mixed = r#"{"data":{"id":1,"name":"Show","originalCountry":"jpn","contentRatings":[{"name":"M","country":"aus"},{"name":"TV-PG","country":"usa"}]}}"#;
        let extended = serde_json::from_str::<Envelope<SeriesExtended>>(mixed)
            .unwrap()
            .data;
        assert_eq!(pick_content_rating(&extended).as_deref(), Some("TV-PG"));

        // Original country wins when it has a rating.
        let original = r#"{"data":{"id":1,"name":"Show","originalCountry":"aus","contentRatings":[{"name":"M","country":"aus"},{"name":"TV-PG","country":"usa"}]}}"#;
        let extended = serde_json::from_str::<Envelope<SeriesExtended>>(original)
            .unwrap()
            .data;
        assert_eq!(pick_content_rating(&extended).as_deref(), Some("M"));

        // Unrelated-only ratings must not leak through.
        let unrelated = r#"{"data":{"id":1,"name":"Show","originalCountry":"jpn","contentRatings":[{"name":"M","country":"aus"}]}}"#;
        let extended = serde_json::from_str::<Envelope<SeriesExtended>>(unrelated)
            .unwrap()
            .data;
        assert_eq!(pick_content_rating(&extended), None);
    }

    #[test]
    fn season_artwork_prefers_aired_order() {
        let extended: SeriesExtended = serde_json::from_str(include_str!(
            "../../tests/fixtures/raw/tvdb-series-2-extended.json"
        ))
        .map(|e: Envelope<SeriesExtended>| e.data)
        .expect("parse extended");

        let raw: serde_json::Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/raw/tvdb-series-2-episodes.json"
        ))
        .expect("parse episodes");
        let episodes: Vec<TvdbEpisode> = raw["data"]["episodes"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|v| serde_json::from_value(v).ok())
            .collect();

        let show = map_show(2, &extended, None, episodes, None, &BTreeMap::new());
        let season_one = show
            .seasons
            .iter()
            .find(|s| s.season_number == 1)
            .expect("season 1");

        // The DVD-order entry for season 1 has a different image; the Aired
        // Order one must win.
        assert_eq!(
            season_one.images.first().map(|i| i.url.as_str()),
            Some("https://example.com/synth2-season-1-aired.jpg")
        );
    }
}
