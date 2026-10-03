mod common;

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use common::*;
use providarr::api;
use tower::ServiceExt;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

async fn json_body(response: axum::response::Response) -> serde_json::Value {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn maps_movie_through_http_with_mocked_tmdb() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/movie/1"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            include_str!("../tests/fixtures/raw/tmdb-movie-1.json"),
            "application/json",
        ))
        .mount(&server)
        .await;

    let state = app_state(|config| {
        let provider = config.providers.get_mut("tmdb").unwrap();
        provider.base_url = server.uri();
        provider.requests_per_second = 1000.0;
        provider.burst = 1000;
    })
    .await;
    let app = api::router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/radarr/v1/movie/1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let value = json_body(response).await;
    assert_eq!(value["TmdbId"], 1);
    assert_eq!(value["Title"], "Synthetic Feature");
    assert_eq!(value["ImdbId"], "tt0000001");
    assert!(value["Images"].is_array());
    assert!(value["Credits"]["Cast"].is_array());
}

#[tokio::test]
async fn maps_series_through_http_with_mocked_tvdb() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/series/1/extended"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            include_str!("../tests/fixtures/raw/tvdb-series-1-extended.json"),
            "application/json",
        ))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/series/1/translations/eng"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            r#"{"data":{"name":"Synthetic Show (English)","overview":"English."}}"#,
            "application/json",
        ))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/series/1/episodes/default/eng"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            include_str!("../tests/fixtures/raw/tvdb-series-1-episodes.json"),
            "application/json",
        ))
        .mount(&server)
        .await;

    // TMDb series rating lookup
    Mock::given(method("GET"))
        .and(path("/tv/200"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            r#"{"vote_count":100,"vote_average":7.0}"#,
            "application/json",
        ))
        .mount(&server)
        .await;

    // TMDb per-season episode ratings
    Mock::given(method("GET"))
        .and(path("/tv/200/season/1"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            r#"{"episodes":[{"episode_number":1,"vote_count":5,"vote_average":8.0}]}"#,
            "application/json",
        ))
        .mount(&server)
        .await;

    let state = app_state(|config| {
        for provider in config.providers.values_mut() {
            provider.base_url = server.uri();
            provider.requests_per_second = 1000.0;
            provider.burst = 1000;
        }
    })
    .await;
    let app = api::router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/sonarr/v1/tvdb/shows/en/1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let value = json_body(response).await;
    assert_eq!(value["tvdbId"], 1);
    assert_eq!(value["title"], "Synthetic Show (English)");
    assert_eq!(value["imdbId"], "tt0000002");
    assert_eq!(value["tmdbId"], 200);
    assert_eq!(value["rating"]["value"], 7.0);
    let rated = value["episodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["seasonNumber"] == 1 && e["episodeNumber"] == 1)
        .expect("episode 1");
    assert_eq!(rated["rating"]["count"], 5);
    assert_eq!(rated["rating"]["value"], 8.0);
    assert!(!value["episodes"].as_array().unwrap().is_empty());
    assert!(!value["actors"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn movie_changed_merges_all_pages() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/movie/changes"))
        .and(query_param("page", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            r#"{"page":1,"total_pages":2,"results":[{"id":11},{"id":22}]}"#,
            "application/json",
        ))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/movie/changes"))
        .and(query_param("page", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            r#"{"page":2,"total_pages":2,"results":[{"id":33}]}"#,
            "application/json",
        ))
        .mount(&server)
        .await;

    let state = app_state(|config| {
        let provider = config.providers.get_mut("tmdb").unwrap();
        provider.base_url = server.uri();
        provider.requests_per_second = 1000.0;
        provider.burst = 1000;
    })
    .await;
    let app = api::router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/radarr/v1/movie/changed?since=2026-10-01")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let value = json_body(response).await;
    assert_eq!(value, serde_json::json!([11, 22, 33]));
}

#[tokio::test]
async fn movie_search_normalises_plus_encoding() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/search/movie"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            r#"{"page":1,"total_pages":1,"results":[]}"#,
            "application/json",
        ))
        .mount(&server)
        .await;

    let state = app_state(|config| {
        let provider = config.providers.get_mut("tmdb").unwrap();
        provider.base_url = server.uri();
        provider.requests_per_second = 1000.0;
        provider.burst = 1000;
    })
    .await;
    let app = api::router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/radarr/v1/search?q=Synthetic%2BFeature")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let requests = server.received_requests().await.expect("received requests");
    let upstream = requests.first().expect("upstream request");
    let query = upstream
        .url
        .query_pairs()
        .find(|(key, _)| key == "query")
        .map(|(_, value)| value.into_owned())
        .expect("query param");
    assert_eq!(query, "Synthetic Feature");
}
