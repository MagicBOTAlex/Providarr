mod common;

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use common::*;
use providarr::api;
use tower::ServiceExt;

async fn replay_app() -> axum::Router {
    let state = app_state(|config| {
        config.replay.enabled = true;
        config.replay.dir = "tests/fixtures".to_string();
    })
    .await;
    api::router(state)
}

async fn json_body(response: axum::response::Response) -> serde_json::Value {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn serves_movie_metadata_fixture() {
    let app = replay_app().await;
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
    assert_eq!(
        response
            .headers()
            .get("x-providarr-source")
            .unwrap()
            .to_str()
            .unwrap(),
        "fixture"
    );

    let value = json_body(response).await;
    assert_eq!(value["TmdbId"], 1);
    assert_eq!(value["Title"], "Synthetic Feature");
}

#[tokio::test]
async fn serves_series_metadata_fixture() {
    let app = replay_app().await;
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
    assert!(
        value["title"].as_str().unwrap().contains("Synthetic"),
        "unexpected title: {}",
        value["title"]
    );
}

#[tokio::test]
async fn proxy_replays_raw_upstream_fixture() {
    let app = replay_app().await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/tmdb/movie/1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("x-providarr-replay")
            .unwrap()
            .to_str()
            .unwrap(),
        "true"
    );

    let value = json_body(response).await;
    assert_eq!(value["id"], 1);
    assert_eq!(value["title"], "Synthetic Feature");
}

#[tokio::test]
async fn missing_fixture_is_not_forwarded_upstream() {
    let app = replay_app().await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/tmdb/movie/999999")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Provider base_url is unreachable, so a fast 404 proves replay short-circuited.
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
