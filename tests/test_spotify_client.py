"""Unit tests for include.spotify_client (no live network calls)."""

import pytest

from include.spotify_client import (
    RateLimitError,
    _normalize_release_date,
    diff_new_releases,
    get_artist_albums,
)


def test_diff_new_releases_filters_seen_albums():
    albums = [
        {"id": "new1", "name": "New Album"},
        {"id": "old1", "name": "Old Album"},
    ]
    seen_ids = {"old1"}

    assert diff_new_releases(albums, seen_ids) == [{"id": "new1", "name": "New Album"}]


def test_diff_new_releases_returns_empty_when_all_seen():
    albums = [{"id": "a"}, {"id": "b"}]
    assert diff_new_releases(albums, {"a", "b"}) == []


def test_diff_new_releases_returns_all_when_none_seen():
    albums = [{"id": "a"}, {"id": "b"}]
    assert diff_new_releases(albums, set()) == albums


def test_normalize_release_date_pads_year_precision():
    assert _normalize_release_date("1999", "year") == "1999-01-01"


def test_normalize_release_date_pads_month_precision():
    assert _normalize_release_date("1999-05", "month") == "1999-05-01"


def test_normalize_release_date_keeps_day_precision():
    assert _normalize_release_date("1999-05-20", "day") == "1999-05-20"


def test_get_artist_albums_sorts_newest_first(mocker):
    fake_response = mocker.Mock()
    fake_response.raise_for_status.return_value = None
    fake_response.json.return_value = {
        "items": [
            {
                "id": "old",
                "name": "Old Album",
                "album_type": "album",
                "release_date": "2020-01-01",
                "release_date_precision": "day",
            },
            {
                "id": "new",
                "name": "New Single",
                "album_type": "single",
                "release_date": "2024-06-01",
                "release_date_precision": "day",
            },
        ]
    }
    mocker.patch("include.spotify_client.requests.get", return_value=fake_response)

    albums = get_artist_albums("fake-token", "artist123")

    assert [album["id"] for album in albums] == ["new", "old"]


def test_get_artist_albums_raises_rate_limit_error_on_429(mocker):
    fake_response = mocker.Mock()
    fake_response.status_code = 429
    fake_response.headers = {"Retry-After": "30"}
    mocker.patch("include.spotify_client.requests.get", return_value=fake_response)

    with pytest.raises(RateLimitError) as exc_info:
        get_artist_albums("fake-token", "artist123")

    assert exc_info.value.retry_after == 30


def test_get_artist_albums_defaults_retry_after_when_header_missing(mocker):
    fake_response = mocker.Mock()
    fake_response.status_code = 429
    fake_response.headers = {}
    mocker.patch("include.spotify_client.requests.get", return_value=fake_response)

    with pytest.raises(RateLimitError) as exc_info:
        get_artist_albums("fake-token", "artist123")

    assert exc_info.value.retry_after == 1
