"""Unit tests for include.spotify_client (no live network calls)."""

import pytest
import requests

from include import spotify_client
from include.spotify_client import (
    RateLimitError,
    _normalize_release_date,
    diff_new_releases,
    get_artist_albums,
    get_artist_releases,
)


@pytest.fixture(autouse=True)
def no_sleep(mocker):
    """Skip real pacing/backoff sleeps; return the mock to inspect waits."""
    return mocker.patch("include.spotify_client.time.sleep")


def _response(mocker, status=200, json_body=None, headers=None):
    response = mocker.Mock()
    response.status_code = status
    response.headers = headers or {}
    response.json.return_value = json_body
    if status >= 400:
        response.raise_for_status.side_effect = requests.HTTPError(response=response)
    else:
        response.raise_for_status.return_value = None
    return response


ALBUMS_BODY = {
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


def test_diff_new_releases_dedupes_by_album_id():
    albums = [{"id": "a", "name": "first"}, {"id": "a", "name": "dupe"}]
    assert diff_new_releases(albums, set()) == [{"id": "a", "name": "first"}]


def test_normalize_release_date_pads_year_precision():
    assert _normalize_release_date("1999", "year") == "1999-01-01"


def test_normalize_release_date_pads_month_precision():
    assert _normalize_release_date("1999-05", "month") == "1999-05-01"


def test_normalize_release_date_keeps_day_precision():
    assert _normalize_release_date("1999-05-20", "day") == "1999-05-20"


def test_get_artist_albums_sorts_newest_first_and_returns_etag(mocker):
    mocker.patch(
        "include.spotify_client.requests.request",
        return_value=_response(mocker, json_body=ALBUMS_BODY, headers={"ETag": '"v1"'}),
    )

    result = get_artist_albums("fake-token", "artist123")

    assert [album["id"] for album in result.albums] == ["new", "old"]
    assert result.etag == '"v1"'


def test_get_artist_albums_sends_if_none_match_and_handles_304(mocker):
    request = mocker.patch(
        "include.spotify_client.requests.request", return_value=_response(mocker, status=304)
    )

    result = get_artist_albums("fake-token", "artist123", etag='"v1"')

    assert result.albums is None
    assert result.etag == '"v1"'
    assert request.call_args.kwargs["headers"]["If-None-Match"] == '"v1"'


def test_long_retry_after_raises_rate_limit_error_without_retrying(mocker):
    request = mocker.patch(
        "include.spotify_client.requests.request",
        return_value=_response(mocker, status=429, headers={"Retry-After": "30"}),
    )

    with pytest.raises(RateLimitError) as exc_info:
        get_artist_albums("fake-token", "artist123")

    assert exc_info.value.retry_after == 30
    assert request.call_count == 1


def test_short_retry_after_is_waited_out_then_retried(mocker, no_sleep):
    mocker.patch(
        "include.spotify_client.requests.request",
        side_effect=[
            _response(mocker, status=429, headers={"Retry-After": "2"}),
            _response(mocker, json_body=ALBUMS_BODY),
        ],
    )

    result = get_artist_albums("fake-token", "artist123")

    assert len(result.albums) == 2
    # Never retries before Retry-After has elapsed.
    assert any(call.args[0] >= 2 for call in no_sleep.call_args_list)


def test_rate_limit_defaults_retry_after_when_header_missing(mocker, monkeypatch):
    monkeypatch.setattr(spotify_client, "MAX_INLINE_RETRY_AFTER", 0)
    mocker.patch(
        "include.spotify_client.requests.request", return_value=_response(mocker, status=429)
    )

    with pytest.raises(RateLimitError) as exc_info:
        get_artist_albums("fake-token", "artist123")

    assert exc_info.value.retry_after == 1


def test_server_errors_retry_with_backoff_then_succeed(mocker):
    request = mocker.patch(
        "include.spotify_client.requests.request",
        side_effect=[
            _response(mocker, status=503),
            requests.ConnectionError(),
            _response(mocker, json_body=ALBUMS_BODY),
        ],
    )

    result = get_artist_albums("fake-token", "artist123")

    assert len(result.albums) == 2
    assert request.call_count == 3


def test_server_errors_give_up_after_max_retries(mocker, monkeypatch):
    monkeypatch.setattr(spotify_client, "MAX_RETRIES", 2)
    request = mocker.patch(
        "include.spotify_client.requests.request", return_value=_response(mocker, status=500)
    )

    with pytest.raises(requests.HTTPError):
        get_artist_albums("fake-token", "artist123")

    assert request.call_count == 3


def test_client_errors_are_not_retried(mocker):
    request = mocker.patch(
        "include.spotify_client.requests.request", return_value=_response(mocker, status=404)
    )

    with pytest.raises(requests.HTTPError):
        get_artist_albums("fake-token", "artist123")

    assert request.call_count == 1


def test_backoff_delay_is_capped(monkeypatch):
    monkeypatch.setattr(spotify_client.random, "uniform", lambda low, high: high)
    assert spotify_client._backoff_delay(0) == 1.0
    assert spotify_client._backoff_delay(3) == 8.0
    assert spotify_client._backoff_delay(20) == spotify_client.BACKOFF_CAP


def test_app_access_token_is_cached(mocker, monkeypatch):
    monkeypatch.setattr(spotify_client, "_app_token", None)
    monkeypatch.setenv("SPOTIFY_CLIENT_ID", "id")
    monkeypatch.setenv("SPOTIFY_CLIENT_SECRET", "secret")
    request = mocker.patch(
        "include.spotify_client.requests.request",
        return_value=_response(mocker, json_body={"access_token": "app-tok", "expires_in": 3600}),
    )

    assert spotify_client.get_app_access_token() == "app-tok"
    assert spotify_client.get_app_access_token() == "app-tok"
    assert request.call_count == 1
    assert request.call_args.kwargs["data"] == {"grant_type": "client_credentials"}


def test_refresh_user_token_returns_rotated_refresh_token(mocker, monkeypatch):
    monkeypatch.setenv("SPOTIFY_CLIENT_ID", "id")
    monkeypatch.setenv("SPOTIFY_CLIENT_SECRET", "secret")
    mocker.patch(
        "include.spotify_client.requests.request",
        return_value=_response(mocker, json_body={"access_token": "a", "refresh_token": "r2"}),
    )

    token = spotify_client.refresh_user_token("r1")

    assert token.access_token == "a"
    assert token.refresh_token == "r2"



def _group_responder(mocker, bodies):
    """Fake requests.request answering per include_groups value."""
    def respond(method, url, **kwargs):
        return bodies[kwargs["params"]["include_groups"]]
    return mocker.patch("include.spotify_client.requests.request", side_effect=respond)


def _album(album_id, album_type, release_date):
    return {
        "id": album_id, "name": album_id, "album_type": album_type,
        "release_date": release_date, "release_date_precision": "day",
    }


def test_get_artist_releases_fetches_albums_and_singles_separately(mocker):
    request = _group_responder(mocker, {
        "album": _response(mocker, json_body={"items": [_album("lp", "album", "2026-01-01")]}, headers={"ETag": '"a1"'}),
        "single": _response(mocker, json_body={"items": [_album("sg", "single", "2026-10-01")]}, headers={"ETag": '"s1"'}),
    })

    result = get_artist_releases("tok", "artist123")

    assert [call.kwargs["params"]["include_groups"] for call in request.call_args_list] == ["album", "single"]
    assert [a["id"] for a in result.albums] == ["sg", "lp"]  # merged, newest first
    assert result.etags == {"album": '"a1"', "single": '"s1"'}


def test_get_artist_releases_none_only_when_every_group_unchanged(mocker):
    _group_responder(mocker, {"album": _response(mocker, status=304), "single": _response(mocker, status=304)})
    unchanged = get_artist_releases("tok", "artist123", etags={"album": '"a1"', "single": '"s1"'})
    assert unchanged.albums is None
    assert unchanged.etags == {"album": '"a1"', "single": '"s1"'}

    _group_responder(mocker, {
        "album": _response(mocker, status=304),
        "single": _response(mocker, json_body={"items": [_album("sg", "single", "2026-10-01")]}, headers={"ETag": '"s2"'}),
    })
    partial = get_artist_releases("tok", "artist123", etags={"album": '"a1"', "single": '"s1"'})
    assert [a["id"] for a in partial.albums] == ["sg"]
    assert partial.etags == {"album": '"a1"', "single": '"s2"'}
