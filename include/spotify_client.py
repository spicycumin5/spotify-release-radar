"""Minimal Spotify Web API client.

Two kinds of access token are used:

- A per-user token, minted from that user's long-lived refresh token
  (stored in spotify_radar.users), for user-scoped endpoints like
  /me/following and /me/top/artists.
- One app token (client-credentials flow) for catalog endpoints like
  /artists/{id}/albums. Catalog data is the same for every user, so it's
  fetched once per artist with this token and shared across all followers.

Every request goes through _request(), which paces calls, retries
transient failures with exponential backoff, and handles 429s (see
RateLimitError).
"""

from __future__ import annotations

import os
import random
import time
from dataclasses import dataclass

import requests

TOKEN_URL = "https://accounts.spotify.com/api/token"
API_BASE = "https://api.spotify.com/v1"

# Minimum gap between consecutive requests from this process. A burst of
# requests is what trips Spotify's Development Mode rate limit, which can
# lock the app out for ~24h.
MIN_REQUEST_INTERVAL = float(os.environ.get("SPOTIFY_MIN_REQUEST_INTERVAL", "1.0"))
# Retries for 5xx responses, timeouts, and connection errors.
MAX_RETRIES = int(os.environ.get("SPOTIFY_MAX_RETRIES", "4"))
BACKOFF_BASE = 1.0
BACKOFF_CAP = 30.0
# A 429 whose Retry-After is at most this many seconds is waited out inline.
# Anything longer raises RateLimitError so the caller stops for this run.
MAX_INLINE_RETRY_AFTER = int(os.environ.get("SPOTIFY_MAX_INLINE_RETRY_AFTER", "10"))

RELEASE_GROUPS = ("album", "single")

_RETRYABLE_STATUSES = {500, 502, 503, 504}
_last_request_at = 0.0
_app_token: tuple[str, float] | None = None  # (token, expires_at monotonic)


class RateLimitError(Exception):
    """Raised when Spotify responds with 429 and a long Retry-After.

    Sending requests before `retry_after` elapses is known to push the
    lockout window back out (sometimes to ~24h), so callers should stop
    making requests rather than retrying.
    """

    def __init__(self, retry_after: int):
        self.retry_after = retry_after
        super().__init__(f"Spotify rate limit hit, retry after {retry_after}s")


@dataclass
class UserToken:
    access_token: str
    # Spotify may rotate the refresh token on refresh. When it does, the
    # new one must be stored or the user's next refresh fails.
    refresh_token: str | None


@dataclass
class ArtistAlbums:
    # None when the response was 304 Not Modified (nothing changed since `etag`).
    albums: list[dict] | None
    etag: str | None


@dataclass
class ArtistReleases:
    # None when every group came back 304 Not Modified.
    albums: list[dict] | None
    etags: dict[str, str | None]


def _backoff_delay(attempt: int) -> float:
    """Exponential backoff with full jitter: uniform(0, min(cap, base * 2^attempt))."""
    return random.uniform(0, min(BACKOFF_CAP, BACKOFF_BASE * 2**attempt))


def _throttle() -> None:
    global _last_request_at
    wait = _last_request_at + MIN_REQUEST_INTERVAL - time.monotonic()
    if wait > 0:
        time.sleep(wait)
    _last_request_at = time.monotonic()


def _request(method: str, url: str, timeout: int = 10, **kwargs) -> requests.Response:
    """Send a request with pacing, backoff, and Spotify-aware 429 handling.

    - 5xx, timeouts, connection errors: retried up to MAX_RETRIES times with
      exponential backoff.
    - 429: if Retry-After is short, sleep exactly that long (never less) and
      retry; otherwise raise RateLimitError immediately.
    - 304: returned as-is for conditional requests.
    - Other 4xx: raised as requests.HTTPError.
    """
    for attempt in range(MAX_RETRIES + 1):
        _throttle()
        try:
            response = requests.request(method, url, timeout=timeout, **kwargs)
        except (requests.ConnectionError, requests.Timeout):
            if attempt == MAX_RETRIES:
                raise
            time.sleep(_backoff_delay(attempt))
            continue

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 1))
            if retry_after > MAX_INLINE_RETRY_AFTER or attempt == MAX_RETRIES:
                raise RateLimitError(retry_after=retry_after)
            # Pad Retry-After a little: retrying early resets the lockout.
            time.sleep(retry_after + random.uniform(0.5, 1.5))
            continue

        if response.status_code in _RETRYABLE_STATUSES and attempt < MAX_RETRIES:
            time.sleep(_backoff_delay(attempt))
            continue

        if response.status_code == 304:
            return response
        response.raise_for_status()
        return response

    raise AssertionError("unreachable")  # pragma: no cover


def refresh_user_token(refresh_token: str) -> UserToken:
    """Exchange a user's refresh token for a fresh access token."""
    response = _request(
        "POST",
        TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": os.environ["SPOTIFY_CLIENT_ID"],
            "client_secret": os.environ["SPOTIFY_CLIENT_SECRET"],
        },
    )
    body = response.json()
    return UserToken(access_token=body["access_token"], refresh_token=body.get("refresh_token"))


def get_app_access_token() -> str:
    """Return a client-credentials token for catalog endpoints, cached until near expiry."""
    global _app_token
    if _app_token and time.monotonic() < _app_token[1]:
        return _app_token[0]

    response = _request(
        "POST",
        TOKEN_URL,
        data={"grant_type": "client_credentials"},
        auth=(os.environ["SPOTIFY_CLIENT_ID"], os.environ["SPOTIFY_CLIENT_SECRET"]),
    )
    body = response.json()
    _app_token = (body["access_token"], time.monotonic() + body.get("expires_in", 3600) - 60)
    return _app_token[0]


def get_current_user(access_token: str) -> dict:
    """Return the authenticated user's id and display name."""
    response = _request("GET", f"{API_BASE}/me", headers={"Authorization": f"Bearer {access_token}"})
    body = response.json()
    return {"id": body["id"], "display_name": body.get("display_name")}


def get_followed_artists(access_token: str) -> list[dict]:
    """Return every artist the authenticated user follows."""
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"{API_BASE}/me/following"
    params = {"type": "artist", "limit": 50}

    artists = []
    while url:
        response = _request("GET", url, headers=headers, params=params)
        page = response.json()["artists"]
        artists.extend(page["items"])
        url = page.get("next")
        params = None  # the `next` URL already carries its own query params

    return [{"id": artist["id"], "name": artist["name"]} for artist in artists]


def get_top_artist_ids(access_token: str, time_range: str) -> list[str]:
    """Return the user's top artist ids for `time_range`, most-listened first.

    Requires the user-top-read scope; raises requests.HTTPError (403) without it.
    """
    response = _request(
        "GET",
        f"{API_BASE}/me/top/artists",
        headers={"Authorization": f"Bearer {access_token}"},
        params={"time_range": time_range, "limit": 50},
    )
    return [artist["id"] for artist in response.json()["items"]]


def get_artist_albums(
    access_token: str,
    artist_id: str,
    include_group: str = "album",
    limit: int = 10,
    etag: str | None = None,
) -> ArtistAlbums:
    """Return an artist's most recent releases in one group ("album" or "single"), newest first.

    Pass the `etag` from the previous call to make a conditional request; if
    nothing changed, Spotify answers 304 and `albums` is None.
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    if etag:
        headers["If-None-Match"] = etag
    url = f"{API_BASE}/artists/{artist_id}/albums"
    params = {"include_groups": include_group, "limit": limit}

    response = _request("GET", url, headers=headers, params=params)
    if response.status_code == 304:
        return ArtistAlbums(albums=None, etag=etag)

    albums = [
        {
            "id": album["id"],
            "name": album["name"],
            "album_type": album["album_type"],
            "release_date": _normalize_release_date(
                album["release_date"], album["release_date_precision"]
            ),
        }
        for album in response.json()["items"]
    ]
    return ArtistAlbums(
        albums=sorted(albums, key=lambda a: a["release_date"], reverse=True),
        etag=response.headers.get("ETag"),
    )


def get_artist_releases(
    access_token: str, artist_id: str, etags: dict[str, str | None] | None = None
) -> ArtistReleases:
    """Return an artist's recent albums and singles, fetched as separate groups.

    One combined include_groups=album,single request lists albums ahead of
    singles, so for an artist with many albums the first page could hold no
    singles at all, and new singles would never be seen. Requesting each
    group separately costs one extra call per artist but guarantees both
    are covered. `etags` maps group -> ETag from the previous check.
    """
    etags = etags or {}
    albums, new_etags, changed = [], {}, False
    for group in RELEASE_GROUPS:
        result = get_artist_albums(access_token, artist_id, include_group=group, etag=etags.get(group))
        new_etags[group] = result.etag
        if result.albums is not None:
            changed = True
            albums.extend(result.albums)
    return ArtistReleases(
        albums=sorted(albums, key=lambda a: a["release_date"], reverse=True) if changed else None,
        etags=new_etags,
    )


def diff_new_releases(albums: list[dict], seen_album_ids: set[str]) -> list[dict]:
    """Return the subset of `albums` whose id isn't in `seen_album_ids`, deduped by id."""
    new, ids = [], set()
    for album in albums:
        if album["id"] not in seen_album_ids and album["id"] not in ids:
            ids.add(album["id"])
            new.append(album)
    return new


def _normalize_release_date(release_date: str, precision: str) -> str:
    """Pad a Spotify release date to a full YYYY-MM-DD string.

    Spotify returns dates at "year", "month", or "day" precision (e.g.
    "1999", "1999-01", "1999-01-15"). Postgres DATE columns require a full
    date, so partial dates are padded to the first of the period.
    """
    if precision == "year":
        return f"{release_date}-01-01"
    if precision == "month":
        return f"{release_date}-01"
    return release_date
