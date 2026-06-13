"""Minimal Spotify Web API client.

Uses a long-lived refresh token (obtained once via
scripts/spotify_auth_setup.py and stored as SPOTIFY_REFRESH_TOKEN) to mint a
fresh access token on every call. This avoids relying on an on-disk token
cache, which doesn't survive between ephemeral Airflow task containers.
"""

import os

import requests

TOKEN_URL = "https://accounts.spotify.com/api/token"
API_BASE = "https://api.spotify.com/v1"


def get_access_token() -> str:
    """Exchange the stored refresh token for a fresh access token."""
    response = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "refresh_token": os.environ["SPOTIFY_REFRESH_TOKEN"],
            "client_id": os.environ["SPOTIFY_CLIENT_ID"],
            "client_secret": os.environ["SPOTIFY_CLIENT_SECRET"],
        },
        timeout=10,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def get_followed_artists(access_token: str) -> list[dict]:
    """Return every artist the authenticated user follows."""
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"{API_BASE}/me/following"
    params = {"type": "artist", "limit": 50}

    artists = []
    while url:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        page = response.json()["artists"]
        artists.extend(page["items"])
        url = page.get("next")
        params = None  # the `next` URL already carries its own query params

    return [{"id": artist["id"], "name": artist["name"]} for artist in artists]


def get_artist_albums(access_token: str, artist_id: str, limit: int = 10) -> list[dict]:
    """Return an artist's most recent albums/singles, newest first."""
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"{API_BASE}/artists/{artist_id}/albums"
    params = {"include_groups": "album,single", "limit": limit}

    response = requests.get(url, headers=headers, params=params, timeout=10)
    response.raise_for_status()
    items = response.json()["items"]

    albums = [
        {
            "id": album["id"],
            "name": album["name"],
            "album_type": album["album_type"],
            "release_date": _normalize_release_date(
                album["release_date"], album["release_date_precision"]
            ),
        }
        for album in items
    ]
    return sorted(albums, key=lambda a: a["release_date"], reverse=True)


def diff_new_releases(albums: list[dict], seen_album_ids: set[str]) -> list[dict]:
    """Return the subset of `albums` whose id isn't in `seen_album_ids`."""
    return [album for album in albums if album["id"] not in seen_album_ids]


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
