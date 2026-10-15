"""Order a user's digest by how much they listen to each release's artists.

Listening history comes from Spotify's /me/top/artists, which ranks a
user's most-played artists over three windows. Those rankings become a
per-artist affinity score; each release is then scored by its best-liked
artist, plus small bonuses for full albums and fresh release dates so the
order is still sensible for artists the user doesn't play much.
"""

from __future__ import annotations

from datetime import date

# Recent listening counts most; long_term is all-time favorites.
TIME_RANGE_WEIGHTS = {"short_term": 1.0, "medium_term": 0.6, "long_term": 0.3}
ALBUM_TYPE_BONUS = {"album": 0.1}
RECENCY_BONUS = 0.1
RECENCY_WINDOW_DAYS = 30


def affinity_from_top_artists(top_artist_ids: dict[str, list[str]]) -> dict[str, float]:
    """Turn {time_range: [artist_id, most-played first]} into {artist_id: score}.

    Within each window, the #1 artist earns the window's full weight and
    lower ranks earn proportionally less. Scores add up across windows, so
    an artist that's both a recent and an all-time favorite ranks highest.
    """
    scores: dict[str, float] = {}
    for time_range, artist_ids in top_artist_ids.items():
        weight = TIME_RANGE_WEIGHTS.get(time_range, 0.0)
        n = len(artist_ids)
        for position, artist_id in enumerate(artist_ids):
            scores[artist_id] = scores.get(artist_id, 0.0) + weight * (n - position) / n
    return scores


def score_release(release: dict, affinity: dict[str, float], today: date) -> float:
    """Score one release. `release` needs artist_ids, album_type, release_date."""
    artist_score = max((affinity.get(a, 0.0) for a in release["artist_ids"]), default=0.0)

    release_date = release["release_date"]
    if isinstance(release_date, str):
        release_date = date.fromisoformat(release_date)
    recency = 0.0
    if release_date is not None:
        days_old = max(0, (today - release_date).days)
        recency = RECENCY_BONUS * max(0.0, 1 - days_old / RECENCY_WINDOW_DAYS)

    return artist_score + ALBUM_TYPE_BONUS.get(release["album_type"], 0.0) + recency


def rank_releases(releases: list[dict], affinity: dict[str, float], today: date) -> list[dict]:
    """Return copies of `releases` with a "score" key, highest score first."""
    scored = [{**r, "score": score_release(r, affinity, today)} for r in releases]
    return sorted(scored, key=lambda r: (-r["score"], r["album_name"]))
