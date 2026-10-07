"""Unit tests for include.ranking."""

from datetime import date

from include.ranking import affinity_from_top_artists, rank_releases

TODAY = date(2026, 10, 7)


def _release(album_id, artist_ids, album_type="single", release_date="2026-10-01"):
    return {
        "album_id": album_id,
        "album_name": album_id,
        "album_type": album_type,
        "release_date": release_date,
        "artist_ids": artist_ids,
    }


def test_affinity_rewards_higher_rank_and_recent_listening():
    scores = affinity_from_top_artists({"short_term": ["a", "b"], "medium_term": ["c", "a"]})

    assert scores["a"] > scores["b"]  # top of both windows
    assert scores["a"] == 1.0 + 0.6 * 0.5
    assert "z" not in scores


def test_affinity_empty_without_history():
    assert affinity_from_top_artists({}) == {}


def test_rank_releases_orders_by_listening_history():
    affinity = {"fav": 1.5, "meh": 0.2}
    releases = [_release("r-unknown", ["stranger"]), _release("r-meh", ["meh"]), _release("r-fav", ["fav"])]

    ranked = rank_releases(releases, affinity, TODAY)

    assert [r["album_id"] for r in ranked] == ["r-fav", "r-meh", "r-unknown"]
    assert all("score" in r for r in ranked)


def test_collab_scores_by_best_liked_artist():
    affinity = {"fav": 1.0}
    ranked = rank_releases(
        [_release("solo", ["other"]), _release("collab", ["other", "fav"])], affinity, TODAY
    )
    assert ranked[0]["album_id"] == "collab"


def test_without_history_albums_and_fresh_releases_outrank_old_singles():
    ranked = rank_releases(
        [
            _release("old-single", ["x"], release_date="2026-06-01"),
            _release("new-single", ["x"], release_date="2026-10-06"),
            _release("old-album", ["x"], album_type="album", release_date="2026-06-01"),
        ],
        {},
        TODAY,
    )
    assert ranked[-1]["album_id"] == "old-single"
