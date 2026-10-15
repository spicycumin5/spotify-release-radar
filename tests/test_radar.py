"""Behavioral tests for include.radar against an in-memory stand-in for include.db.

FakeDB mirrors the semantics of the SQL in include/db.py (dedup keys,
baseline flags, digest claim states) so these tests exercise the
pipeline's guarantees end to end: fan-out sharing, album dedup, and
never sending a digest twice.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from itertools import count

import pytest
import requests

from include import radar, spotify_client

DAY = date(2026, 10, 7)


class FakeDB:
    def __init__(self):
        self.users = {}
        self.follows = {}  # (user_id, artist_id) -> followed_since tick
        self.artist_names = {}
        self.checked = {}  # artist_id -> etag
        self.releases = {}  # album_id -> release dict
        self.artist_releases = {}  # (artist_id, album_id) -> (is_baseline, discovered tick)
        self.affinity = {}
        self.digests = {}  # (user_id, date) -> status
        self.digest_items = {}  # (user_id, album_id) -> (date, score)
        self.cooldown = 0.0
        self._clock = count()

    def add_user(self, user_id, follows=(), synced=False):
        self.users[user_id] = {
            "user_id": user_id,
            "display_name": user_id,
            "email": f"{user_id}@example.com",
            "refresh_token": f"rt-{user_id}",
            "unsubscribe_token": f"unsub-{user_id}",
        }
        self.replace_user_follows(user_id, [{"id": a, "name": a.upper()} for a in follows])
        synced_at = datetime.now(timezone.utc) if synced else None
        self.users[user_id]["follows_synced_at"] = synced_at
        self.users[user_id]["affinity_synced_at"] = synced_at

    # users
    def count_users(self):
        return len(self.users)

    def upsert_user(self, user_id, display_name, email, refresh_token):
        self.users[user_id] = {
            "user_id": user_id, "display_name": display_name, "email": email,
            "refresh_token": refresh_token, "follows_synced_at": None, "affinity_synced_at": None,
        }

    def get_users(self, user_ids):
        return [dict(self.users[u]) for u in sorted(user_ids) if u in self.users]

    def update_refresh_token(self, user_id, refresh_token):
        self.users[user_id]["refresh_token"] = refresh_token

    def replace_user_follows(self, user_id, artists):
        ids = {a["id"] for a in artists}
        for a in artists:
            self.artist_names[a["id"]] = a["name"]
            self.follows.setdefault((user_id, a["id"]), next(self._clock))
        for key in [k for k in self.follows if k[0] == user_id and k[1] not in ids]:
            del self.follows[key]
        self.users[user_id]["follows_synced_at"] = datetime.now(timezone.utc)

    def replace_user_affinity(self, user_id, scores):
        self.affinity[user_id] = dict(scores)
        self.users[user_id]["affinity_synced_at"] = datetime.now(timezone.utc)

    def get_user_affinity(self, user_id):
        return self.affinity.get(user_id, {})

    # artist checks
    def get_artists_to_check(self, limit, min_recheck_hours):
        followed = sorted({a for (_, a) in self.follows})
        due = [a for a in followed if a not in self.checked]
        return [{"id": a, "name": self.artist_names[a], "etags": {}} for a in due[:limit]]

    def get_seen_release_ids(self, artist_id):
        return {alb for (art, alb) in self.artist_releases if art == artist_id}

    def record_artist_check(self, artist_id, releases, is_baseline, etags):
        for r in releases:
            self.releases.setdefault(r["id"], r)
            self.artist_releases.setdefault((artist_id, r["id"]), (is_baseline, next(self._clock)))
        self.checked[artist_id] = etags

    def get_cooldown_remaining(self):
        return self.cooldown

    def set_cooldown(self, seconds):
        self.cooldown = max(self.cooldown, seconds)

    # digests
    def get_pending_releases(self, user_id, released_since):
        by_album = {}
        for (artist_id, album_id), (is_baseline, discovered) in self.artist_releases.items():
            since = self.follows.get((user_id, artist_id))
            if since is None or is_baseline or discovered < since:
                continue
            if (user_id, album_id) in self.digest_items:
                continue
            r = self.releases[album_id]
            if date.fromisoformat(r["release_date"]) < released_since:
                continue
            entry = by_album.setdefault(album_id, {
                "album_id": album_id, "album_name": r["name"], "album_type": r["album_type"],
                "release_date": r["release_date"], "artist_ids": [], "artist_names": [],
            })
            entry["artist_ids"].append(artist_id)
            entry["artist_names"].append(self.artist_names[artist_id])
        return list(by_album.values())

    def add_digest_items(self, user_id, digest_date, items):
        status = self.digests.setdefault((user_id, digest_date), "pending")
        if status != "pending":
            return 0
        added = 0
        for item in items:
            if (user_id, item["album_id"]) not in self.digest_items:
                self.digest_items[(user_id, item["album_id"])] = (digest_date, item["score"])
                added += 1
        return added

    def get_pending_digest_dates(self, user_id, up_to):
        return sorted(d for (u, d), s in self.digests.items() if u == user_id and s == "pending" and d <= up_to)

    def claim_digest(self, user_id, digest_date):
        if self.digests.get((user_id, digest_date)) != "pending":
            return False
        self.digests[(user_id, digest_date)] = "sending"
        return True

    def mark_digest_sent(self, user_id, digest_date):
        self.digests[(user_id, digest_date)] = "sent"

    def release_digest_claim(self, user_id, digest_date):
        if self.digests.get((user_id, digest_date)) == "sending":
            self.digests[(user_id, digest_date)] = "pending"

    def get_digest_items(self, user_id, digest_date):
        items = []
        for (u, album_id), (d, score) in self.digest_items.items():
            if u == user_id and d == digest_date:
                r = self.releases[album_id]
                names = sorted(self.artist_names[a] for (a, alb) in self.artist_releases if alb == album_id)
                items.append({
                    "album_id": album_id, "album_name": r["name"], "album_type": r["album_type"],
                    "release_date": r["release_date"], "artist_names": names, "score": score,
                })
        return sorted(items, key=lambda i: -i["score"])


class FakeSpotify:
    """Serves each artist's current discography; counts calls."""

    def __init__(self):
        self.catalog = {}  # artist_id -> list of album dicts
        self.album_calls = []

    def release(self, artist_ids, album_id, album_type="single", release_date="2026-10-01"):
        album = {"id": album_id, "name": album_id, "album_type": album_type, "release_date": release_date}
        for artist_id in artist_ids:
            self.catalog.setdefault(artist_id, []).insert(0, album)

    def get_artist_releases(self, token, artist_id, etags=None):
        self.album_calls.append(artist_id)
        return spotify_client.ArtistReleases(albums=list(self.catalog.get(artist_id, [])), etags={})


@pytest.fixture
def fake_db(monkeypatch):
    fake = FakeDB()
    monkeypatch.setattr(radar, "db", fake)
    return fake


@pytest.fixture
def spotify(monkeypatch):
    fake = FakeSpotify()
    monkeypatch.setattr(radar.spotify_client, "get_app_access_token", lambda: "app-token")
    monkeypatch.setattr(radar.spotify_client, "get_artist_releases", fake.get_artist_releases)
    return fake


class Outbox:
    def __init__(self, fail=False):
        self.sent = []
        self.fail = fail

    def __call__(self, to, subject, html_content, custom_headers=None):
        if self.fail:
            raise ConnectionError("SMTP down")
        self.sent.append({"to": to, "subject": subject, "html": html_content, "headers": custom_headers})


def _baseline_then_release(fake_db, spotify, *releases):
    """Baseline every followed artist's existing catalog, then publish releases and discover again."""
    for artist_id in {a for (_, a) in fake_db.follows}:
        spotify.release([artist_id], f"catalog-{artist_id}", release_date="2020-01-01")
    radar.discover_releases(batch_size=10_000, min_recheck_hours=0)
    fake_db.checked.clear()
    for artist_ids, album_id in releases:
        spotify.release(artist_ids, album_id)
    radar.discover_releases(batch_size=10_000, min_recheck_hours=0)


def test_artist_followed_by_many_users_costs_one_api_call(fake_db, spotify):
    for i in range(1000):
        fake_db.add_user(f"user{i:04}", follows=["shared"])

    radar.discover_releases(batch_size=10_000, min_recheck_hours=0)

    assert spotify.album_calls == ["shared"]


def test_first_check_is_baseline_and_not_notified(fake_db, spotify):
    fake_db.add_user("u1", follows=["a1"])
    spotify.release(["a1"], "back-catalog")
    outbox = Outbox()

    radar.discover_releases(batch_size=10, min_recheck_hours=0)
    radar.deliver_digests(["u1"], DAY, outbox)

    assert outbox.sent == []


def test_rerunning_a_day_never_double_sends(fake_db, spotify):
    fake_db.add_user("u1", follows=["a1"])
    _baseline_then_release(fake_db, spotify, (["a1"], "new-album"))
    outbox = Outbox()

    radar.deliver_digests(["u1"], DAY, outbox)
    # Clear and re-run the same day, including another discovery pass.
    fake_db.checked.clear()
    radar.discover_releases(batch_size=10, min_recheck_hours=0)
    radar.deliver_digests(["u1"], DAY, outbox)

    assert len(outbox.sent) == 1
    assert "new-album" in outbox.sent[0]["html"]


def test_release_is_never_repeated_in_a_later_digest(fake_db, spotify):
    fake_db.add_user("u1", follows=["a1"])
    _baseline_then_release(fake_db, spotify, (["a1"], "first"))
    outbox = Outbox()
    radar.deliver_digests(["u1"], DAY, outbox)

    spotify.release(["a1"], "second")
    fake_db.checked.clear()
    radar.discover_releases(batch_size=10, min_recheck_hours=0)
    radar.deliver_digests(["u1"], DAY + timedelta(days=1), outbox)

    assert len(outbox.sent) == 2
    assert "first" not in outbox.sent[1]["html"]
    assert "second" in outbox.sent[1]["html"]


def test_collab_between_followed_artists_appears_once(fake_db, spotify):
    fake_db.add_user("u1", follows=["a1", "a2"])
    _baseline_then_release(fake_db, spotify, (["a1", "a2"], "collab"))
    outbox = Outbox()

    radar.deliver_digests(["u1"], DAY, outbox)

    assert outbox.sent[0]["subject"].startswith("\U0001F3B5 1 new")
    assert "A1, A2" in outbox.sent[0]["html"]


def test_each_user_gets_only_their_followed_artists(fake_db, spotify):
    fake_db.add_user("u1", follows=["a1"])
    fake_db.add_user("u2", follows=["a2"])
    _baseline_then_release(fake_db, spotify, (["a1"], "for-u1"), (["a2"], "for-u2"))
    outbox = Outbox()

    radar.deliver_digests(["u1", "u2"], DAY, outbox)

    by_user = {m["to"]: m["html"] for m in outbox.sent}
    assert "for-u1" in by_user["u1@example.com"] and "for-u2" not in by_user["u1@example.com"]
    assert "for-u2" in by_user["u2@example.com"] and "for-u1" not in by_user["u2@example.com"]


def test_digest_is_ordered_by_listening_history(fake_db, spotify):
    fake_db.add_user("u1", follows=["casual", "fav"])
    fake_db.affinity["u1"] = {"fav": 1.5, "casual": 0.1}
    _baseline_then_release(fake_db, spotify, (["casual"], "casual-drop"), (["fav"], "fav-drop"))
    outbox = Outbox()

    radar.deliver_digests(["u1"], DAY, outbox)

    html = outbox.sent[0]["html"]
    assert html.index("fav-drop") < html.index("casual-drop")


def test_digest_links_to_unsubscribe_when_public_url_is_set(fake_db, spotify, monkeypatch):
    monkeypatch.setenv("RADAR_PUBLIC_URL", "https://radar.example.com/")
    fake_db.add_user("u1", follows=["a1"])
    _baseline_then_release(fake_db, spotify, (["a1"], "new-drop"))
    outbox = Outbox()

    radar.deliver_digests(["u1"], DAY, outbox)

    url = "https://radar.example.com/unsubscribe/unsub-u1"
    assert f'href="{url}"' in outbox.sent[0]["html"]
    assert outbox.sent[0]["headers"] == {
        "List-Unsubscribe": f"<{url}>",
        "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
    }


def test_digest_has_no_unsubscribe_link_without_public_url(fake_db, spotify, monkeypatch):
    monkeypatch.delenv("RADAR_PUBLIC_URL", raising=False)
    fake_db.add_user("u1", follows=["a1"])
    _baseline_then_release(fake_db, spotify, (["a1"], "new-drop"))
    outbox = Outbox()

    radar.deliver_digests(["u1"], DAY, outbox)

    assert "Unsubscribe" not in outbox.sent[0]["html"]
    assert outbox.sent[0]["headers"] is None


def test_failed_send_is_retried_on_rerun(fake_db, spotify):
    fake_db.add_user("u1", follows=["a1"])
    _baseline_then_release(fake_db, spotify, (["a1"], "new-album"))

    with pytest.raises(radar.BatchFailed):
        radar.deliver_digests(["u1"], DAY, Outbox(fail=True))
    outbox = Outbox()
    radar.deliver_digests(["u1"], DAY, outbox)

    assert len(outbox.sent) == 1


def test_digest_stuck_mid_send_is_not_resent(fake_db, spotify):
    fake_db.add_user("u1", follows=["a1"])
    _baseline_then_release(fake_db, spotify, (["a1"], "new-album"))
    radar.deliver_digests(["u1"], DAY, Outbox())
    fake_db.digests[("u1", DAY)] = "sending"  # crashed after sending, before marking sent
    outbox = Outbox()

    radar.deliver_digests(["u1"], DAY, outbox)

    assert outbox.sent == []


def test_rate_limit_stops_discovery_and_sets_cooldown(fake_db, spotify, monkeypatch):
    for artist in ["a1", "a2", "a3"]:
        fake_db.add_user(f"user-{artist}", follows=[artist])
    calls = []

    def rate_limited(token, artist_id, etags=None):
        calls.append(artist_id)
        if len(calls) == 2:
            raise spotify_client.RateLimitError(retry_after=3600)
        return spotify_client.ArtistReleases(albums=[], etags={})

    monkeypatch.setattr(radar.spotify_client, "get_artist_releases", rate_limited)

    stats = radar.discover_releases(batch_size=10, min_recheck_hours=0)
    assert stats["rate_limited"] and stats["checked"] == 1
    assert fake_db.cooldown == 3600
    assert "a2" not in fake_db.checked  # stays at the front of the queue

    # The next run respects the cooldown instead of calling Spotify again.
    radar.discover_releases(batch_size=10, min_recheck_hours=0)
    assert len(calls) == 2


def test_not_modified_artist_is_marked_checked_without_new_releases(fake_db, monkeypatch):
    fake_db.add_user("u1", follows=["a1"])
    monkeypatch.setattr(radar.spotify_client, "get_app_access_token", lambda: "app-token")
    fresh_etags = {"album": '"v1"', "single": '"v2"'}
    monkeypatch.setattr(
        radar.spotify_client,
        "get_artist_releases",
        lambda token, artist_id, etags=None: spotify_client.ArtistReleases(albums=None, etags=fresh_etags),
    )

    stats = radar.discover_releases(batch_size=10, min_recheck_hours=0)

    assert stats["not_modified"] == 1
    assert fake_db.checked["a1"] == fresh_etags


# --- sync_users -------------------------------------------------------------


@pytest.fixture
def user_api(monkeypatch):
    calls = {"token": 0, "follows": 0, "top": 0}

    def refresh(rt):
        calls["token"] += 1
        if rt == "rt-revoked":
            raise requests.HTTPError("invalid_grant")
        return spotify_client.UserToken(access_token=f"at-{rt}", refresh_token="rotated")

    def follows(token):
        calls["follows"] += 1
        return [{"id": "a1", "name": "A1"}]

    def top(token, time_range):
        calls["top"] += 1
        return ["a1"]

    monkeypatch.setattr(radar.spotify_client, "refresh_user_token", refresh)
    monkeypatch.setattr(radar.spotify_client, "get_followed_artists", follows)
    monkeypatch.setattr(radar.spotify_client, "get_top_artist_ids", top)
    return calls


def test_sync_users_refreshes_follows_affinity_and_rotated_token(fake_db, user_api):
    fake_db.add_user("u1")

    radar.sync_users(["u1"], follows_max_age_hours=20, affinity_max_age_hours=72)

    assert ("u1", "a1") in fake_db.follows
    assert fake_db.affinity["u1"]["a1"] > 0
    assert fake_db.users["u1"]["refresh_token"] == "rotated"


def test_sync_users_skips_recently_synced_users_without_api_calls(fake_db, user_api):
    fake_db.add_user("u1", synced=True)

    stats = radar.sync_users(["u1"], follows_max_age_hours=20, affinity_max_age_hours=72)

    assert stats["skipped"] == 1
    assert user_api == {"token": 0, "follows": 0, "top": 0}


def test_one_bad_user_does_not_block_the_batch(fake_db, user_api):
    fake_db.add_user("u1")
    fake_db.add_user("u2")
    fake_db.users["u1"]["refresh_token"] = "rt-revoked"

    with pytest.raises(radar.BatchFailed, match="u1"):
        radar.sync_users(["u1", "u2"], follows_max_age_hours=20, affinity_max_age_hours=72)

    assert ("u2", "a1") in fake_db.follows


def test_missing_top_read_scope_degrades_to_unpersonalized(fake_db, user_api, monkeypatch):
    fake_db.add_user("u1")
    forbidden = requests.Response()
    forbidden.status_code = 403

    def no_scope(token, time_range):
        raise requests.HTTPError(response=forbidden)

    monkeypatch.setattr(radar.spotify_client, "get_top_artist_ids", no_scope)

    radar.sync_users(["u1"], follows_max_age_hours=20, affinity_max_age_hours=72)

    assert fake_db.affinity["u1"] == {}
    assert ("u1", "a1") in fake_db.follows


def test_digest_claimed_by_concurrent_delivery_is_not_sent(fake_db, spotify, monkeypatch):
    fake_db.add_user("u1", follows=["a1"])
    _baseline_then_release(fake_db, spotify, (["a1"], "new-album"))
    # Another attempt claims the digest between listing and claiming it.
    monkeypatch.setattr(fake_db, "claim_digest", lambda user_id, digest_date: False)
    outbox = Outbox()

    radar.deliver_digests(["u1"], DAY, outbox)

    assert outbox.sent == []



def test_old_release_that_looks_new_is_not_emailed(fake_db, spotify):
    """A reissue (new album id) or back-catalog album surfacing after the baseline."""
    fake_db.add_user("u1", follows=["a1"])
    _baseline_then_release(fake_db, spotify, (["a1"], "fresh-single"))
    spotify.release(["a1"], "remaster-of-2009-album", album_type="album", release_date="2009-05-01")
    fake_db.checked.clear()
    radar.discover_releases(batch_size=10, min_recheck_hours=0)
    outbox = Outbox()

    radar.deliver_digests(["u1"], DAY, outbox, max_release_age_days=30)

    assert len(outbox.sent) == 1
    assert "fresh-single" in outbox.sent[0]["html"]
    assert "remaster-of-2009-album" not in outbox.sent[0]["html"]


def test_release_age_cutoff_is_relative_to_the_run_date(fake_db, spotify):
    fake_db.add_user("u1", follows=["a1"])
    _baseline_then_release(fake_db, spotify, (["a1"], "september-drop"))
    fake_db.releases["september-drop"]["release_date"] = "2026-09-01"
    outbox = Outbox()

    # 36 days old on DAY: excluded with a 30-day window, included with 40.
    radar.deliver_digests(["u1"], DAY, outbox, max_release_age_days=30)
    assert outbox.sent == []
    radar.deliver_digests(["u1"], DAY, outbox, max_release_age_days=40)
    assert len(outbox.sent) == 1
