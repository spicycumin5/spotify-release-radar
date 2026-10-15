"""Unit tests for include.db (mocked DB-API connection, no live Postgres)."""

from datetime import date

from include import db


class FakeCursor:
    def __init__(self, fetchall_result=None, fetchone_result=None, rowcount=1):
        self.fetchall_result = fetchall_result or []
        self.fetchone_result = fetchone_result
        self.rowcount = rowcount
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchall(self):
        return self.fetchall_result

    def fetchone(self):
        return self.fetchone_result


class FakeConn:
    def __init__(self, **cursor_kwargs):
        self.cursor_obj = FakeCursor(**cursor_kwargs)
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


def test_upsert_user_marks_affinity_stale_on_reregistration():
    conn = FakeConn()

    db.upsert_user("u1", "User One", "u1@example.com", "new-token", conn=conn)

    query, params = conn.cursor_obj.executed[0]
    assert "affinity_synced_at = NULL" in query.split("ON CONFLICT")[1]
    assert params == ("u1", "User One", "u1@example.com", "new-token")
    assert conn.committed


def test_replace_user_follows_upserts_artists_and_prunes_unfollows():
    conn = FakeConn()

    db.replace_user_follows("u1", [{"id": "a1", "name": "Artist One"}], conn=conn)

    queries = conn.cursor_obj.executed
    artist_query, artist_params = queries[0]
    assert "VALUES (%s, %s, NULL)" in artist_query
    assert "last_checked_at" not in artist_query.split("ON CONFLICT")[1]
    assert artist_params == ("a1", "Artist One")
    follow_query, follow_params = queries[1]
    assert "ON CONFLICT (user_id, artist_id) DO NOTHING" in follow_query  # keeps followed_since
    assert follow_params == ("u1", "a1")
    delete_query, delete_params = queries[2]
    assert "NOT (artist_id = ANY(%s))" in delete_query
    assert delete_params == ("u1", ["a1"])
    assert "follows_synced_at = now()" in queries[3][0]
    assert conn.committed
    assert not conn.closed  # caller-provided conn isn't closed


def test_get_artists_to_check_only_followed_and_due_artists():
    conn = FakeConn(fetchall_result=[("a1", "Artist One", '"e1"', '"e2"'), ("a2", "Artist Two", None, None)])

    result = db.get_artists_to_check(20, min_recheck_hours=12, conn=conn)

    assert result == [
        {"id": "a1", "name": "Artist One", "etags": {"album": '"e1"', "single": '"e2"'}},
        {"id": "a2", "name": "Artist Two", "etags": {"album": None, "single": None}},
    ]
    query, params = conn.cursor_obj.executed[0]
    assert "FROM spotify_radar.user_follows f" in query  # unfollowed artists cost nothing
    assert "ORDER BY a.last_checked_at ASC NULLS FIRST" in query
    assert params == (12 * 3600, 20)


def test_record_artist_check_dedupes_by_album_id_and_marks_checked_atomically():
    conn = FakeConn()
    release = {"id": "alb1", "name": "Album", "album_type": "album", "release_date": "2026-01-01"}

    db.record_artist_check("a1", [release], is_baseline=True, etags={"album": '"v2"', "single": '"s2"'}, conn=conn)

    queries = conn.cursor_obj.executed
    assert "INSERT INTO spotify_radar.releases" in queries[0][0]
    assert "ON CONFLICT (album_id) DO NOTHING" in queries[0][0]
    assert queries[1][1] == ("a1", "alb1", "Album", "album", "2026-01-01", True)
    assert "albums_etag = %s, singles_etag = %s" in queries[2][0]
    assert queries[2][1] == ('"v2"', '"s2"', "a1")
    assert conn.committed


def test_get_cooldown_remaining_zero_when_no_active_cooldown():
    assert db.get_cooldown_remaining(conn=FakeConn(fetchone_result=None)) == 0.0
    assert db.get_cooldown_remaining(conn=FakeConn(fetchone_result=(42.5,))) == 42.5


def test_set_cooldown_never_shortens_existing_cooldown():
    conn = FakeConn()

    db.set_cooldown(600, conn=conn)

    query, params = conn.cursor_obj.executed[0]
    assert "GREATEST(" in query
    assert params == ("spotify", 600)


def test_get_pending_releases_groups_by_album_and_excludes_already_digested():
    conn = FakeConn(
        fetchall_result=[("alb1", "Collab", "single", date(2026, 1, 1), ["a2", "a1"], ["B", "A"])]
    )

    result = db.get_pending_releases("u1", date(2025, 12, 1), conn=conn)

    assert result == [
        {
            "album_id": "alb1",
            "album_name": "Collab",
            "album_type": "single",
            "release_date": date(2026, 1, 1),
            "artist_ids": ["a2", "a1"],
            "artist_names": ["A", "B"],
        }
    ]
    query, params = conn.cursor_obj.executed[0]
    assert params == ("u1", date(2025, 12, 1))
    assert "r.release_date >= %s" in query
    assert "NOT ar.is_baseline" in query
    assert "ar.discovered_at >= f.followed_since" in query
    assert "spotify_radar.digest_items" in query
    assert "GROUP BY r.album_id" in query


def test_add_digest_items_inserts_into_pending_digest():
    conn = FakeConn(fetchone_result=("pending",), rowcount=1)

    added = db.add_digest_items("u1", date(2026, 1, 2), [{"album_id": "alb1", "score": 0.5}], conn=conn)

    assert added == 1
    query, params = conn.cursor_obj.executed[-1]
    assert "ON CONFLICT (user_id, album_id) DO NOTHING" in query  # one digest per album per user
    assert params == ("u1", "alb1", date(2026, 1, 2), 0.5)
    assert conn.committed


def test_add_digest_items_skips_digest_already_sent():
    conn = FakeConn(fetchone_result=("sent",))

    added = db.add_digest_items("u1", date(2026, 1, 2), [{"album_id": "alb1", "score": 0.5}], conn=conn)

    assert added == 0
    assert conn.rolled_back
    assert not any("digest_items" in q for q, _ in conn.cursor_obj.executed)


def test_claim_digest_only_claims_pending():
    conn = FakeConn(rowcount=1)
    assert db.claim_digest("u1", date(2026, 1, 2), conn=conn) is True
    query, _ = conn.cursor_obj.executed[0]
    assert "SET status = 'sending'" in query
    assert "AND status = 'pending'" in query

    assert db.claim_digest("u1", date(2026, 1, 2), conn=FakeConn(rowcount=0)) is False
