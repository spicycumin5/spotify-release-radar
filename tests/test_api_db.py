"""Unit tests for api.db (mocked DB-API connection, no live Postgres)."""

from api import db


class FakeCursor:
    def __init__(self, fetchall_result=None):
        self.fetchall_result = fetchall_result or []
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchall(self):
        return self.fetchall_result


class FakeConn:
    def __init__(self, fetchall_result=None):
        self.cursor_obj = FakeCursor(fetchall_result)
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def close(self):
        self.closed = True


def test_list_releases_orders_by_discovered_at_desc_and_maps_rows():
    conn = FakeConn(
        fetchall_result=[
            ("a1", "Artist One", "alb1", "Album One", "album", "2026-01-01", "2026-01-02T00:00:00"),
        ]
    )

    result = db.list_releases(limit=50, conn=conn)

    assert result == [
        {
            "artist_id": "a1",
            "artist_name": "Artist One",
            "album_id": "alb1",
            "album_name": "Album One",
            "album_type": "album",
            "release_date": "2026-01-01",
            "discovered_at": "2026-01-02T00:00:00",
        }
    ]
    query, params = conn.cursor_obj.executed[0]
    assert "ORDER BY r.discovered_at DESC" in query
    assert "FROM spotify_radar.releases r" in query
    assert "GROUP BY r.album_id" in query
    assert params == (50,)
    assert not conn.closed  # caller-provided conn isn't closed


def test_list_artists_orders_alphabetically():
    conn = FakeConn(fetchall_result=[("a1", "Artist One", None)])

    result = db.list_artists(conn=conn)

    assert result == [{"artist_id": "a1", "artist_name": "Artist One", "last_checked_at": None}]
    query, params = conn.cursor_obj.executed[0]
    assert "ORDER BY artist_name ASC" in query
    assert params is None


def test_list_alerts_orders_by_alerted_at_desc_and_maps_rows():
    conn = FakeConn(
        fetchall_result=[
            (1, "dag", "task", "2026-01-01T00:00:00", "boom", "http://logs", "2026-01-01T00:01:00"),
        ]
    )

    result = db.list_alerts(limit=10, conn=conn)

    assert result == [
        {
            "id": 1,
            "dag_id": "dag",
            "task_id": "task",
            "logical_date": "2026-01-01T00:00:00",
            "error_message": "boom",
            "log_url": "http://logs",
            "alerted_at": "2026-01-01T00:01:00",
        }
    ]
    query, params = conn.cursor_obj.executed[0]
    assert "ORDER BY alerted_at DESC" in query
    assert params == (10,)
