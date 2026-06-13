-- Application schema for the Spotify Release Radar pipeline.
-- Lives alongside Airflow's own metadata tables (public schema) in the
-- same local Postgres instance provided by `astro dev start`.

CREATE SCHEMA IF NOT EXISTS spotify_radar;

-- Artists the connected Spotify account follows.
CREATE TABLE IF NOT EXISTS spotify_radar.tracked_artists (
    artist_id       TEXT PRIMARY KEY,
    artist_name     TEXT NOT NULL,
    last_checked_at TIMESTAMPTZ
);

-- Releases (albums/singles) we've already seen for each tracked artist.
CREATE TABLE IF NOT EXISTS spotify_radar.artist_releases (
    artist_id     TEXT NOT NULL REFERENCES spotify_radar.tracked_artists (artist_id),
    album_id      TEXT NOT NULL,
    album_name    TEXT NOT NULL,
    album_type    TEXT NOT NULL,
    release_date  DATE,
    discovered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (artist_id, album_id)
);

-- Audit log of structured failure alerts raised by on_failure_callback.
CREATE TABLE IF NOT EXISTS spotify_radar.alert_log (
    id            SERIAL PRIMARY KEY,
    dag_id        TEXT NOT NULL,
    task_id       TEXT NOT NULL,
    logical_date  TIMESTAMPTZ,
    error_message TEXT,
    log_url       TEXT,
    alerted_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
