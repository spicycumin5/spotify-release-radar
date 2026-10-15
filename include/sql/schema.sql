-- Application schema for the Spotify Release Radar pipeline.
-- Lives alongside Airflow's own metadata tables (public schema) in the
-- same local Postgres instance provided by `astro dev start`.
--
-- Safe to re-apply: every statement is idempotent, and the ALTERs below
-- migrate a database created by the earlier single-user version in place.

CREATE SCHEMA IF NOT EXISTS spotify_radar;

-- People receiving digests. Each has their own Spotify refresh token
-- (registered via scripts/spotify_auth_setup.py --email ...).
CREATE TABLE IF NOT EXISTS spotify_radar.users (
    user_id            TEXT PRIMARY KEY,          -- Spotify user id
    display_name       TEXT,
    email              TEXT NOT NULL,
    refresh_token      TEXT NOT NULL,
    active             BOOLEAN NOT NULL DEFAULT TRUE,
    follows_synced_at  TIMESTAMPTZ,
    affinity_synced_at TIMESTAMPTZ,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Secret per-user code in each digest's unsubscribe link (signup/app.py
-- deactivates the user who presents it). Random, so links can't be guessed.
ALTER TABLE spotify_radar.users ADD COLUMN IF NOT EXISTS unsubscribe_token TEXT NOT NULL
    DEFAULT replace(gen_random_uuid()::text, '-', '');
CREATE UNIQUE INDEX IF NOT EXISTS users_unsubscribe_token_idx ON spotify_radar.users (unsubscribe_token);

-- Every artist followed by at least one user (the union of all follow
-- lists). Release checks run once per artist here, however many users
-- follow it.
CREATE TABLE IF NOT EXISTS spotify_radar.tracked_artists (
    artist_id       TEXT PRIMARY KEY,
    artist_name     TEXT NOT NULL,
    last_checked_at TIMESTAMPTZ
);
-- ETags from the last /artists/{id}/albums responses (albums and singles
-- are fetched separately), sent back as If-None-Match so an unchanged
-- discography costs a 304 instead of a body.
ALTER TABLE spotify_radar.tracked_artists ADD COLUMN IF NOT EXISTS albums_etag TEXT;
ALTER TABLE spotify_radar.tracked_artists ADD COLUMN IF NOT EXISTS singles_etag TEXT;

-- Per-user follow lists. followed_since is when we first saw the follow:
-- only releases discovered after it are eligible for that user's digest,
-- so following an artist doesn't email you their existing catalog.
CREATE TABLE IF NOT EXISTS spotify_radar.user_follows (
    user_id        TEXT NOT NULL REFERENCES spotify_radar.users (user_id) ON DELETE CASCADE,
    artist_id      TEXT NOT NULL REFERENCES spotify_radar.tracked_artists (artist_id),
    followed_since TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, artist_id)
);
CREATE INDEX IF NOT EXISTS user_follows_artist_idx ON spotify_radar.user_follows (artist_id);

-- How much each user listens to each artist, derived from their Spotify
-- top artists. Drives digest ordering (include/ranking.py).
CREATE TABLE IF NOT EXISTS spotify_radar.user_artist_affinity (
    user_id    TEXT NOT NULL REFERENCES spotify_radar.users (user_id) ON DELETE CASCADE,
    artist_id  TEXT NOT NULL,
    score      DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (user_id, artist_id)
);

-- One row per Spotify album id -- the deduplicated release catalog. A
-- collaboration found under two followed artists is still one release.
CREATE TABLE IF NOT EXISTS spotify_radar.releases (
    album_id      TEXT PRIMARY KEY,
    album_name    TEXT NOT NULL,
    album_type    TEXT NOT NULL,
    release_date  DATE,
    discovered_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Which tracked artist each release was found under (the per-artist "seen"
-- set used to diff new releases).
CREATE TABLE IF NOT EXISTS spotify_radar.artist_releases (
    artist_id     TEXT NOT NULL REFERENCES spotify_radar.tracked_artists (artist_id),
    album_id      TEXT NOT NULL,
    album_name    TEXT NOT NULL,
    album_type    TEXT NOT NULL,
    release_date  DATE,
    discovered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (artist_id, album_id)
);
-- Releases recorded the first time an artist is checked are a baseline,
-- never notified. Rows that predate this column are treated as already
-- handled (DEFAULT TRUE on add), new rows default to FALSE.
ALTER TABLE spotify_radar.artist_releases ADD COLUMN IF NOT EXISTS is_baseline BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE spotify_radar.artist_releases ALTER COLUMN is_baseline SET DEFAULT FALSE;

-- Backfill the deduplicated catalog from the earlier per-artist table.
INSERT INTO spotify_radar.releases (album_id, album_name, album_type, release_date, discovered_at)
SELECT DISTINCT ON (album_id) album_id, album_name, album_type, release_date, discovered_at
FROM spotify_radar.artist_releases
ORDER BY album_id, discovered_at
ON CONFLICT (album_id) DO NOTHING;

-- One digest email per user per DAG-run date. status moves
-- pending -> sending -> sent; a digest is claimed ('sending') before the
-- email goes out, so re-running a day can never send it twice.
CREATE TABLE IF NOT EXISTS spotify_radar.digests (
    user_id     TEXT NOT NULL REFERENCES spotify_radar.users (user_id) ON DELETE CASCADE,
    digest_date DATE NOT NULL,
    status      TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'sending', 'sent')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at     TIMESTAMPTZ,
    PRIMARY KEY (user_id, digest_date)
);

-- Releases assigned to a digest. The (user_id, album_id) primary key means
-- an album lands in at most one digest per user, ever.
CREATE TABLE IF NOT EXISTS spotify_radar.digest_items (
    user_id     TEXT NOT NULL,
    album_id    TEXT NOT NULL REFERENCES spotify_radar.releases (album_id),
    digest_date DATE NOT NULL,
    score       DOUBLE PRECISION NOT NULL DEFAULT 0,
    PRIMARY KEY (user_id, album_id),
    FOREIGN KEY (user_id, digest_date) REFERENCES spotify_radar.digests (user_id, digest_date) ON DELETE CASCADE
);

-- Persistent circuit breaker for Spotify's rate limit. A long Retry-After
-- is recorded here so later tasks and runs stop calling the API until it
-- passes, instead of retrying into the lockout.
CREATE TABLE IF NOT EXISTS spotify_radar.api_cooldown (
    api            TEXT PRIMARY KEY,
    cooldown_until TIMESTAMPTZ NOT NULL
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
