"""Read-only dashboard API for Spotify Release Radar.

Serves the same spotify_radar Postgres tables the Airflow pipeline writes to
(see include/db.py and include/sql/schema.sql) over a small HTTP API for the
frontend/ dashboard. No write endpoints, no auth -- local read-only viewer.

Run with: uvicorn api.main:app --reload --port 8000
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api import db

app = FastAPI(title="Spotify Release Radar API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/releases")
def get_releases(limit: int = 50) -> list[dict]:
    return db.list_releases(limit=limit)


@app.get("/artists")
def get_artists() -> list[dict]:
    return db.list_artists()


@app.get("/alerts")
def get_alerts(limit: int = 50) -> list[dict]:
    return db.list_alerts(limit=limit)
