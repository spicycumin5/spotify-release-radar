"""Signup and unsubscribe pages for Release Radar, deployed on Vercel.

    GET  /                     signup form (email address)
    POST /signup               remembers the email, sends the visitor to Spotify
    GET  /callback             Spotify redirects here; registers the user
    GET  /unsubscribe/{token}  confirmation page linked from every digest
    POST /unsubscribe/{token}  deactivates the user (also Gmail's one-click button)

Self-contained on purpose: Vercel deploys only this folder, with the small
requirements.txt next to it, so it doesn't import include/ (whose project
requirements pull in Airflow). The SQL mirrors include/db.py's upsert_user.

Environment: SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, RADAR_PUBLIC_URL (this
site's URL; RADAR_PUBLIC_URL + "/callback" must be a redirect URI on the
Spotify app), and PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD/PGSSLMODE.

Spotify's Development Mode only lets in accounts added under the app's User
Management, so that list is the guest list: anyone else gets a page saying
to ask for an invite.
"""

from __future__ import annotations

import html
import os
import re
import secrets
from urllib.parse import urlencode

import psycopg2
import requests
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

AUTHORIZE_URL = "https://accounts.spotify.com/authorize"
TOKEN_URL = "https://accounts.spotify.com/api/token"
ME_URL = "https://api.spotify.com/v1/me"
SCOPE = "user-follow-read user-top-read"
EMAIL_RE = re.compile(r"[^@\s]+@[^@\s]+\.[^@\s]+")
STATE_COOKIE = "radar_signup_state"
EMAIL_COOKIE = "radar_signup_email"

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)


def _public_url() -> str:
    return os.environ["RADAR_PUBLIC_URL"].rstrip("/")


def _redirect_uri() -> str:
    return f"{_public_url()}/callback"


def _connect():
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=os.environ.get("PGPORT", "5432"),
        dbname=os.environ.get("PGDATABASE", "postgres"),
        user=os.environ.get("PGUSER", "postgres"),
        password=os.environ.get("PGPASSWORD", "postgres"),
    )


def _execute(query: str, params: tuple) -> list[tuple]:
    conn = _connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall() if cur.description else []
    finally:
        conn.close()


def register_user(user_id: str, display_name: str | None, email: str, refresh_token: str) -> None:
    """Same upsert as include.db.upsert_user: (re)activates the user and marks their history stale."""
    _execute(
        """
        INSERT INTO spotify_radar.users (user_id, display_name, email, refresh_token)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (user_id) DO UPDATE
            SET display_name = EXCLUDED.display_name,
                email = EXCLUDED.email,
                refresh_token = EXCLUDED.refresh_token,
                active = TRUE,
                affinity_synced_at = NULL
        """,
        (user_id, display_name, email, refresh_token),
    )


def find_user_by_unsubscribe_token(token: str) -> tuple[str, bool] | None:
    """Return (email, active) for the user with this unsubscribe token."""
    rows = _execute("SELECT email, active FROM spotify_radar.users WHERE unsubscribe_token = %s", (token,))
    return rows[0] if rows else None


def deactivate_user(token: str) -> bool:
    rows = _execute(
        "UPDATE spotify_radar.users SET active = FALSE WHERE unsubscribe_token = %s RETURNING user_id",
        (token,),
    )
    return bool(rows)


def _page(title: str, body: str, status_code: int = 200) -> HTMLResponse:
    return HTMLResponse(
        f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(title)} · Release Radar</title>
<style>
  :root {{ --bg: #f6f6f4; --card: #fff; --text: #191414; --muted: #6a6a6a; --accent: #1db954; --border: #e2e2de; }}
  @media (prefers-color-scheme: dark) {{
    :root {{ --bg: #121212; --card: #1c1c1c; --text: #f0f0f0; --muted: #a7a7a7; --border: #2e2e2e; }}
  }}
  * {{ box-sizing: border-box; }}
  body {{ margin: 0; min-height: 100vh; display: grid; place-items: center; padding: 16px;
         background: var(--bg); color: var(--text);
         font: 16px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif; }}
  main {{ width: 100%; max-width: 420px; background: var(--card); border: 1px solid var(--border);
         border-radius: 16px; padding: 32px 28px; }}
  h1 {{ font-size: 1.4rem; margin: 0 0 8px; }}
  p {{ margin: 0 0 16px; color: var(--muted); }}
  label {{ display: block; font-weight: 600; margin-bottom: 6px; }}
  input[type=email] {{ width: 100%; padding: 12px; border-radius: 10px; border: 1px solid var(--border);
         background: var(--bg); color: var(--text); font: inherit; margin-bottom: 16px; }}
  button, .button {{ display: inline-block; width: 100%; padding: 12px; border: 0; border-radius: 999px;
         background: var(--accent); color: #000; font: inherit; font-weight: 700; text-align: center;
         text-decoration: none; cursor: pointer; }}
  .secondary {{ background: transparent; color: var(--text); border: 1px solid var(--border); }}
  small {{ display: block; margin-top: 16px; color: var(--muted); }}
</style>
</head>
<body><main>{body}</main></body>
</html>""",
        status_code=status_code,
    )


def _signup_again(text: str = "Start over") -> str:
    return f'<a class="button secondary" href="/">{html.escape(text)}</a>'


@app.get("/", response_class=HTMLResponse)
def signup_form() -> HTMLResponse:
    return _page(
        "Sign up",
        """
        <h1>🎵 Release Radar</h1>
        <p>A daily email when artists you follow on Spotify put out something new,
        ordered by how much you listen to them. No email on days with nothing new.</p>
        <form method="post" action="/signup">
          <label for="email">Send my digest to</label>
          <input id="email" name="email" type="email" required autocomplete="email" placeholder="you@example.com">
          <button type="submit">Continue with Spotify</button>
        </form>
        <small>Spotify will ask to let this app see the artists you follow and your top artists.
        This app is invite-only: your Spotify account needs to be added by whoever sent you the link.</small>
        """,
    )


@app.post("/signup")
def start_signup(request: Request, email: str = Form(...)):
    email = email.strip()
    if not EMAIL_RE.fullmatch(email) or len(email) > 254:
        return _page("Check your email", f"<h1>That email doesn't look right</h1><p>{html.escape(email)}</p>{_signup_again('Try again')}", 400)

    state = secrets.token_urlsafe(24)
    params = {
        "client_id": os.environ["SPOTIFY_CLIENT_ID"],
        "response_type": "code",
        "redirect_uri": _redirect_uri(),
        "scope": SCOPE,
        "state": state,
    }
    response = RedirectResponse(f"{AUTHORIZE_URL}?{urlencode(params)}", status_code=303)
    secure = request.url.scheme == "https"
    for name, value in ((STATE_COOKIE, state), (EMAIL_COOKIE, email)):
        response.set_cookie(name, value, max_age=600, httponly=True, secure=secure, samesite="lax")
    return response


@app.get("/callback")
def finish_signup(request: Request, code: str | None = None, state: str | None = None, error: str | None = None):
    if error:
        return _page("Not connected", f"<h1>Spotify wasn't connected</h1><p>Spotify said: {html.escape(error)}. No account was created.</p>{_signup_again()}", 400)

    expected_state = request.cookies.get(STATE_COOKIE)
    email = request.cookies.get(EMAIL_COOKIE)
    if not (code and state and expected_state and email) or not secrets.compare_digest(state, expected_state):
        return _page("Link expired", f"<h1>This signup link expired</h1><p>Signups have to be finished within 10 minutes, in the same browser.</p>{_signup_again()}", 400)

    token_response = requests.post(
        TOKEN_URL,
        data={"grant_type": "authorization_code", "code": code, "redirect_uri": _redirect_uri()},
        auth=(os.environ["SPOTIFY_CLIENT_ID"], os.environ["SPOTIFY_CLIENT_SECRET"]),
        timeout=15,
    )
    if token_response.status_code != 200:
        return _page("Something went wrong", f"<h1>Couldn't connect to Spotify</h1><p>Please try again.</p>{_signup_again()}", 502)
    tokens = token_response.json()

    me_response = requests.get(ME_URL, headers={"Authorization": f"Bearer {tokens['access_token']}"}, timeout=15)
    if me_response.status_code == 403:
        # Development Mode: this Spotify account isn't on the app's User Management list.
        return _page(
            "Invite needed",
            "<h1>You're not on the guest list yet</h1>"
            "<p>This app is invite-only. Ask the person who sent you this link to add your "
            "Spotify account's email in their Spotify developer dashboard, then sign up again.</p>"
            + _signup_again(),
            403,
        )
    if me_response.status_code != 200:
        return _page("Something went wrong", f"<h1>Couldn't read your Spotify profile</h1><p>Please try again.</p>{_signup_again()}", 502)
    me = me_response.json()

    register_user(me["id"], me.get("display_name"), email, tokens["refresh_token"])

    response = _page(
        "You're in",
        f"""
        <h1>You're in 🎉</h1>
        <p>Digests for <strong>{html.escape(me.get("display_name") or me["id"])}</strong> will go to
        <strong>{html.escape(email)}</strong>.</p>
        <p>The first run only takes note of what's already out, so your first email arrives
        when an artist you follow releases something new. Every email has an unsubscribe link.</p>
        """,
    )
    response.delete_cookie(STATE_COOKIE)
    response.delete_cookie(EMAIL_COOKIE)
    return response


@app.get("/unsubscribe/{token}", response_class=HTMLResponse)
def confirm_unsubscribe(token: str) -> HTMLResponse:
    # A GET never unsubscribes: mail scanners open links in emails.
    user = find_user_by_unsubscribe_token(token)
    if user is None:
        return _page("Link not valid", "<h1>This unsubscribe link isn't valid</h1><p>It may have been mistyped.</p>", 404)
    email, active = user
    if not active:
        return _page("Unsubscribed", f"<h1>Already unsubscribed</h1><p>{html.escape(email)} isn't getting digests.</p>{_signup_again('Sign up again')}")
    return _page(
        "Unsubscribe",
        f"""
        <h1>Unsubscribe?</h1>
        <p>Stop sending Release Radar digests to <strong>{html.escape(email)}</strong>.</p>
        <form method="post" action="/unsubscribe/{html.escape(token)}">
          <button type="submit">Unsubscribe</button>
        </form>
        """,
    )


@app.post("/unsubscribe/{token}", response_class=HTMLResponse)
def unsubscribe(token: str) -> HTMLResponse:
    # Also the target of Gmail's one-click button (RFC 8058 List-Unsubscribe-Post).
    if not deactivate_user(token):
        return _page("Link not valid", "<h1>This unsubscribe link isn't valid</h1><p>It may have been mistyped.</p>", 404)
    return _page(
        "Unsubscribed",
        f"<h1>You're unsubscribed</h1><p>No more digests will be sent. Changed your mind? Signing up again turns them back on.</p>{_signup_again('Sign up again')}",
    )
