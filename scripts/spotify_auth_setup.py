"""Register a Spotify user for Release Radar digests.

Run this locally (NOT inside Airflow) once per user, in two steps:

    python scripts/spotify_auth_setup.py
        -> prints a URL. Have the user open it in a browser and authorize
           the app. They'll be redirected to a 127.0.0.1 URL that won't
           load (that's expected) -- copy that full URL.

    python scripts/spotify_auth_setup.py "<redirect URL>" --email user@example.com
        -> exchanges it for a refresh token and registers the user in
           spotify_radar.users, so the DAG starts sending them digests.

Without --email, it just prints SPOTIFY_REFRESH_TOKEN=... for .env (the
single-user setup, which the DAG registers automatically on its next run
using ALERT_EMAIL_TO).

Requires SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET in your .env, a
Spotify app configured with redirect URI http://127.0.0.1:8888/callback,
and -- for --email -- the local Postgres reachable via the PG* env vars
(same as scripts/init_db.py). In Spotify's Development Mode, each user must
also be added under the app's "User Management" settings.
"""

import argparse
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyOAuth

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from include import db, spotify_client  # noqa: E402

REDIRECT_URI = "http://127.0.0.1:8888/callback"
# user-top-read lets the digest be ordered by the user's listening history.
SCOPE = "user-follow-read user-top-read"


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("redirect_url", nargs="?", help="the 127.0.0.1 URL you were redirected to")
    parser.add_argument("--email", help="register the user in Postgres with this digest address")
    args = parser.parse_args()

    oauth = SpotifyOAuth(
        client_id=os.environ["SPOTIFY_CLIENT_ID"],
        client_secret=os.environ["SPOTIFY_CLIENT_SECRET"],
        redirect_uri=REDIRECT_URI,
        scope=SCOPE,
        open_browser=False,
        cache_handler=None,
    )

    if not args.redirect_url:
        auth_url = oauth.get_authorize_url()
        print("1. Open this URL in your browser and authorize the app:\n")
        print(f"   {auth_url}\n")
        print("2. You'll be redirected to a 127.0.0.1 URL that won't load")
        print("   (that's expected). Copy that full URL, then run:\n")
        print('   python scripts/spotify_auth_setup.py "<redirect URL>" --email you@example.com')
        return

    code = oauth.parse_response_code(args.redirect_url)
    token_info = oauth.get_access_token(code, as_dict=True, check_cache=False)

    if not args.email:
        print("Success! Add this to your .env file:\n")
        print(f"SPOTIFY_REFRESH_TOKEN={token_info['refresh_token']}")
        return

    me = spotify_client.get_current_user(token_info["access_token"])
    conn = psycopg2.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=os.environ.get("PGPORT", "5432"),
        dbname=os.environ.get("PGDATABASE", "postgres"),
        user=os.environ.get("PGUSER", "postgres"),
        password=os.environ.get("PGPASSWORD", "postgres"),
    )
    try:
        db.upsert_user(me["id"], me["display_name"], args.email, token_info["refresh_token"], conn=conn)
    finally:
        conn.close()
    print(f"Registered Spotify user {me['id']} ({me['display_name']}) -> {args.email}")


if __name__ == "__main__":
    main()
