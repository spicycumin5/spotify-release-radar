"""One-time interactive script to obtain a Spotify refresh token.

Run this locally once (NOT inside Airflow):

    python scripts/spotify_auth_setup.py

It walks you through the Authorization Code flow for the `user-follow-read`
scope and prints a long-lived refresh token. Put that value in your `.env`
as SPOTIFY_REFRESH_TOKEN -- the Airflow DAGs use it to mint fresh access
tokens on every run via include/spotify_client.get_access_token().

Requires SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET to be set in the
environment (e.g. exported from your .env), and a Spotify app configured
with redirect URI http://localhost:8888/callback.
"""

import os

from spotipy.oauth2 import SpotifyOAuth

REDIRECT_URI = "http://localhost:8888/callback"
SCOPE = "user-follow-read"


def main() -> None:
    client_id = os.environ["SPOTIFY_CLIENT_ID"]
    client_secret = os.environ["SPOTIFY_CLIENT_SECRET"]

    oauth = SpotifyOAuth(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=REDIRECT_URI,
        scope=SCOPE,
        open_browser=False,
        cache_handler=None,
    )

    auth_url = oauth.get_authorize_url()
    print("1. Open this URL in your browser and authorize the app:\n")
    print(f"   {auth_url}\n")
    print("2. After authorizing, you'll be redirected to a localhost URL")
    print("   that won't load (that's expected). Copy that full URL.\n")

    redirected_url = input("Paste the full redirect URL here: ").strip()
    code = oauth.parse_response_code(redirected_url)
    token_info = oauth.get_access_token(code, as_dict=True, check_cache=False)

    print("\nSuccess! Add this to your .env file:\n")
    print(f"SPOTIFY_REFRESH_TOKEN={token_info['refresh_token']}")


if __name__ == "__main__":
    main()
