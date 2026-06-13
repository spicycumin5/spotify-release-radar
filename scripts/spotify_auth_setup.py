"""One-time script to obtain a Spotify refresh token.

Run this locally once (NOT inside Airflow), in two steps:

    python scripts/spotify_auth_setup.py
        -> prints a URL. Open it in a browser and authorize the app.
           You'll be redirected to a 127.0.0.1 URL that won't load
           (that's expected) -- copy that full URL.

    python scripts/spotify_auth_setup.py "<the redirect URL you copied>"
        -> exchanges it for a refresh token and prints
           SPOTIFY_REFRESH_TOKEN=... to add to your .env

Put that value in your .env -- the Airflow DAGs use it to mint fresh access
tokens on every run via include/spotify_client.get_access_token().

Requires SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET to be set in your
.env file, and a Spotify app configured with redirect URI
http://127.0.0.1:8888/callback.
"""

import os
import sys

from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyOAuth

REDIRECT_URI = "http://127.0.0.1:8888/callback"
SCOPE = "user-follow-read"


def main() -> None:
    load_dotenv()
    oauth = SpotifyOAuth(
        client_id=os.environ["SPOTIFY_CLIENT_ID"],
        client_secret=os.environ["SPOTIFY_CLIENT_SECRET"],
        redirect_uri=REDIRECT_URI,
        scope=SCOPE,
        open_browser=False,
        cache_handler=None,
    )

    if len(sys.argv) < 2:
        auth_url = oauth.get_authorize_url()
        print("1. Open this URL in your browser and authorize the app:\n")
        print(f"   {auth_url}\n")
        print("2. You'll be redirected to a 127.0.0.1 URL that won't load")
        print("   (that's expected). Copy that full URL, then run:\n")
        print('   python scripts/spotify_auth_setup.py "<redirect URL>"')
        return

    redirected_url = sys.argv[1]
    code = oauth.parse_response_code(redirected_url)
    token_info = oauth.get_access_token(code, as_dict=True, check_cache=False)

    print("Success! Add this to your .env file:\n")
    print(f"SPOTIFY_REFRESH_TOKEN={token_info['refresh_token']}")


if __name__ == "__main__":
    main()
