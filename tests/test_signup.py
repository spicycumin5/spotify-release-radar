"""Tests for signup/app.py, the signup and unsubscribe site (mocked Spotify and DB)."""

from __future__ import annotations

from unittest import mock
from urllib.parse import parse_qs, urlparse

import pytest
from fastapi.testclient import TestClient

from signup import app as signup


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("SPOTIFY_CLIENT_ID", "client-id")
    monkeypatch.setenv("SPOTIFY_CLIENT_SECRET", "client-secret")
    monkeypatch.setenv("RADAR_PUBLIC_URL", "https://radar.example.com/")
    return TestClient(signup.app, base_url="https://radar.example.com")


def _response(status_code, body=None):
    return mock.Mock(status_code=status_code, json=mock.Mock(return_value=body or {}))


def _start(client, email="friend@example.com"):
    response = client.post("/signup", data={"email": email}, follow_redirects=False)
    assert response.status_code == 303
    return parse_qs(urlparse(response.headers["location"]).query)


def test_signup_redirects_to_spotify_with_state_and_scopes(client):
    params = _start(client)

    assert params["client_id"] == ["client-id"]
    assert params["redirect_uri"] == ["https://radar.example.com/callback"]
    assert params["scope"] == ["user-follow-read user-top-read"]
    assert params["state"][0] == client.cookies[signup.STATE_COOKIE]


def test_signup_rejects_bad_email(client):
    response = client.post("/signup", data={"email": "not-an-email"}, follow_redirects=False)

    assert response.status_code == 400


def test_callback_registers_user(client, monkeypatch):
    state = _start(client)["state"][0]
    post = mock.Mock(return_value=_response(200, {"access_token": "at", "refresh_token": "rt"}))
    get = mock.Mock(return_value=_response(200, {"id": "friend", "display_name": "Friend"}))
    register = mock.Mock()
    monkeypatch.setattr(signup.requests, "post", post)
    monkeypatch.setattr(signup.requests, "get", get)
    monkeypatch.setattr(signup, "register_user", register)

    response = client.get("/callback", params={"code": "the-code", "state": state})

    assert response.status_code == 200
    assert "You're in" in response.text
    register.assert_called_once_with("friend", "Friend", "friend@example.com", "rt")
    assert post.call_args.kwargs["data"]["redirect_uri"] == "https://radar.example.com/callback"
    assert post.call_args.kwargs["auth"] == ("client-id", "client-secret")


def test_callback_rejects_state_mismatch(client, monkeypatch):
    _start(client)
    register = mock.Mock()
    monkeypatch.setattr(signup, "register_user", register)

    response = client.get("/callback", params={"code": "the-code", "state": "forged"})

    assert response.status_code == 400
    register.assert_not_called()


def test_callback_explains_invite_when_spotify_returns_403(client, monkeypatch):
    state = _start(client)["state"][0]
    monkeypatch.setattr(signup.requests, "post", mock.Mock(return_value=_response(200, {"access_token": "at", "refresh_token": "rt"})))
    monkeypatch.setattr(signup.requests, "get", mock.Mock(return_value=_response(403)))
    register = mock.Mock()
    monkeypatch.setattr(signup, "register_user", register)

    response = client.get("/callback", params={"code": "the-code", "state": state})

    assert response.status_code == 403
    assert "guest list" in response.text
    register.assert_not_called()


def test_callback_handles_denied_consent(client):
    response = client.get("/callback", params={"error": "access_denied"})

    assert response.status_code == 400
    assert "access_denied" in response.text


def test_unsubscribe_link_only_confirms(client, monkeypatch):
    monkeypatch.setattr(signup, "find_user_by_unsubscribe_token", lambda token: ("friend@example.com", True))
    deactivate = mock.Mock()
    monkeypatch.setattr(signup, "deactivate_user", deactivate)

    response = client.get("/unsubscribe/tok")

    assert response.status_code == 200
    assert 'action="/unsubscribe/tok"' in response.text
    deactivate.assert_not_called()


def test_unsubscribe_post_deactivates(client, monkeypatch):
    deactivate = mock.Mock(return_value=True)
    monkeypatch.setattr(signup, "deactivate_user", deactivate)

    # Gmail's one-click unsubscribe sends this body (RFC 8058).
    response = client.post("/unsubscribe/tok", data={"List-Unsubscribe": "One-Click"})

    assert response.status_code == 200
    deactivate.assert_called_once_with("tok")


def test_unknown_unsubscribe_token(client, monkeypatch):
    monkeypatch.setattr(signup, "find_user_by_unsubscribe_token", lambda token: None)
    monkeypatch.setattr(signup, "deactivate_user", lambda token: False)

    assert client.get("/unsubscribe/nope").status_code == 404
    assert client.post("/unsubscribe/nope").status_code == 404
