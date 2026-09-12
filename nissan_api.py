# Minimal NissanConnect/Kamereon client for EU Nissan vehicles.
# Authentication flow adapted from dan-r/HomeAssistant-NissanConnect v0.8.2 (MIT).

from __future__ import annotations

import base64
import hashlib
import json
import secrets
import time
from html.parser import HTMLParser
from urllib.parse import parse_qs, urljoin, urlparse

import requests
from oauthlib.oauth2 import TokenExpiredError
from requests_oauthlib import OAuth2Session


SETTINGS = {
    "client_id": "ZM3WK7ax1OtQKYQ8Qqzcv5VgiA8a",
    "scope": "openid name profile email offline_access",
    "kamereon_scope": "openid profile vehicles",
    "auth_base_url": "https://login.mynissan-account.com/",
    "redirect_uri": "com://wso2.service.nci",
    "auth_brand": "Nissan",
    "auth_client": "mynissanapp",
    "auth_platform": "Android",
    "auth_locale": "en_GB",
    "car_adapter_base_url": "https://alliance-platform-caradapter-prod.apps.eu2.kamereon.io/car-adapter/",
    "user_adapter_base_url": "https://alliance-platform-usersadapter-prod.apps.eu2.kamereon.io/user-adapter/",
    "user_base_url": "https://nci-bff-web-prod.apps.eu2.kamereon.io/bff-web/",
}


class NissanAuthError(RuntimeError):
    pass


class _LoginFormParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.forms = []
        self._form = None

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        if tag == "form":
            self._form = {
                "action": attributes.get("action"),
                "inputs": {},
            }
        elif tag == "input" and self._form is not None:
            name = attributes.get("name")
            if name:
                self._form["inputs"][name] = attributes.get("value", "")

    def handle_endtag(self, tag):
        if tag == "form" and self._form is not None:
            self.forms.append(self._form)
            self._form = None

    @property
    def login_form(self):
        for form in self.forms:
            inputs = form["inputs"]
            if "sessionDataKey" in inputs and "password" in inputs:
                return form
        return None


class NissanSession:
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self.session = requests.Session()
        self._oauth = None
        self._user_id = None
        self._kamereon_refresh_token = None

    @staticmethod
    def _generate_pkce_pair():
        verifier = secrets.token_urlsafe(64)
        digest = hashlib.sha256(verifier.encode("ascii")).digest()
        challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
        return verifier, challenge

    @staticmethod
    def _is_auth_url(url: str) -> bool:
        expected = urlparse(SETTINGS["auth_base_url"])
        parsed = urlparse(url)
        return (
            parsed.scheme == "https"
            and parsed.hostname == expected.hostname
            and (parsed.port or 443) == (expected.port or 443)
        )

    @staticmethod
    def _parse_token_response(response, name: str, require_id_token=False):
        try:
            data = response.json()
        except ValueError as exc:
            raise RuntimeError(f"Invalid {name} response") from exc
        if not response.ok or data.get("error") or not data.get("access_token"):
            raise RuntimeError(f"Unable to obtain {name}")
        if require_id_token and not data.get("id_token"):
            raise RuntimeError(f"Missing ID token in {name} response")
        return data

    def _follow_login_redirects(self, response):
        for _ in range(10):
            if response.is_redirect or response.is_permanent_redirect:
                location = response.headers.get("Location")
                if not location:
                    break
                target = urljoin(response.url, location)
                if not self._is_auth_url(target):
                    raise RuntimeError("Unexpected Nissan login redirect")
                response = self.session.get(target, allow_redirects=False, timeout=30)
                continue
            if response.ok and self._is_auth_url(response.url):
                return response
            break
        raise RuntimeError("Unable to load Nissan login")

    def _follow_authorization_redirects(self, response):
        expected_callback = urlparse(SETTINGS["redirect_uri"])
        for _ in range(10):
            if response.is_redirect or response.is_permanent_redirect:
                location = response.headers.get("Location")
                if not location:
                    break
                target = urljoin(response.url, location)
                parsed_target = urlparse(target)
                if (parsed_target.scheme, parsed_target.netloc) == (
                    expected_callback.scheme,
                    expected_callback.netloc,
                ):
                    return target
                if not self._is_auth_url(target):
                    raise RuntimeError("Unexpected Nissan authorization redirect")
                response = self.session.get(target, allow_redirects=False, timeout=30)
                continue
            if response.ok:
                parser = _LoginFormParser()
                parser.feed(response.text)
                if parser.login_form is not None:
                    raise NissanAuthError("Invalid credentials")
            break
        raise RuntimeError("Nissan login did not return an authorization code")

    def _authorization_code(self):
        verifier, challenge = self._generate_pkce_pair()
        state = secrets.token_urlsafe(32)
        response = self.session.get(
            urljoin(SETTINGS["auth_base_url"], "oauth2/authorize"),
            params={
                "response_type": "code",
                "redirect_uri": SETTINGS["redirect_uri"],
                "client_id": SETTINGS["client_id"],
                "state": state,
                "scope": SETTINGS["scope"],
                "code_challenge": challenge,
                "code_challenge_method": "S256",
                "locale": SETTINGS["auth_locale"],
                "brand": SETTINGS["auth_brand"],
                "client": SETTINGS["auth_client"],
            },
            allow_redirects=False,
            timeout=30,
        )
        response = self._follow_login_redirects(response)

        parser = _LoginFormParser()
        parser.feed(response.text)
        form = parser.login_form
        if form is None or not form["action"]:
            raise RuntimeError("Nissan login form is unavailable")

        login_data = dict(form["inputs"])
        login_region = login_data.get("regionCode", "")
        login_data.update(
            {
                "userName": self.username,
                "username": f"{login_region}/{self.username}" if login_region else self.username,
                "password": self.password,
            }
        )
        form_url = urljoin(response.url, form["action"])
        if not self._is_auth_url(form_url):
            raise RuntimeError("Unexpected Nissan login form target")
        origin = urlparse(form_url)
        response = self.session.post(
            form_url,
            data=login_data,
            headers={
                "Origin": f"{origin.scheme}://{origin.netloc}",
                "Referer": response.url,
            },
            allow_redirects=False,
            timeout=30,
        )

        callback_url = self._follow_authorization_redirects(response)
        callback = urlparse(callback_url)
        callback_data = parse_qs(callback.query)
        if callback_data.get("state", [None])[0] != state:
            raise RuntimeError("Invalid Nissan login state")
        code = callback_data.get("code", [None])[0]
        if not code:
            raise NissanAuthError("Invalid credentials")
        return code, verifier

    def login(self):
        self.session = requests.Session()
        code, verifier = self._authorization_code()

        response = self.session.post(
            urljoin(SETTINGS["auth_base_url"], "oauth2/token"),
            data={
                "redirect_uri": SETTINGS["redirect_uri"],
                "grant_type": "authorization_code",
                "client_id": SETTINGS["client_id"],
                "code": code,
                "code_verifier": verifier,
                "scope": SETTINGS["scope"],
            },
            allow_redirects=False,
            timeout=30,
        )
        oneid = self._parse_token_response(response, "Nissan OneID token", require_id_token=True)

        response = self.session.post(
            urljoin(SETTINGS["user_base_url"], "v1/oauth2/access_token"),
            params={"platform": SETTINGS["auth_platform"]},
            headers={
                "Authorization": oneid["id_token"],
                "Content-Type": "application/vnd.api+json",
            },
            allow_redirects=False,
            timeout=30,
        )
        self._install_kamereon_token(self._parse_token_response(response, "Kamereon token"))

    def _install_kamereon_token(self, token):
        expires_in = int(token.get("expires_in", 3600))
        refresh_token = token.get("refresh_token") or self._kamereon_refresh_token
        oauth_token = {
            "access_token": token["access_token"],
            "token_type": token.get("token_type", "Bearer"),
            "expires_in": expires_in,
            "expires_at": time.time() + expires_in,
        }
        if refresh_token:
            oauth_token["refresh_token"] = refresh_token
        self._kamereon_refresh_token = refresh_token
        self._oauth = OAuth2Session(client_id=SETTINGS["client_id"], token=oauth_token)

    def _refresh_authentication(self):
        if self._kamereon_refresh_token:
            try:
                response = self.session.post(
                    urljoin(SETTINGS["user_base_url"], "v1/oauth2/refresh-token"),
                    params={"platform": SETTINGS["auth_platform"]},
                    headers={
                        "Authorization": self._kamereon_refresh_token,
                        "Content-Type": "application/vnd.api+json",
                    },
                    data=json.dumps({"scope": SETTINGS["kamereon_scope"]}),
                    allow_redirects=False,
                    timeout=30,
                )
                self._install_kamereon_token(
                    self._parse_token_response(response, "Kamereon refresh token")
                )
                return
            except Exception:
                pass
        self.login()

    def request(self, method: str, url: str, **kwargs):
        if self._oauth is None:
            self.login()
        for attempt in range(2):
            try:
                response = self._oauth.request(method, url, **kwargs)
            except TokenExpiredError:
                if attempt:
                    raise
                self._refresh_authentication()
                continue
            if response.status_code != 401:
                response.raise_for_status()
                return response
            if not attempt:
                self._refresh_authentication()
        raise TokenExpiredError()

    @property
    def user_id(self):
        if self._user_id is None:
            response = self.request(
                "GET",
                f'{SETTINGS["user_adapter_base_url"]}v1/users/current',
            )
            self._user_id = response.json()["userId"]
        return self._user_id

    def vehicles(self):
        response = self.request(
            "GET",
            f'{SETTINGS["user_base_url"]}v5/users/{self.user_id}/cars',
        )
        return response.json()["data"]

    def battery_status(self, vin: str):
        response = self.request(
            "GET",
            f'{SETTINGS["car_adapter_base_url"]}v1/cars/{vin}/battery-status',
            headers={"Content-Type": "application/vnd.api+json"},
        )
        return response.json()["data"]["attributes"]
