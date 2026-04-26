from __future__ import annotations

import asyncio

from arquimedes.mcp_auth import OIDCAuthConfig, OIDCTokenVerifier, _normalized_hosted_well_known_urls


def test_normalized_well_known_urls_with_path():
    urls = _normalized_hosted_well_known_urls("https://auth.example.com/oidc")

    assert urls == [
        "https://auth.example.com/.well-known/openid-configuration/oidc",
        "https://auth.example.com/.well-known/oauth-authorization-server/oidc",
    ]


def test_token_scopes_merges_scope_claims():
    claims = {
        "scope": "arq.read offline_access",
        "scp": ["arq.read", "arq.personal"],
    }

    scopes = OIDCTokenVerifier._token_scopes(claims)

    assert scopes == ["arq.read", "offline_access", "arq.personal"]


def test_verify_token_enforces_principal_filters(monkeypatch):
    config = OIDCAuthConfig(
        issuer_url="https://auth.example.com",
        resource_server_url="https://mcp.example.com/mcp",
        audience=("https://mcp.example.com/mcp",),
        allowed_emails=frozenset({"owner@example.com"}),
    )
    verifier = OIDCTokenVerifier(config)

    class FakeJWT:
        class PyJWKClient:
            def __init__(self, jwks_url):
                self.jwks_url = jwks_url

        @staticmethod
        def decode(*args, **kwargs):
            return {
                "iss": "https://auth.example.com",
                "sub": "user-1",
                "email": "someone@example.com",
                "aud": "https://mcp.example.com/mcp",
                "scope": "arq.read",
                "exp": 2000000000,
            }

    class FakeKey:
        key = "secret"

    class FakeJWKClient:
        def get_signing_key_from_jwt(self, token):
            return FakeKey()

    monkeypatch.setattr(verifier, "_get_jwks_client", lambda: (FakeJWT, FakeJWKClient()))

    payload = asyncio.run(verifier.verify_token("token"))

    assert payload is None


def test_verify_token_returns_access_token(monkeypatch):
    config = OIDCAuthConfig(
        issuer_url="https://auth.example.com",
        resource_server_url="https://mcp.example.com/mcp",
        audience=("https://mcp.example.com/mcp",),
        allowed_email_domains=frozenset({"example.com"}),
    )
    verifier = OIDCTokenVerifier(config)

    class FakeJWT:
        class PyJWKClient:
            def __init__(self, jwks_url):
                self.jwks_url = jwks_url

        @staticmethod
        def decode(*args, **kwargs):
            return {
                "iss": "https://auth.example.com",
                "sub": "user-1",
                "email": "reader@example.com",
                "aud": ["https://mcp.example.com/mcp"],
                "scope": "arq.read offline_access",
                "exp": 2000000000,
            }

    class FakeKey:
        key = "secret"

    class FakeJWKClient:
        def get_signing_key_from_jwt(self, token):
            return FakeKey()

    monkeypatch.setattr(verifier, "_get_jwks_client", lambda: (FakeJWT, FakeJWKClient()))

    access = asyncio.run(verifier.verify_token("token"))

    assert access.client_id == "reader@example.com"
    assert access.scopes == ["arq.read", "offline_access"]
    assert access.resource == "https://mcp.example.com/mcp"
