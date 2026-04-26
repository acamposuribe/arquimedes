"""OAuth/OIDC helpers for the remote Arquimedes MCP server."""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import urlopen


_DEFAULT_JWT_ALGORITHMS = ("RS256", "RS384", "RS512", "ES256", "ES384", "ES512")


@dataclass(frozen=True)
class OIDCAuthConfig:
    issuer_url: str
    resource_server_url: str
    required_scopes: tuple[str, ...] = ()
    audience: tuple[str, ...] = ()
    allowed_subjects: frozenset[str] = frozenset()
    allowed_emails: frozenset[str] = frozenset()
    allowed_email_domains: frozenset[str] = frozenset()
    service_documentation_url: str | None = None
    jwks_url: str | None = None


def _normalized_hosted_well_known_urls(issuer_url: str) -> list[str]:
    parsed = urlparse(issuer_url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    path = parsed.path.rstrip("/")
    return [
        f"{base}/.well-known/openid-configuration{path}",
        f"{base}/.well-known/oauth-authorization-server{path}",
    ]


class OIDCTokenVerifier:
    """Validate OAuth bearer tokens issued by an external OIDC provider."""

    def __init__(self, config: OIDCAuthConfig):
        self.config = config
        self._jwks_client = None
        self._jwks_url: str | None = None

    def _load_json(self, url: str) -> dict[str, Any]:
        try:
            with urlopen(url, timeout=5) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"Failed to load OAuth metadata from {url}") from exc
        if not isinstance(payload, dict):
            raise RuntimeError(f"Expected JSON object from {url}")
        return payload

    def _resolve_jwks_url(self) -> str:
        if self.config.jwks_url:
            return self.config.jwks_url

        for url in _normalized_hosted_well_known_urls(self.config.issuer_url):
            try:
                metadata = self._load_json(url)
            except RuntimeError:
                continue
            jwks_uri = metadata.get("jwks_uri")
            if isinstance(jwks_uri, str) and jwks_uri:
                return jwks_uri
        raise RuntimeError(
            "Unable to resolve jwks_uri from issuer metadata. "
            "Set --auth-jwks-url explicitly if your provider does not expose standard discovery."
        )

    def _get_jwks_client(self):
        try:
            import jwt
        except ModuleNotFoundError as exc:  # pragma: no cover - exercised in real installs
            raise RuntimeError(
                "Missing JWT dependency: PyJWT. Install project dependencies for the current "
                "Python environment, e.g. `python3 -m pip install -e .`."
            ) from exc

        jwks_url = self._resolve_jwks_url()
        if self._jwks_client is None or self._jwks_url != jwks_url:
            self._jwks_client = jwt.PyJWKClient(jwks_url)
            self._jwks_url = jwks_url
        return jwt, self._jwks_client

    @staticmethod
    def _token_scopes(claims: dict[str, Any]) -> list[str]:
        scopes: list[str] = []
        raw_scope = claims.get("scope")
        if isinstance(raw_scope, str):
            scopes.extend(scope for scope in raw_scope.split() if scope)
        raw_scp = claims.get("scp")
        if isinstance(raw_scp, str):
            scopes.extend(scope for scope in raw_scp.split() if scope)
        elif isinstance(raw_scp, list):
            scopes.extend(str(scope) for scope in raw_scp if scope)
        seen: set[str] = set()
        unique: list[str] = []
        for scope in scopes:
            if scope not in seen:
                seen.add(scope)
                unique.append(scope)
        return unique

    def _principal_allowed(self, claims: dict[str, Any]) -> bool:
        subject = str(claims.get("sub") or "")
        email = str(claims.get("email") or "").strip().lower()
        email_domain = email.partition("@")[2]

        if self.config.allowed_subjects and subject not in self.config.allowed_subjects:
            return False
        if self.config.allowed_emails and email not in self.config.allowed_emails:
            return False
        if self.config.allowed_email_domains and email_domain not in self.config.allowed_email_domains:
            return False
        return True

    async def verify_token(self, token: str):
        from mcp.server.auth.provider import AccessToken

        jwt, jwks_client = self._get_jwks_client()
        audiences = list(self.config.audience) or [self.config.resource_server_url]
        try:
            signing_key = jwks_client.get_signing_key_from_jwt(token).key
            claims = jwt.decode(
                token,
                signing_key,
                algorithms=list(_DEFAULT_JWT_ALGORITHMS),
                issuer=self.config.issuer_url,
                audience=audiences,
                options={"require": ["exp", "iss", "sub"], "verify_aud": bool(audiences)},
            )
        except Exception:
            return None
        if not self._principal_allowed(claims):
            return None

        scopes = self._token_scopes(claims)
        resource = claims.get("aud")
        if isinstance(resource, list):
            resource = resource[0] if resource else None
        elif resource is not None:
            resource = str(resource)
        principal = str(claims.get("email") or claims.get("sub") or claims.get("azp") or "unknown")
        expires_at = int(claims["exp"]) if claims.get("exp") is not None else None
        return AccessToken(
            token=token,
            client_id=principal,
            scopes=scopes,
            expires_at=expires_at,
            resource=resource,
        )
