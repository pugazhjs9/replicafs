"""Optional shared tokens for a private, single-user cluster.

These tokens provide authentication, not encryption or per-user permissions.
Use HTTPS or a private encrypted network when traffic leaves your computer.
"""
import os
import secrets

from fastapi import Header, HTTPException


def _require_token(variable: str, authorization: str | None) -> None:
    expected = os.environ.get(variable, "")
    if expected and not secrets.compare_digest(
        (authorization or "").encode(), f"Bearer {expected}".encode()
    ):
        raise HTTPException(
            status_code=401,
            detail="invalid or missing bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )


def require_api_token(authorization: str | None = Header(default=None)) -> None:
    _require_token("API_TOKEN", authorization)


def require_cluster_token(authorization: str | None = Header(default=None)) -> None:
    _require_token("CLUSTER_TOKEN", authorization)


def cluster_headers() -> dict[str, str]:
    token = os.environ.get("CLUSTER_TOKEN", "")
    return {"Authorization": f"Bearer {token}"} if token else {}
