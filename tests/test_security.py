import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from replicafs.security import cluster_headers, require_api_token, require_cluster_token


@pytest.mark.parametrize(
    "variable,dependency",
    [
        ("API_TOKEN", require_api_token),
        ("CLUSTER_TOKEN", require_cluster_token),
    ],
)
def test_tokens_are_optional_and_checked_exactly(monkeypatch, variable, dependency):
    app = FastAPI()
    app.get("/private", dependencies=[Depends(dependency)])(lambda: {"ok": True})
    with TestClient(app) as client:
        monkeypatch.delenv(variable, raising=False)
        assert client.get("/private").status_code == 200
        monkeypatch.setenv(variable, "secret")
        for headers in ({}, {"Authorization": "Bearer wrong"}, {"Authorization": "secret"}):
            response = client.get("/private", headers=headers)
            assert response.status_code == 401
            assert response.headers["www-authenticate"] == "Bearer"
        assert client.get("/private", headers={"Authorization": "Bearer secret"}).status_code == 200


def test_internal_headers(monkeypatch):
    monkeypatch.delenv("CLUSTER_TOKEN", raising=False)
    assert cluster_headers() == {}
    monkeypatch.setenv("CLUSTER_TOKEN", "cluster-secret")
    assert cluster_headers() == {"Authorization": "Bearer cluster-secret"}
