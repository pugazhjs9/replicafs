import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from replicafs.limits import UploadBodyLimit


@pytest.fixture
def limited_client():
    app = FastAPI()
    app.add_middleware(UploadBodyLimit, limit=lambda: 8)

    @app.post("/files")
    @app.post("/other")
    async def read(request: Request):
        return {"size": len(await request.body())}

    return TestClient(app)


def test_declared_oversize_rejected(limited_client):
    assert limited_client.post("/files", content=b"123456789").status_code == 413
    assert limited_client.post("/files", content=b"12345678").json() == {"size": 8}
    assert limited_client.post("/other", content=b"123456789").status_code == 200


def test_chunked_body_limit_without_content_length(limited_client):
    response = limited_client.post("/files", content=iter([b"1234", b"5678", b"9"]))
    assert response.status_code == 413
    assert response.json()["detail"] == "upload request too large"


@pytest.mark.parametrize("length", ["bad", "-1"])
def test_bad_length(limited_client, length):
    assert limited_client.post("/files", content=b"x", headers={"content-length": length}).status_code == 400


def test_lying_small_length_still_bounded(limited_client):
    assert (
        limited_client.post("/files", content=b"123456789", headers={"content-length": "1"}).status_code
        == 413
    )
