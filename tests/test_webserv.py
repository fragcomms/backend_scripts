from fastapi.testclient import TestClient
import sys
import os
import time

src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../src"))
sys.path.insert(0, src_path)
from server import app  # noqa: E402


def gitaction_awareness():
  return os.getenv("GITHUB_ACTIONS") == "true"


def test_webserver():
  if gitaction_awareness():  # if this is ran in github actions
    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
  else:
    with TestClient(app) as client:  # local tests use lifespan
      time.sleep(5)
      response = client.get("/health")  # /health to see if web server is responsive
      assert response.status_code == 200
      assert response.json() == {"status": "ok"}
      response = client.post(
        "/transcribe", json={"file_path": "bruh"}
      )  # expected to not be found
      assert response.status_code == 404
