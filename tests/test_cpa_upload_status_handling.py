from src.core.upload import cpa_upload


class FakeResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        if self._payload is None:
            raise ValueError("no json payload")
        return self._payload


class FakeMime:
    def __init__(self):
        self.parts = []

    def addpart(self, **kwargs):
        self.parts.append(kwargs)


def test_upload_to_cpa_accepts_202_as_success(monkeypatch):
    monkeypatch.setattr(cpa_upload, "CurlMime", FakeMime)
    monkeypatch.setattr(
        cpa_upload.cffi_requests,
        "post",
        lambda url, **kwargs: FakeResponse(status_code=202),
    )

    success, message = cpa_upload.upload_to_cpa(
        {"email": "tester@example.com"},
        api_url="https://cpa.example.com",
        api_token="token-123",
    )

    assert success is True
    assert message == "涓婁紶鎴愬姛"


def test_upload_to_cpa_falls_back_to_raw_json_when_multipart_returns_422(monkeypatch):
    calls = []
    responses = [
        FakeResponse(status_code=422, payload={"message": "validation failed"}),
        FakeResponse(status_code=204),
    ]

    def fake_post(url, **kwargs):
        calls.append({"url": url, "kwargs": kwargs})
        return responses.pop(0)

    monkeypatch.setattr(cpa_upload, "CurlMime", FakeMime)
    monkeypatch.setattr(cpa_upload.cffi_requests, "post", fake_post)

    success, message = cpa_upload.upload_to_cpa(
        {"email": "tester@example.com", "type": "codex"},
        api_url="https://cpa.example.com",
        api_token="token-123",
    )

    assert success is True
    assert message == "涓婁紶鎴愬姛"
    assert calls[0]["kwargs"]["multipart"] is not None
    assert calls[1]["url"] == "https://cpa.example.com/v0/management/auth-files?name=tester%40example.com.json"
    assert calls[1]["kwargs"]["headers"]["Content-Type"] == "application/json"
