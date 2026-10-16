import io
import json
from urllib.error import HTTPError

import pytest

from gov_transparency_hub.resources import portal_transparencia_api as api


class FakeResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _http_error(code):
    return HTTPError("http://portal", code, "error", {}, None)


@pytest.fixture
def portal(monkeypatch):
    """Scripted urlopen: each item is an HTTP status to raise or a dict to return."""
    script, waits = [], []

    def fake_urlopen(request, timeout):
        outcome = script.pop(0)
        if isinstance(outcome, int):
            raise _http_error(outcome)
        return FakeResponse(json.dumps(outcome).encode())

    monkeypatch.setattr(api, "urlopen", fake_urlopen)
    monkeypatch.setattr(api.time, "sleep", waits.append)
    return script, waits


def test_cloudflare_timeout_524_is_retried(portal):
    script, waits = portal
    script.extend([524, {"resultado": "ok"}])

    assert api.fetch("http://portal") == {"resultado": "ok"}
    assert len(waits) == 1


def test_gateway_errors_back_off_longer(portal):
    script, waits = portal
    script.extend([502, 522, {"resultado": "ok"}])

    api.fetch("http://portal")

    assert waits == [10, 20]


def test_gateway_error_gives_up_after_max_retries(portal):
    script, waits = portal
    script.extend([502] * api.MAX_RETRIES)

    with pytest.raises(HTTPError):
        api.fetch("http://portal")
    assert len(waits) == api.MAX_RETRIES - 1


def test_forbidden_is_not_retried(portal):
    script, waits = portal
    script.extend([403])

    with pytest.raises(HTTPError):
        api.fetch("http://portal")
    assert waits == []
