"""Tests for YumanClient.

These tests use the `requests_mock` fixture provided by the
`pytest‑requests‑mock` plugin (add to dev requirements if missing).
No real HTTP calls are made.
"""

import os
from typing import List, Dict

import pytest

from yuman_client import YumanClient

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BASE_URL = "https://api.yuman.io/v1"


def _mk_client() -> YumanClient:
    token = os.environ.get("YUMAN_TOKEN", "test-token")
    return YumanClient(token=token)


# ---------------------------------------------------------------------------
# Pagination — list_sites() should transparently merge pages 1..n
# ---------------------------------------------------------------------------

def test_list_sites_pagination(requests_mock):
    client = _mk_client()

    # Page 1
    requests_mock.get(
        f"{BASE_URL}/sites",
        json={
            "total_pages": 2,
            "total_entries": 3,
            "items": [
                {"id": 1, "name": "Site 1"},
                {"id": 2, "name": "Site 2"},
            ],
        },
        additional_matcher=lambda r: r.qs == {"page": ["1"], "per_page": ["100"]},
    )

    # Page 2
    requests_mock.get(
        f"{BASE_URL}/sites",
        json={
            "total_pages": 2,
            "total_entries": 3,
            "items": [
                {"id": 3, "name": "Site 3"},
            ],
        },
        additional_matcher=lambda r: r.qs == {"page": ["2"], "per_page": ["100"]},
    )

    sites: List[Dict] = client.list_sites()
    ids = [s["id"] for s in sites]
    assert ids == [1, 2, 3]


# ---------------------------------------------------------------------------
# Retry 429 — _get should sleep and retry automatically
# ---------------------------------------------------------------------------

def test_retry_on_429(requests_mock, monkeypatch):
    client = _mk_client()

    # Counter to ensure second call invoked
    call_counter = {"n": 0}

    def _callback(request, context):
        call_counter["n"] += 1
        if call_counter["n"] == 1:
            context.status_code = 429  # first attempt triggers retry
            return {}
        context.status_code = 200
        return {"total_pages": 1, "items": []}

    requests_mock.get(f"{BASE_URL}/sites", json=_callback)

    sites = client.list_sites()
    assert sites == []
    # We should have called twice (1st 429, 2nd success)
    assert call_counter["n"] == 2


# ---------------------------------------------------------------------------
# create_site — ensure POST payload is relayed and response returned
# ---------------------------------------------------------------------------

def test_create_site(requests_mock):
    client = _mk_client()

    payload = {"name": "Test Site"}
    response_body = {"id": 123, **payload}

    requests_mock.post(f"{BASE_URL}/sites", status_code=201, json=response_body)

    result = client.create_site(payload)
    assert result == response_body


# ---------------------------------------------------------------------------
# update_material — ensure PATCH method works and returns updated object
# ---------------------------------------------------------------------------

def test_update_material(requests_mock):
    client = _mk_client()

    material_id = 42
    patch = {"serial_number": "SN123"}
    updated = {"id": material_id, **patch}

    requests_mock.patch(f"{BASE_URL}/materials/{material_id}", json=updated)

    result = client.update_material(material_id, patch)
    assert result["serial_number"] == "SN123"
