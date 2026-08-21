import pytest

import backend.analytics as analytics


@pytest.fixture(autouse=True)
def disable_external_analytics(monkeypatch):
    """Local tests must never send events to a configured GA4 property."""
    monkeypatch.setattr(analytics, "GA4_MEASUREMENT_ID", "")
    monkeypatch.setattr(analytics, "GA4_API_SECRET", "")
