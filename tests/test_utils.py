import datetime as dt

import pytest

from compliance_tracker.utils import normalize_since


def test_normalize_since_iso():
    now = dt.datetime(2024, 1, 2, tzinfo=dt.timezone.utc)
    result = normalize_since("2024-01-01T00:00:00Z", now=now)
    assert result == dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)


def test_normalize_since_numeric_string():
    now = dt.datetime(2024, 1, 10, tzinfo=dt.timezone.utc)
    result = normalize_since("2", now=now)
    assert result == now - dt.timedelta(days=2)


def test_normalize_since_float():
    now = dt.datetime(2024, 1, 10, tzinfo=dt.timezone.utc)
    result = normalize_since(1.5, now=now)
    assert result == now - dt.timedelta(days=1.5)


def test_normalize_since_rejects_negative():
    with pytest.raises(ValueError):
        normalize_since(-1)
