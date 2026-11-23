"""
Tests for exponential backoff calculation.

These tests verify that the backoff formula produces correct values
without relying on timing tolerances.
"""

import pytest
from datetime import datetime

from app.services.outbox_dispatcher import (
    OutboxDispatcher,
    INITIAL_BACKOFF_SECONDS,
    MAX_BACKOFF_SECONDS,
    BACKOFF_MULTIPLIER,
)
from app.services.queue_service import QueueService
from unittest.mock import MagicMock


@pytest.fixture
def dispatcher():
    """Create dispatcher instance for testing"""
    mock_queue_service = MagicMock(spec=QueueService)
    return OutboxDispatcher(mock_queue_service, batch_size=10)


def test_backoff_first_attempt(dispatcher):
    """
    Test backoff after first failed attempt.

    Formula: INITIAL * (MULTIPLIER ^ (attempts - 1))
    attempts=1: 1 * (2 ^ 0) = 1 second
    """
    now = datetime.utcnow()
    next_attempt = dispatcher._calculate_next_attempt_time(current_attempts=1)

    backoff_seconds = (next_attempt - now).total_seconds()

    # Should be exactly 1 second (allowing tiny floating point error)
    assert 0.99 <= backoff_seconds <= 1.01, f"Expected 1s, got {backoff_seconds}s"


def test_backoff_second_attempt(dispatcher):
    """
    Test backoff after second failed attempt.

    attempts=2: 1 * (2 ^ 1) = 2 seconds
    """
    now = datetime.utcnow()
    next_attempt = dispatcher._calculate_next_attempt_time(current_attempts=2)

    backoff_seconds = (next_attempt - now).total_seconds()

    assert 1.99 <= backoff_seconds <= 2.01, f"Expected 2s, got {backoff_seconds}s"


def test_backoff_third_attempt(dispatcher):
    """
    Test backoff after third failed attempt.

    attempts=3: 1 * (2 ^ 2) = 4 seconds
    """
    now = datetime.utcnow()
    next_attempt = dispatcher._calculate_next_attempt_time(current_attempts=3)

    backoff_seconds = (next_attempt - now).total_seconds()

    assert 3.99 <= backoff_seconds <= 4.01, f"Expected 4s, got {backoff_seconds}s"


def test_backoff_fourth_attempt(dispatcher):
    """
    Test backoff after fourth failed attempt.

    attempts=4: 1 * (2 ^ 3) = 8 seconds
    """
    now = datetime.utcnow()
    next_attempt = dispatcher._calculate_next_attempt_time(current_attempts=4)

    backoff_seconds = (next_attempt - now).total_seconds()

    assert 7.99 <= backoff_seconds <= 8.01, f"Expected 8s, got {backoff_seconds}s"


def test_backoff_fifth_attempt(dispatcher):
    """
    Test backoff after fifth failed attempt.

    attempts=5: 1 * (2 ^ 4) = 16 seconds
    """
    now = datetime.utcnow()
    next_attempt = dispatcher._calculate_next_attempt_time(current_attempts=5)

    backoff_seconds = (next_attempt - now).total_seconds()

    assert 15.99 <= backoff_seconds <= 16.01, f"Expected 16s, got {backoff_seconds}s"


def test_backoff_exponential_sequence(dispatcher):
    """Test that backoff doubles each time until cap."""
    expected_sequence = [
        (1, 1),  # 2^0
        (2, 2),  # 2^1
        (3, 4),  # 2^2
        (4, 8),  # 2^3
        (5, 16),  # 2^4
        (6, 32),  # 2^5
        (7, 64),  # 2^6
        (8, 128),  # 2^7
    ]

    for attempts, expected_seconds in expected_sequence:
        now = datetime.utcnow()
        next_attempt = dispatcher._calculate_next_attempt_time(current_attempts=attempts)
        backoff_seconds = (next_attempt - now).total_seconds()

        # Allow 0.01s tolerance for floating point
        assert (
            expected_seconds - 0.01 <= backoff_seconds <= expected_seconds + 0.01
        ), f"Attempt {attempts}: expected {expected_seconds}s, got {backoff_seconds}s"


def test_backoff_caps_at_maximum(dispatcher):
    """
    Test that backoff caps at MAX_BACKOFF_SECONDS (300s = 5 minutes).

    attempts=9: 1 * (2 ^ 8) = 256 seconds (under cap)
    attempts=10: 1 * (2 ^ 9) = 512 seconds → capped at 300
    attempts=100: huge → capped at 300
    """
    # Just under cap
    now = datetime.utcnow()
    next_attempt = dispatcher._calculate_next_attempt_time(current_attempts=9)
    backoff_seconds = (next_attempt - now).total_seconds()
    assert 255.99 <= backoff_seconds <= 256.01, "Attempt 9 should be 256s (under cap)"

    # At cap
    now = datetime.utcnow()
    next_attempt = dispatcher._calculate_next_attempt_time(current_attempts=10)
    backoff_seconds = (next_attempt - now).total_seconds()
    assert 299.99 <= backoff_seconds <= 300.01, "Attempt 10 should be capped at 300s"

    # Well over cap
    now = datetime.utcnow()
    next_attempt = dispatcher._calculate_next_attempt_time(current_attempts=100)
    backoff_seconds = (next_attempt - now).total_seconds()
    assert 299.99 <= backoff_seconds <= 300.01, "Attempt 100 should be capped at 300s"


def test_backoff_constants_are_correct():
    """Verify backoff constants match documentation."""
    assert INITIAL_BACKOFF_SECONDS == 1, "Initial backoff should be 1 second"
    assert MAX_BACKOFF_SECONDS == 300, "Max backoff should be 300 seconds (5 minutes)"
    assert BACKOFF_MULTIPLIER == 2, "Backoff should double each time"


def test_backoff_formula_matches_documentation(dispatcher):
    """
    Verify the backoff formula produces the exact sequence documented.

    Documentation states:
    - First failure: 1 second
    - Second failure: 2 seconds
    - Third failure: 4 seconds
    - Fourth failure: 8 seconds
    - Continues doubling until capped at 300 seconds
    """
    documented_sequence = [
        (1, 1, "First failure"),
        (2, 2, "Second failure"),
        (3, 4, "Third failure"),
        (4, 8, "Fourth failure"),
        (5, 16, "Fifth failure"),
        (8, 128, "Eighth failure"),
        (9, 256, "Ninth failure (last before cap)"),
        (10, 300, "Tenth failure (capped)"),
        (20, 300, "Twentieth failure (capped)"),
    ]

    for attempts, expected_seconds, description in documented_sequence:
        now = datetime.utcnow()
        next_attempt = dispatcher._calculate_next_attempt_time(current_attempts=attempts)
        backoff_seconds = (next_attempt - now).total_seconds()

        assert (
            expected_seconds - 0.01 <= backoff_seconds <= expected_seconds + 0.01
        ), f"{description}: expected {expected_seconds}s, got {backoff_seconds}s"
