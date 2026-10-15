import pytest

from payment_service.messaging.topology import RetryPolicy


def test_delays_grow_exponentially() -> None:
    policy = RetryPolicy(max_attempts=5, base_delay_seconds=2, max_delay_seconds=300)
    assert [policy.delay_ms(n) for n in range(1, 5)] == [2000, 4000, 8000, 16000]


def test_delay_is_capped() -> None:
    policy = RetryPolicy(max_attempts=10, base_delay_seconds=2, max_delay_seconds=10)
    assert policy.delay_ms(9) == 10_000


def test_retry_queues_cover_every_retry() -> None:
    assert RetryPolicy(max_attempts=3, base_delay_seconds=2, max_delay_seconds=300).retry_delays_ms() == [2000, 4000]
    assert RetryPolicy(max_attempts=1, base_delay_seconds=2, max_delay_seconds=300).retry_delays_ms() == [2000]


def test_attempts_are_one_based() -> None:
    with pytest.raises(ValueError, match="1-based"):
        RetryPolicy(max_attempts=3, base_delay_seconds=1, max_delay_seconds=10).delay_ms(0)
