import pytest

from persil.retain import (
    RetentionApplicator,
    at_most,
    every,
    maximum,
    minimum,
    serial,
    throttle,
)

from .common import one_test_per_assert


@pytest.fixture
def some_history():
    return [
        {"serial": i, "time": i, "data": {"value": ((i - 50) / 50) ** 2}}
        for i in range(100)
    ]


@pytest.fixture
def check(some_history):
    def _check(policy, expected):
        app = RetentionApplicator(policy)
        results = []
        for entry in some_history:
            _, results = app(entry, results)

        results = [entry["serial"] for entry in results]
        print(results)
        print(expected)
        return results == expected

    return _check


@one_test_per_assert
def test_serial(check):
    assert check(serial(0), [0])
    assert check(serial(51, 4), [4, 51])


@one_test_per_assert
def test_every(check):
    assert check(every(10), list(range(0, 100, 10)))
    assert check(every(17), list(range(0, 100, 17)))
    assert check(every(17, offset=1), list(range(1, 100, 17)))
    assert check(every(17, offset=-1), list(range(16, 100, 17)))


@one_test_per_assert
def test_throttle(check):
    assert check(throttle(7), list(range(0, 100, 7)))
    assert check(throttle(minutes=1), list(range(0, 100, 60)))


@one_test_per_assert
def test_minimum(check):
    assert check(minimum("value"), [50])


@one_test_per_assert
def test_maximum(check):
    assert check(maximum("value"), [0])


@one_test_per_assert
def test_at_most(check):
    assert check(every(3) & at_most(5), [0, 27, 63, 96, 99])


@one_test_per_assert
def test_intersect(check):
    assert check(every(7) & every(5), list(range(0, 100, 35)))


@one_test_per_assert
def test_union(check):
    assert check(
        every(7) | every(5), sorted(set(range(0, 100, 5)) | set(range(0, 100, 7)))
    )
