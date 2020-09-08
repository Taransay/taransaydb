from datetime import datetime, timedelta
import pytest


@pytest.fixture
def regular_interval_data_device(test_device, faker):
    # 2 weeks of data at 30 second intervals.
    start = datetime(2020, 4, 30, 4, 48, 30)
    stop = datetime(2020, 5, 13, 2, 57, 0)
    interval = timedelta(seconds=30)

    with test_device.writer() as driver:
        for tick, value in faker.time_series(start, stop, interval):
            driver.append_data(tick, [value])

    with test_device.reader() as driver:
        yield driver, start, stop, interval


def test_empty_group_query_is_empty(test_device, faker):
    some_datetime = faker.date_time()

    with test_device.reader() as driver:
        result = driver.query_interval(some_datetime, some_datetime)

        assert list(result) == []


def test_stop_same_as_start_is_always_empty(test_device, faker):
    """Query ranges where stop == start return nothing."""
    some_datetime = faker.date_time()

    with test_device.writer() as driver:
        driver.append_data(some_datetime, [faker.pyfloat()])

    with test_device.reader() as driver:
        result = driver.query_interval(some_datetime, some_datetime)

        assert list(result) == []


def test_date_range(regular_interval_data_device):
    driver, start, stop, interval = regular_interval_data_device

    # Some reference points.
    oneintervalin = start + timedelta(seconds=30)
    onedayin = start + timedelta(days=1)
    oneweekin = start + timedelta(days=7)

    # Total data.
    assert len(list(driver.query_interval(start, stop))) == (stop - start) / interval
    # One interval of data.
    assert (
        len(list(driver.query_interval(start, oneintervalin)))
        == (oneintervalin - start) / interval
    )
    # One day of data.
    assert (
        len(list(driver.query_interval(start, onedayin)))
        == (onedayin - start) / interval
    )
    # One week of data.
    assert (
        len(list(driver.query_interval(start, oneweekin)))
        == (oneweekin - start) / interval
    )


def test_reverse(regular_interval_data_device, faker):
    driver, start, stop, _ = regular_interval_data_device

    threshold = faker.date_time_between_dates(start, stop)

    query_forward = list(driver.query_interval(start, threshold))
    query_reverse = list(reversed(driver.query_interval(start, threshold)))

    assert query_reverse == list(reversed(query_forward))
