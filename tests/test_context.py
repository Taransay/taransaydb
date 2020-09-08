from datetime import timedelta
import pytest
from taransaydb.exceptions import ProgrammingError


def test_iterator_out_of_context_throws_error(float_device, faker):
    append_datetime = faker.date_time()

    with float_device.writer() as driver:
        driver.append(faker.date_time(), [faker.pyfloat()])

    with float_device.reader() as driver:
        # Query to 1 second after so the data point is returned.
        result = driver.query_interval(
            append_datetime, append_datetime + timedelta(seconds=1)
        )

    with pytest.raises(ProgrammingError):
        # The result should not be accessible outside the context of `float_device`
        list(result)


def test_read_not_allowed_in_write_context(float_device, faker):
    query_datetime = faker.date_time()

    with float_device.writer() as driver:
        with pytest.raises(
            ProgrammingError, match=r"not opened in a way that supports reading"
        ):
            driver.query_interval(query_datetime, query_datetime + timedelta(seconds=1))


def test_append_not_allowed_in_read_context(float_device, faker):
    append_datetime = faker.date_time()

    with float_device.reader() as driver:
        with pytest.raises(
            ProgrammingError, match=r"not opened in a way that supports appending"
        ):
            driver.append(append_datetime, [faker.pyfloat()])
