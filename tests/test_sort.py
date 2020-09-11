from datetime import datetime, timedelta
from copy import copy
from random import shuffle


def test_sort(float_device, faker):
    """Test database sorting.

    This generates unique datetimes in a regularly spaced interval. The datetimes must be unique
    because the sorting algorithm is a merge sort which is not stable; this rules out the use of
    Faker's `date_times_between` method.
    """
    start = datetime(2020, 3, 24, 0, 0, 0)
    stop = datetime(2020, 4, 9, 23, 59, 59)
    interval = timedelta(minutes=5)

    data = []

    for tick, value in faker.time_series(start, stop, interval):
        data.append([tick, [value]])

    # Copy data and randomly reorder.
    shuffled_data = copy(data)
    shuffle(shuffled_data)

    with float_device.writer() as driver:
        for tick, readings in shuffled_data:
            driver.append(tick, readings)

        # Sort back into ascending time order.
        driver.sort()

    with float_device.reader() as driver:
        # Query to 1 second after so the data point is returned.
        result = list(driver.query_interval(start, stop + timedelta(seconds=1)))

    assert result == data
