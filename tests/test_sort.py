from datetime import datetime, timedelta


def test_sort(float_device, faker):
    start = datetime(2020, 3, 24, 0, 0, 0)
    stop = datetime(2020, 4, 9, 23, 59, 59)

    # Generate readings with randomly ordered datetimes.
    data = []
    for _ in range(100):
        tick = faker.date_time_between(start, stop)
        readings = [faker.pyfloat()]
        data.append([tick, readings])

    tmp = float_device.path / "tmpsort.dat"
    tmp.parent.mkdir(parents=True)

    with float_device.writer() as driver:
        for tick, readings in data:
            driver.append(tick, readings)

        driver.sort()

    with float_device.reader() as driver:
        # Query to 1 second after so the data point is returned.
        result = list(driver.query_interval(start, stop + timedelta(seconds=1)))

    assert result == sorted(data, key=lambda row: row[0])
