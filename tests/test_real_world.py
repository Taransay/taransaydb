from datetime import datetime, timedelta


def test_real_world(float_device, faker):
    start = datetime(2020, 3, 26, 19, 30, 0)
    stop = datetime(2020, 6, 13, 12, 0, 0)
    interval = timedelta(minutes=30)

    # Nominal data points.
    nnominal = (stop - start) / interval

    with float_device.writer() as driver:
        for tick, value in faker.time_series(start, stop, interval):
            driver.append(tick, [value])

    with float_device.reader() as driver:
        # Total data.
        assert len(list(driver.query_interval(start, stop))) == nnominal

    # Insert some values in random locations.
    with float_device.writer() as driver:
        driver.insert(faker.date_time_between(start, stop), [faker.pyfloat()])
        driver.insert(faker.date_time_between(start, stop), [faker.pyfloat()])
        driver.insert(faker.date_time_between(start, stop), [faker.pyfloat()])

    with float_device.reader() as driver:
        # Total data.
        assert len(list(driver.query_interval(start, stop))) == nnominal + 3
