from datetime import datetime, timedelta


def test_insert(float_device, faker):
    tick1 = datetime(2020, 2, 15, 11, 57, 35)
    tick2 = datetime(2020, 2, 15, 12, 1, 20)
    insert_datetime = datetime(2020, 2, 15, 12, 0, 5)
    data = [faker.pyfloat(), faker.pyfloat()]

    with float_device.writer() as driver:
        driver.append(tick1, data)
        driver.append(tick2, data)
        driver.insert(insert_datetime, data)

    with float_device.reader() as driver:
        # Query to 1 second after so the data point is returned.
        result = list(driver.query_interval(tick1, tick2 + timedelta(seconds=1)))

    assert result == [[tick1, data], [insert_datetime, data], [tick2, data]]
