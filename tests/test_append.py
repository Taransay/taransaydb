from datetime import timedelta


def test_append(float_device, faker):
    append_datetime = faker.date_time()
    append_data = [faker.pyfloat(), faker.pyfloat()]

    with float_device.writer() as driver:
        driver.append(append_datetime, append_data)

    with float_device.reader() as driver:
        # Query to 1 second after so the data point is returned.
        result = list(
            driver.query_interval(
                append_datetime, append_datetime + timedelta(seconds=1)
            )
        )

    assert result == [[append_datetime] + append_data]
