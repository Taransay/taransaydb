import pytest
from taransaydb import Device


@pytest.fixture
def test_device(tmp_path):
    """Empty device."""
    return Device(tmp_path, "test_device")


@pytest.fixture
def float_device(monkeypatch, test_device):
    # When parsing, convert to float. No need to implement test_device.format_data as by default it
    # calls __str__ like we would do anyway.
    monkeypatch.setattr(
        test_device, "parse_data", lambda data: [float(value) for value in data]
    )
    return test_device
