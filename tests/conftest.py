import pytest
from taransaydb import Device, FloatDevice


@pytest.fixture
def test_device(tmp_path):
    """Empty device."""
    return Device(tmp_path, "test_device")


@pytest.fixture
def float_device(tmp_path):
    return FloatDevice(tmp_path, "test_device")
