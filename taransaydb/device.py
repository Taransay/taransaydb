"""Devices."""

from pathlib import Path
from contextlib import contextmanager
from .driver import DirectoryDriver, DriverAccessType


class Device:
    def __init__(self, database, name):
        self.database = Path(database)
        self.name = str(name)

    @contextmanager
    def reader(self):
        return self._yield_driver_with_mode(DriverAccessType.READ)

    @contextmanager
    def writer(self):
        return self._yield_driver_with_mode(DriverAccessType.WRITE)

    def _yield_driver_with_mode(self, mode):
        ctx_driver = DirectoryDriver(self.path, mode, self.format_data, self.parse_data)
        ctx_driver.open()
        yield ctx_driver
        ctx_driver.close()

    def sort(self):
        with self.writer() as driver:
            driver.sort()

    def __str__(self):
        return self.name

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"

    @property
    def path(self):
        return self.database / self.name

    def format_data(self, data):
        return [str(value) for value in data]

    def parse_data(self, data):
        return data
