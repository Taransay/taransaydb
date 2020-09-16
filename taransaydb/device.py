"""Devices."""

from pathlib import Path
from contextlib import contextmanager
from .driver import DirectoryDriver, DriverAccessType


class Device:
    """A wrapper for a named collection of related data within a database."""

    def __init__(self, database, name, encoding="UTF-8"):
        self.database = Path(database)
        self.name = str(name)
        self.encoding = encoding

    @property
    def path(self):
        """Top level directory containing this device's data.

        Returns
        -------
        :py:class:`pathlib.Path`
            The device's path.
        """
        return self.database / self.name

    @contextmanager
    def reader(self):
        """Open the device driver in read mode."""
        return self._yield_driver_with_mode(DriverAccessType.READ)

    @contextmanager
    def writer(self):
        """Open the device driver in write mode."""
        return self._yield_driver_with_mode(DriverAccessType.WRITE)

    def sort(self):
        """Sort the device's data in ascending order of time."""
        with self.writer() as driver:
            driver.sort()

    def format_data(self, data):
        """Convert the supplied list items to strings."""
        return [str(value) for value in data]

    def parse_data(self, data):
        """No-op pass-through of the supplied data."""
        return data

    def __str__(self):
        return self.name

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"

    def _yield_driver_with_mode(self, mode):
        ctx_driver = DirectoryDriver(
            self.path, mode, self.encoding, self.format_data, self.parse_data
        )
        ctx_driver.open()
        yield ctx_driver
        ctx_driver.close()


class FloatDevice(Device):
    """Device for storing floats.

    The encoding used is iso-8859-1 (latin-1), which due to only supporting ASCII characters
    provides very fast (sometimes orders of magnitude faster than unicode) read performance.
    """

    def __init__(self, database, name):
        super().__init__(database, name, encoding="iso-8859-1")

    def parse_data(self, data):
        """Parse the specified data as floats."""
        return [float(value) for value in data]
