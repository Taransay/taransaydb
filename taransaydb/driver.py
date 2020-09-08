"""Database driver."""

import os
from enum import Flag, auto
from pathlib import Path
from shutil import copyfile
from tempfile import NamedTemporaryFile
from datetime import datetime, time, timedelta
from collections.abc import Iterable, Reversible
from .exceptions import ProgrammingError


class DriverAccessType(Flag):
    APPEND = auto()
    READ = auto()
    _WRITE_FULL = auto()
    WRITE = APPEND | _WRITE_FULL

    @classmethod
    def file_mode(cls, flag):
        access_map = {
            cls.APPEND: "a",
            cls.READ: "r",
            # Note: write mode is not used anywhere... yet.
        }

        return access_map[flag]


class DirectoryDriver:
    def __init__(self, path, access_type, format_fnc, parse_fnc):
        self._path = Path(path)
        self.access_type = access_type
        self._format_data = format_fnc
        self._parse_data = parse_fnc
        self._file_cache = {}
        self._is_open = False

    def open(self):
        self._is_open = True

    def close(self):
        for shard in self._file_cache.values():
            shard.close()

        self._is_open = False

    def flush(self):
        for shard in self._file_cache.values():
            shard.flush()

    def query_interval(self, start, stop):
        """Query data between start and stop.

        The data returned lies in the half-open interval [`start`, `stop`). This means that
        exactly (stop - start) / interval rows will be returned for data lying between `start` and
        `stop` and spaced in `interval` intervals.

        Queries with the same start and stop values always return an empty result, even if a data
        point lies exactly at that time.
        """
        if DriverAccessType.READ not in self.access_type:
            # Incorrect access type for this driver.
            raise ProgrammingError(
                f"{self} is not opened in a way that supports reading."
            )

        return Cursor.from_range(self, start, stop)

    def append(self, tick, data):
        if DriverAccessType.APPEND not in self.access_type:
            # Incorrect access type for this driver.
            raise ProgrammingError(
                f"{self} is not opened in a way that supports appending."
            )

        fp = self._shard_stream(tick.date(), DriverAccessType.APPEND, create=True)
        fp.write(self._format_line(tick, data))

    def insert(self, tick, data):
        """Insert data in order.

        This assumes the data is already ordered.

        If you know your `tick` is later than the last reading in the database, then
        :class:`.append` is much quicker.
        """
        if DriverAccessType.WRITE not in self.access_type:
            # Incorrect access type for this driver.
            raise ProgrammingError(
                f"{self} is not opened in a way that supports writing."
            )

        tick_date = tick.date()

        fp_existing, fp_temp = self._shard_stream_with_tmp_buffer(
            tick_date,
            DriverAccessType.READ,  # Only read because we use a temporary write buffer instead.
            create=True,
        )

        # Ensure buffered data is written.
        fp_existing.flush()

        insert_line = self._format_line(tick, data)
        pivot_time = tick.time()
        pivot_passed = False

        for line in self._read_lines(fp_existing):
            line_time, line_data = self._parse_line_time(line)

            if line_time > pivot_time:
                # The insert data should be inserted before this line.
                fp_temp.file.write(insert_line)
                pivot_passed = True

            # Copy the existing line into the new file.
            fp_temp.file.write(line + "\n")

            if pivot_passed:
                # Quickly write the rest of the chunks.
                while True:
                    data = fp_existing.read(1024)
                    if not data:
                        break
                    fp_temp.file.write(data)

                # We're done.
                break

        if not pivot_passed:
            # The insert line is at the end.
            fp_temp.file.write(insert_line)

        # Substitute the shard with the temporary buffer.
        self._shard_stream_replace_cache(
            fp_existing, fp_temp, self._shard_path(tick_date)
        )
        # Delete the temporary file.
        fp_temp.close()

    def _format_line(self, tick, data):
        return " ".join([str(tick.time())] + self._format_data(data)) + "\n"

    @property
    def path(self):
        return self._path

    def _shard_path(self, date):
        return (
            self.path / f"{date.year:04d}" / f"{date.month:02d}" / f"{date.day:02d}.txt"
        )

    def _read_raw_lines(self, fp, reverse=False, buf_size=8192):
        """Memory-efficient, reversible line reader.

        Based on flyingcircus.readline.
        """
        is_bytes = isinstance(fp.read(0), bytes)
        newline = b"\n" if is_bytes else "\n"
        remainder = b"" if is_bytes else ""

        def blocks(fp):
            fp.seek(0)

            while True:
                block = fp.read(buf_size)

                if not block:
                    break
                else:
                    yield block

        def reversed_blocks(fp):
            offset = fp.seek(0, os.SEEK_END)

            while offset > 0:
                block_size = min(offset, buf_size)
                offset -= block_size
                fp.seek(offset)
                block = fp.read(block_size)

                yield block

        block_generator = reversed_blocks if reverse else blocks

        for block in block_generator(fp):
            lines = block.split(newline)

            if remainder:
                if not reverse:
                    lines[0] = remainder + lines[0]
                else:
                    lines[-1] = lines[-1] + remainder

            remainder = lines[-1 if not reverse else 0]
            mask = slice(0, -1, 1) if not reverse else slice(-1, 0, -1)

            for line in lines[mask]:
                yield line

        yield remainder

    def _read_lines(self, *args, **kwargs):
        for line in self._read_raw_lines(*args, **kwargs):
            if not line or line.strip().startswith("#"):
                # Skip empty or comment line.
                continue

            yield line

    def _parse_lines(self, shard_date, start, stop, reverse=False):
        try:
            fp = self._shard_stream(shard_date, DriverAccessType.READ)
        except FileNotFoundError:
            # No file, so nothing to read.
            return

        for lineno, line in enumerate(self._read_lines(fp, reverse=reverse), start=1):
            try:
                line_time, line_data = self._parse_line_time(line)
            except ValueError as e:
                if reverse:
                    e.args = (f"{e} (line -{lineno} of {self})",)
                else:
                    e.args = (f"{e} (line {lineno} of {self})",)
                raise e

            if (reverse and line_time < start) or (not reverse and line_time >= stop):
                break

            if (reverse and line_time < stop) or (not reverse and line_time >= start):
                line_datetime = datetime.combine(shard_date, line_time)
                yield [line_datetime] + self._parse_data(line_data)

    def _parse_line_time(self, line):
        """Parse line time and return it along with the raw line data."""
        pieces = line.split()
        assert len(pieces) >= 2
        return time.fromisoformat(f"{pieces[0]}"), pieces[1:]

    def _shard_stream(self, shard_date, mode=DriverAccessType.READ, create=False):
        if not self._is_open:
            raise ProgrammingError(
                f"{self} is not open. Data can only be queried when the driver is open and in read "
                f"mode."
            )

        file_mode = DriverAccessType.file_mode(mode)
        shard_path = self._shard_path(shard_date)

        if shard_path in self._file_cache:
            if self._file_cache[shard_path].mode == file_mode:
                # We're done.
                return self._file_cache[shard_path]

            # Not the correct mode. Close and reopen.
            self._file_cache[shard_path].close()
        elif create and not shard_path.is_file():
            shard_path.parent.mkdir(exist_ok=True, parents=True)
            shard_path.touch(exist_ok=False)

        self._file_cache[shard_path] = shard_path.open(file_mode)

        return self._file_cache[shard_path]

    def _shard_stream_with_tmp_buffer(self, shard_date, *args, **kwargs):
        """Get two shard streams for a given date: the real one, and a temporary buffer.

        The real shard stream is the same one returned by :class:`._shard_stream`. The temporary one
        has the same path and filename except with ".tmp" appended. The temporary buffer is
        write-only. Once closed, the temporary file is deleted.
        """
        real_path = self._shard_path(shard_date)
        fp_real = self._shard_stream(shard_date, *args, **kwargs)
        fp_temp = NamedTemporaryFile(
            mode="w", prefix=real_path.name, dir=str(real_path.parent)
        )

        return fp_real, fp_temp

    def _shard_stream_replace_cache(self, fp_cached, fp_replacement, cached_path):
        """Overwrite `fp_cached` file object's file with `fp_replacement` file object's contents."""
        assert not fp_cached.closed
        assert cached_path == Path(fp_cached.name)

        fp_cached.close()

        # Finish writing data.
        fp_replacement.flush()

        # Overwrite the original file's contents with that of the temporary file.
        # This uses a memory-optimised copy operation starting from Python 3.8.
        copyfile(fp_replacement.name, fp_cached.name)

        # Re-open.
        self._file_cache[cached_path] = cached_path.open(fp_cached.mode)

    def __str__(self):
        return f"{self.__class__.__name__}(access_type={self.access_type})"


class Cursor(Reversible, Iterable):
    def __init__(self, driver):
        self._driver = driver
        self.query_intervals = {}

    @classmethod
    def from_range(cls, driver, start, stop):
        cursor = cls(driver)

        if start > stop:
            raise ValueError(f"start ({start}) cannot be > stop ({stop})")

        start_date = start.date()
        stop_date = stop.date()

        # Calculate the delta based on the dates, not datetimes, to avoid problems with end times
        # before start times on different dates.
        delta = stop_date - start_date

        for day_offset in range(delta.days + 1):
            shard_date = start_date + timedelta(days=day_offset)

            # Set the query's time span for the day.
            query_start = start.time() if start_date == shard_date else time.min
            query_stop = stop.time() if stop_date == shard_date else time.max

            cursor.query_intervals[shard_date] = query_start, query_stop

        return cursor

    def __iter__(self):
        for shard_date, (start, stop) in self.query_intervals.items():
            yield from self._driver._parse_lines(shard_date, start, stop)

    def __reversed__(self):
        for shard_date, (start, stop) in reversed(self.query_intervals.items()):
            yield from self._driver._parse_lines(shard_date, start, stop, reverse=True)
