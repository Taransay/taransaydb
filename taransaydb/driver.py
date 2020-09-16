"""Database driver."""

import os
from enum import Flag, auto
from pathlib import Path
from tempfile import NamedTemporaryFile
from heapq import merge
from functools import wraps
from datetime import datetime, time, timedelta
from collections.abc import Iterable, Reversible
from .exceptions import ProgrammingError


def requires_access_type(access_type):
    """Check that the driver is opened in correct access mode before executing wrapped method."""

    def check_required_access_type(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            # Throw an error if the driver is not opened in the correct mode...
            if access_type not in self.access_type:
                raise ProgrammingError(
                    f"{self} is not opened in a way that supports {access_type.name}."
                )

            # ...otherwise call the intended function.
            return func(self, *args, **kwargs)

        return wrapper

    return check_required_access_type


class DriverAccessType(Flag):
    """Flag determining the file mode for a driver."""

    APPEND = auto()
    READ = auto()
    _OVERWRITE = auto()
    WRITE = APPEND | _OVERWRITE

    @classmethod
    def file_mode(cls, flag):
        """Map the specified flag to a Python file mode."""
        access_map = {
            cls.APPEND: "a",
            cls.READ: "r",
            # Note: write mode is not used anywhere... yet.
        }

        return access_map[flag]


class DirectoryDriver:
    """Directory-based database driver."""

    def __init__(self, path, access_type, encoding, format_fnc, parse_fnc):
        self._path = Path(path)
        self.access_type = access_type
        self.encoding = encoding
        self._format_data = format_fnc
        self._parse_data = parse_fnc
        self._file_cache = {}
        self._is_open = False

    @property
    def path(self):
        """Top level directory for this driver instance.

        Returns
        -------
        :py:class:`pathlib.Path`
            The path.
        """
        return self._path

    def open(self):
        """Open the database."""
        self._is_open = True

    def close(self):
        """Close the database.

        This closes any open file objects.
        """
        for shard in self._file_cache.values():
            shard.close()

        self._is_open = False

    def flush(self):
        """Perform any pending write operations on open file objects."""
        for shard in self._file_cache.values():
            shard.flush()

    @requires_access_type(DriverAccessType.READ)
    def query_interval(self, start, stop):
        """Query data between start and stop.

        The data returned lies in the half-open interval [`start`, `stop`). This means that
        exactly (stop - start) / interval rows will be returned for data lying between `start` and
        `stop` and spaced in `interval` intervals.

        Queries with the same start and stop values always return an empty result, even if a data
        point lies exactly at that time.

        Returns
        -------
        :class:`.Cursor`
            The result cursor from which to read the query results.
        """
        return Cursor.from_range(self, start, stop)

    @requires_access_type(DriverAccessType.APPEND)
    def append(self, tick, data):
        """Append data to the end of the corresponding day file.

        Parameters
        ----------
        tick : :py:class:`datetime.datetime`
            The time the data was recorded.
        data : sequence
            A sequence of data to store.
        """
        shard_path = self._shard_path(tick.date())
        fp = self._shard_stream(shard_path, DriverAccessType.APPEND, create=True)
        fp.write(self._format_line(tick.time(), data))

    @requires_access_type(DriverAccessType.WRITE)
    def insert(self, tick, data):
        """Insert data in order.

        This assumes the data is already ordered.

        If you know your `tick` is later than the last reading in the corresponding day file, then
        you should use the much quicker :meth:`~DirectoryDriver.append` method.

        Parameters
        ----------
        tick : :py:class:`datetime.datetime`
            The time the data was recorded.
        data : sequence
            A sequence of data to store.
        """
        tick_date = tick.date()
        shard_path = self._shard_path(tick_date)

        fp_existing, fp_temp = self._shard_stream_with_tmp_buffer(
            shard_path,
            DriverAccessType.READ,  # Only read because we use a temporary write buffer instead.
            create=True,
        )

        # Ensure buffered data is written.
        fp_existing.flush()

        pivot_time = tick.time()
        pivot_passed = False
        insert_line = self._format_line(pivot_time, data)

        for line in self._read_lines(fp_existing):
            if pivot_passed:
                # Just copy the line directly.
                fp_temp.file.write(line + "\n")
                continue

            line_time, _ = self._parse_line_time(line)

            if line_time > pivot_time:
                # The insert data should be inserted before this line.
                fp_temp.file.write(insert_line)
                pivot_passed = True

            # Copy the existing line into the new file.
            fp_temp.file.write(line + "\n")

        if not pivot_passed:
            # The insert line is at the end.
            fp_temp.file.write(insert_line)

        # Substitute the shard with the temporary buffer.
        self._shard_replace(fp_existing, fp_temp)

    @requires_access_type(DriverAccessType.WRITE)
    def sort(self):
        """Sort every file in the database in ascending order of time."""
        for shard_path in self._shard_paths():
            self._sort_shard(shard_path)

    def _sort_shard(self, shard_path):
        """Sort day file.

        This works best for almost-sorted files. The algorithm is essentially a memory-efficient
        heapsort.

        Like all heapsorts, this is not stable. Measurements made at identical times may be swapped
        in the sorted file.
        """
        # Open a temporary file to use for the sorted result.
        fp_existing, fp_temp = self._shard_stream_with_tmp_buffer(
            shard_path,
            DriverAccessType.READ,  # Only read because we use a temporary write buffer instead.
            create=False,
        )

        # Ensure buffered data is written.
        fp_existing.flush()

        def subsort_file(fp):
            """Sort a file using heapsort.

            Returns a list of sorted, open temporary file pointers.
            """
            # Open two files: one for sorted lines, one for unsorted. The sorted file is not deleted
            # on close, but rather deleted later when we merge it into the main file. The unsorted
            # file is deleted at the end of this method.
            with NamedTemporaryFile(
                mode="r+",
                prefix=f"{fp_existing.name}_sorted",
                dir=str(shard_path.parent),
                delete=False,
                encoding=self.encoding,
            ) as sub_fp_sorted, NamedTemporaryFile(
                mode="r+",
                prefix=f"{fp_existing.name}_unsorted",
                dir=str(shard_path.parent),
                delete=True,
                encoding=self.encoding,
            ) as sub_fp_unsorted:
                last_time = None
                has_unsorted = False

                for line in self._read_lines(fp):
                    line_time, line_data = self._parse_line_time(line)

                    # Choose whether to write this line into the sorted or unsorted file.
                    if last_time is None or line_time > last_time:
                        target = sub_fp_sorted
                        # Update the last sorted time.
                        last_time = line_time
                    else:
                        target = sub_fp_unsorted
                        has_unsorted = True

                    target.write(self._format_line(line_time, line_data))

                sorted_paths = [Path(sub_fp_sorted.name)]

                if has_unsorted:
                    # Sort the unsorted heap.
                    sorted_paths.extend(subsort_file(sub_fp_unsorted))

                return sorted_paths

        heap_paths = subsort_file(fp_existing)
        # Open the heap files for reading.
        heap_files = [
            heap_path.open(encoding=self.encoding) for heap_path in heap_paths
        ]
        # Merge the sorted subfiles and write to buffer.
        merged_lines = merge(*heap_files, key=self._parse_line_time)
        fp_temp.writelines(merged_lines)

        # Delete the temporary files.
        for heap_file, heap_path in zip(heap_files, heap_paths):
            heap_file.close()
            heap_path.unlink()

        # Overwrite the unsorted shard with the buffer.
        self._shard_replace(fp_existing, fp_temp)

    def _format_line(self, tick_time, data):
        return " ".join([str(tick_time)] + self._format_data(data)) + "\n"

    def _shard_path(self, date):
        return (
            self.path / f"{date.year:04d}" / f"{date.month:02d}" / f"{date.day:02d}.txt"
        )

    def _shard_paths(self):
        return self.path.glob("**/*.txt")

    @staticmethod
    def _read_lines(fp, reverse=False, buf_size=8192):
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
                if line:  # Ignores empty lines.
                    yield line

        if remainder:  # Ignores empty last line.
            yield remainder

    def _parse_lines(self, shard_date, start, stop, reverse=False):
        shard_path = self._shard_path(shard_date)

        try:
            fp = self._shard_stream(shard_path, DriverAccessType.READ)
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
                yield [line_datetime, self._parse_data(line_data)]

    @staticmethod
    def _parse_line_time(line):
        """Parse line time and return it along with the raw line data."""
        pieces = line.split()
        assert len(pieces) >= 2
        return time.fromisoformat(pieces[0]), pieces[1:]

    def _shard_stream(self, shard_path, mode=DriverAccessType.READ, create=False):
        if not self._is_open:
            raise ProgrammingError(
                f"{self} is not open. Data can only be queried when the driver is open and in read "
                f"mode."
            )

        file_mode = DriverAccessType.file_mode(mode)

        if shard_path in self._file_cache:
            if self._file_cache[shard_path].mode == file_mode:
                # We're done.
                return self._file_cache[shard_path]

            # Not the correct mode. Close and reopen.
            self._file_cache[shard_path].close()
        elif create and not shard_path.is_file():
            shard_path.parent.mkdir(exist_ok=True, parents=True)
            shard_path.touch(exist_ok=False)

        self._file_cache[shard_path] = shard_path.open(
            file_mode, encoding=self.encoding
        )

        return self._file_cache[shard_path]

    def _shard_stream_with_tmp_buffer(self, shard_path, *args, **kwargs):
        """Get two shard streams for a given date: the real one, and a temporary buffer.

        The real shard stream is the same one returned by :class:`._shard_stream`. The temporary one
        has the same path and filename except with ".tmp" appended. The temporary buffer is
        write-only, and it is up to the user to delete the file if/when needed.
        """
        fp_real = self._shard_stream(shard_path, *args, **kwargs)
        fp_temp = NamedTemporaryFile(
            mode="w", prefix=shard_path.name, dir=str(shard_path.parent), delete=False
        )

        return fp_real, fp_temp

    def _shard_replace(self, fp_cached, fp_replacement):
        """Rename the file `fp_replacement` represents to the path that `fp_cached` represents.

        The renamed file is cached and reopened in the same mode as the file that `fp_cached`
        represents. Both file pointers are closed by this method.
        """
        cached_path = Path(fp_cached.name)

        assert cached_path in self._file_cache
        assert not fp_cached.closed
        assert cached_path == Path(fp_cached.name)

        replacement_path = Path(fp_replacement.name)

        # Close the files.
        fp_cached.close()
        fp_replacement.close()

        # Both files should still exist.
        assert cached_path.is_file()
        assert replacement_path.is_file()

        # Rename.
        replacement_path.rename(cached_path)

        # Re-open with the original mode.
        self._file_cache[cached_path] = cached_path.open(
            fp_cached.mode, encoding=self.encoding
        )

    def __str__(self):
        return f"{self.__class__.__name__}(access_type={self.access_type})"


class Cursor(Reversible, Iterable):
    """Query result cursor."""

    def __init__(self, driver):
        self._driver = driver
        self.query_intervals = {}

    @classmethod
    def from_range(cls, driver, start, stop):
        """Build a Cursor from a driver and datetime interval."""
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
