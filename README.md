# TaransayDB
Serverless, file-based, zero-configuration, zero-dependency, embeddable Python time series database.

*Note: TaransayDB is currently in alpha development and not yet suitable for use in other projects.*

TaransayDB is good at storing times and corresponding values such as sensor readings, GPS tracks or
system states. Its organisation is pretty much as simple as it gets: each database starts with a top
level directory on the file system with the name of the database. Each device you want to handle is
given its own subdirectory. The data for this device is then stored in the form `year / month /
day`, so e.g. the sensor data for "garden-shed" on 1st August 2020 is found at
`/path/to/database/garden-shed/2020/08/01.txt`. Within each day's file, each row contains one or
more readings at a given time.

## Quick example

...

## Strengths and weaknesses
TaransayDB is intentionally simple and relies on some common sense from the applications and
developers using it:

- *Fixed delimiters.* Whitespace is the column delimiter, newline is the row delimiter.
- *No special handling of timezones.* All dates are stored in ISO 8601 format. If you pass in
  dates that are timezone-aware, that's how they will be stored; similarly, if you query using
  timezone-aware dates, that's what will be used to compare against the times on each row of data.
  It's probably easiest if you convert everything to/from UTC.
- *No order checks on append.* If you append data then it's up to you to make sure the appended
  times are later than the latest at the end of the file. If your or your application can't
  guarantee this, you can still call a maintenance task later to fix misordering asynchronously.
- *No handling of metadata.* Your application is responsible for knowing which columns correspond to
  which readings, and each column's datatype.
- *No read/write locks other than what your file system provides.* Don't run multiple instances of
  TaransayDB writing to the same underlying directories unless you know what you're doing.

In return you get:

- *An easy dependency.* TaransayDB itself has no dependencies other than Python 3.7, and the licence
  is the permissive LGPL.
- *Small memory footprint.* All search and sort operations use iterators and generators to avoid
  having to load large files into memory, making TaransayDB suitable for use on embedded devices
  and for streaming over a network.
- *Human-readable data.* Want to quickly plot something with another programming language? Just load
  the relevant whitespace-separated files. Want to edit a data point? Just open up its file in your
  favourite text editor and change it. Want to generate a diff between two days? Just load the files
  with your favourite diff viewer. Want to archive the data for the future? You don't have to worry
  whether your chosen database technology will still be around later.
- *No separate database server to maintain.* The library works anywhere Python does. Just load
  TaransayDB as part of your Python application.
- *No separate database configuration.* The configuration comes from the application interfacing
  with TaransayDB.
- *The power of the file system.* Want backups? Just set up `rsync` or add the database path to your
  existing backup script (the data compress nicely, too). Want to host the data elsewhere? Just
  mount the database directory over the network.

Of course, if you need the power of SQL, concurrency, or high speed reads and writes, use something
like [SQLite](https://sqlite.org/) or [Redis](https://redis.io/). There are even databases designed
for timeseries, like [InfluxDB](https://www.influxdata.com/), albeit with more complex file formats.
If you're looking for lots of speed and features, you should probably look elsewhere!

## Driver contexts
TaransayDB handles file access via the database driver and opening modes exposed as context
managers. Reads and writes on the same context are forbidden because query results (from reads) are
not loaded from disk until accessed, which, if writes were allowed, could corrupt earlier results.
This also means that access to query results (e.g. converting to a list, or numpy array, or
whatever) needs to take place while the driver context is still open. On the one hand, this means
that in cases where you want to both read and write you have to close your first context and open a
second one, but on the other hand it allows the returned query results to be an iterator which is
memory efficient and reversible. Since the majority of probable use cases for TaransayDB will
involve separate reads and writes, this trade-off seems appropriate.
