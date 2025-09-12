# dailylogfile
Python package for creating and managing daily log files that roll over at midnight.

This is an opinionated logger that handles the following:
* makes one log file per day with the date added to the log file name.
* rolls over log files at midnight.
* optionally handles bz2 compressing older log files.
* optionally handles aging off of really old log files.

## Installation
Install from pypi via:
```sh
pip install dailylogfile
```

## Usage
The most basic usage is to import `setup_daily_logger` and use the arguments to configure the logger.
```python
from dailylogfile import setup_daily_logger

logger = setup_daily_logger('/data/logs/MyProject.log')
logger.info('Logger initialized')
```

The following arguments are supported by `setup_daily_logger` and are passed to the `DailyLogFileHandler` class initiator:
* `logfile`: log file path to pass to the DailyLogFileHanlder, passing `None` logs to stdout.
* `date_format`: the date format to add to the logfile name (default = `'%Y-%m-%d'`).
* `date_sep`: the separator to use between the logfile prefix and date (default = `'_'`).
* `compress_after_days`: files older than this many days are bz2 compressed, use `None` to disable (default = `2`).
* `max_history_days`: files older than this many days are removed, use `None` to disable (default = `30`).
* `logger_name`: name of the logger, `None` uses the stem of the log file as the logger name (default = ` None`).
* `logger_level`: log level to set for the logger (default = `logging.INFO`).
* `logger_format`: log format to use when writting (default = `'[%(asctime)s] %(levelname)s - %(message)s'`).
* `logger_date_format`: date format to use in the log messages (default = `'%Y-%m-%d %H:%M:%S'`).
* `mode`: mode to use when opening logfile (default = `'a'`).
* `encoding`: text encoding to use when writing (default = `None`).
* `delay`: whether file opening is deferred until the first `emit()` (default = `False`).
* `errors`: determines how encoding errors are handled (default = `None`).
* `file_permission`: permissions to set for the logs (default=0o640).

## Details
This is how the `DailyLogFileHandler` handles files under the hood.

### File name templating
The logfile names are generated using a template.
#### `logfile`
The `logfile` argument NOT where the log file will be written.  It is parsed and then included in the tamplate when creating the log file name. 
If a file extension is present, it will be used, otherwise `'.log'` is used.  The log file names are formatted something like: `/data/logs/MyProject_2025-08-22.log`. 

#### `date_format`
The format of the date appended to the logfile name is controlled using `date_format`.  See the [Python docs on time code formats](https://docs.python.org/3.13/library/datetime.html#strftime-and-strptime-format-codes) for my information.  Note: the current `date` is used when generating logfile names, not the `datetime`, i.e. hours, minutes, and seconds are all zeros.

#### `date_sep`
This is the string used to join the logfile name with the date string. Avoid using `%` here as it could cause the parsing for date strings to fail.

### File rollover
If a script is running the logger or when logger is instantiated, it will check if a rollover needs to be run.  During rollover, the logger will look for files in the log directory that match
the log file naming scheme and optionally compress and/or age-off log files.

#### `compress_after_days`
During rollover, matching files older than this number of days will be compressed using `bz2` compression.  If `compress_after_days` is None or zero, this is disabled.

#### `max_history_days`
During rollover, matching files older than this number of days will be removed. If `max_history_days` is None or zero, this is disabled.

## Detailed Example
This is an example with all options explained:

```python
from dailylogfile import setup_daily_logger

logger = setup_daily_logger(
    logfile='/data/logs/MyProject.zzz,
    date_format='%Y-%m-%d',
    date_sep='___',
    compress_after_days=1,
    max_history_days=4,
    logger_name = None,
    logger_level=logging.INFO,
    logger_format='[%(asctime)s] %(levelname)s - %(message)s',
    logger_date_format='%Y-%m-%d %H:%M:%S',
    mode='a',
    encoding=None,
    delay=False,
    errors=None,
    file_permission=0o640,
)
```

The arguments `logfile='/data/logs/MyProject.zzz`, `date_format='%Y-%m-%d'`, and `date_sep='___'` will set the log name template to be `/data/logs/MyProject___YYYY-MM-DD.zzz`, e.g. the log files would be: 
```
/data/logs/MyProject___2025-08-23.zzz
/data/logs/MyProject___2025-08-24.zzz
```

The next arguments: `compress_after_days=1` and `max_history_days=4` will cause log files older than 1 day to compress and older than 4 days to be deleted. On 2025-08-25, the log files would be:
```
# 2025-08-25 log files:
/data/logs/MyProject___2025-08-23.zzz.bz2
/data/logs/MyProject___2025-08-24.zzz            # 1 day old
/data/logs/MyProject___2025-08-25.zzz            # today, 0 days old
```

And on 2025-08-27 the previously `.zzz` log files have been compressed:
```
# 2025-08-27 log files:
/data/logs/MyProject___2025-08-23.zzz.bz2
/data/logs/MyProject___2025-08-24.zzz.bz2
/data/logs/MyProject___2025-08-25.zzz.bz2
/data/logs/MyProject___2025-08-26.zzz
/data/logs/MyProject___2025-08-27.zzz
```

When the date moves from 2025-08-27 to 2025-08-28, the oldest log file will be aged off:
```
# 2025-08-28 log files:
/data/logs/MyProject___2025-08-24.zzz.bz2
/data/logs/MyProject___2025-08-25.zzz.bz2
/data/logs/MyProject___2025-08-26.zzz.bz2
/data/logs/MyProject___2025-08-27.zzz
/data/logs/MyProject___2025-08-28.zzz
```

The argument `logger_name = None` will use the logfile name as the logger name, setting it to `'MyProject'`.  This allows you to get the logger via:
```python
import logging
logging.getLogger('MyProject')
```

The next arguments: `logger_level=logging.INFO`, `logger_format='[%(asctime)s] %(levelname)s - %(message)s'`, and `logger_date_format='%Y-%m-%d %H:%M:%S'` set the logger level for this logger to INFO, set the logger format, and datetime format for `%(asctime)s`.

The arguments: `mode='a'`, `encoding=None`, `delay=False`, and `errors=None` are the passed to the super class `logging.FileHandler`.  They control the file mode, file encoding, delayed file creation, and how encoding errors are handled.

The final argument is used to set file permissions when creating adding the log file.