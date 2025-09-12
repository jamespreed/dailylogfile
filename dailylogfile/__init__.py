import bz2
import datetime as dt
import logging
import re
import shutil
import sys
from pathlib import Path


__version__ = '0.2.1'


def _date_format_to_regex(date_format: str) -> str:
    """
    Converts a date format string to a regex pattern for matching dates in
    filenames.

    args:
        date_format: the date format string to convert.
    returns:
        str: the regex pattern.
    """
    replacements = {
        '%a': r'\w+',
        '%A': r'\w',
        '%w': r'\d',
        '%d': r'\d{2}',
        '%b': r'\w{3}',
        '%B': r'\w+',
        '%m': r'\d{2}',
        '%y': r'\d{2}',
        '%Y': r'\d{4}',
        '%H': r'\d{2}',
        '%I': r'\d{2}',
        '%p': r'AM|PM',
        '%M': r'\d{2}',
        '%S': r'\d{2}',
        '%f': r'\d{6}',
        '%z': r'\+|-?\d{4}',
        '%Z': r'\w+',
        '%j': r'\d{3}',
        '%U': r'\d{2}',
        '%W': r'\d{2}',
        '%G': r'\d{4}',
        '%u': r'\d',
        '%V': r'\d{2}',
    }
    percent = '%%'
    unsupported = ['%c', '%x', '%X']
    for us in unsupported:
        if us in date_format:
            raise ValueError(f'Unsupported date format: {us}')
    regex = date_format
    for k, v in replacements.items():
        regex = regex.replace(k, v)
    regex = regex.replace(percent, '%')
    return regex


class DailyLogFileHandler(logging.FileHandler):
    """
    A simple log file handler that rotates at midnight, adds dates to log names,
    compresses old logs, and ages off log files.
    """
    def __init__(
        self, 
        logfile: str|Path,
        date_format: str='%Y-%m-%d',
        date_sep: str='_',
        compress_after_days: int|None=5,
        max_history_days: int|None=30,
        mode: str='a',
        encoding: str|None=None,
        delay: bool=False,
        errors: str|None=None,
        file_permission: int = 0o640,
        ) -> None:
        """
        args:
            logfile: the path to the log file, a date will be inserted before
                the extension. If not extension is present, '.log' is added.
            date_format: the date format to add to the logfile name.
            date_sep: the separator to use between the logfile prefix and date.
            compress_after_days: after this many days old log files are 
                compressed with bz2, use None to disable.
            max_history_days: after this many days old bz2 log files are
                removed, use None to disable.
            mode: mode to use when opening logfile.
            encoding: text encoding to use when writing.
            delay: whether file opening is deferred until the first emit().
            errors: determines how encoding errors are handled.
            file_permission: permissions to set for the logs (default=0o640).
        """
        if compress_after_days and max_history_days:
            if (compress_after_days >= max_history_days):
                raise ValueError(
                    'compress_after_days must be less than max_history_days'
                )
        if Path(logfile).as_posix().lower().endswith('.bz2'):
            raise ValueError('logfile suffix cannot be .bz2')
        self.logfile = Path(logfile)
        self.date_format = date_format
        self.date_sep = date_sep
        self.compress_after_days = compress_after_days
        self.max_history_days = max_history_days
        self.file_permission = file_permission
        self._current_day = dt.date.today()
        self._logfile_prefix = self.logfile.with_suffix('')
        self._logfile_suffix = self.logfile.suffix or '.log'
        self.logfile.parent.mkdir(exist_ok=True, parents=True)
        super().__init__(self._file_name(), mode, encoding, delay, errors)
        self._compress_old_logfiles()
        self._handle_ageoff()

    def _open(self):
        f = Path(self.baseFilename)
        f.touch(exist_ok=True)
        f.chmod(self.file_permission)
        return super()._open()

    def emit(self, record: logging.LogRecord) -> None:
        if self._needs_rollover():
            self._rollover()
        return super().emit(record)

    def _file_name(self) -> str:
        """
        Creates the file name based on the logfile prefix, date, and extension.
        """
        d = self._current_day.strftime(self.date_format)
        return f'{self._logfile_prefix}{self.date_sep}{d}{self._logfile_suffix}'

    def _compress_old_logfiles(self) -> None:
        """
        Applies bz2 compression to the older log files.
        """
        if not self.compress_after_days:
            return
        today = dt.date.today()
        glob_pattern = f'{self._logfile_prefix.name}*{self._logfile_suffix}'
        stem_pattern = (
            f'^{re.escape(self._logfile_prefix.name)}'
            f'{re.escape(self.date_sep)}'
            f'({_date_format_to_regex(self.date_format)})$'
        )
        for file in self.logfile.parent.glob(glob_pattern):
            if not (m:=re.match(stem_pattern, file.stem)):
                continue
            fdate = dt.datetime.strptime(m.group(1), self.date_format).date()
            if (today - fdate).days <= self.compress_after_days:
                continue
            outfile = file.with_suffix(f'{file.suffix}.bz2')
            with file.open('rb') as fpin, bz2.open(outfile, 'wb') as fpout:
                shutil.copyfileobj(fpin, fpout)
            outfile.chmod(self.file_permission)
            file.unlink()

    def _handle_ageoff(self) -> None:
        """
        Removes old log files that are passed the age-off limit.
        """
        if not self.max_history_days:
            return
        today = dt.date.today()
        suffix = (
            f'{self._logfile_suffix}.bz2' if self.compress_after_days 
            else self._logfile_suffix
        )
        glob_pattern = f'{self._logfile_prefix.name}*{suffix}'
        stem_pattern = (
            f'^{re.escape(self._logfile_prefix.name)}'
            f'{re.escape(self.date_sep)}'
            f'({_date_format_to_regex(self.date_format)})$'
        )
        for file in self.logfile.parent.glob(glob_pattern):
            if not (m:=re.match(stem_pattern, file.name.removesuffix(suffix))):
                continue
            fdate = dt.datetime.strptime(m.group(1), self.date_format).date()
            if (today - fdate).days > self.max_history_days:
                file.unlink()
    
    def _needs_rollover(self) -> bool:
        """
        Checks if a rollover is needed.
        """
        new_day = dt.date.today()
        return new_day != self._current_day

    def _rollover(self) -> None:
        """
        Handles rollover of log files at midnight when a script is running.
        """ 
        self._current_day = dt.date.today()
        if self.stream:
            self.stream.close()
        self.baseFilename = self._file_name()
        self.stream = self._open()
        self._compress_old_logfiles()
        self._handle_ageoff()


def setup_daily_logger(
    logfile: str|Path|None,
    date_format: str='%Y-%m-%d',
    date_sep: str='_',
    compress_after_days: int|None=5,
    max_history_days: int|None=30,
    logger_name: str|None = None,
    logger_level: int=logging.INFO,
    logger_format: str='[%(asctime)s] %(levelname)s - %(message)s',
    logger_date_format: str='%Y-%m-%d %H:%M:%S',
    mode: str='a',
    encoding: str|None=None,
    delay: bool=False,
    errors: str|None=None,
    file_permission: int = 0o640,
    ) -> logging.Logger:
    """
    Sets up a daily logger using the supplied arguments.

    args:
        logfile: log file path to pass to the DailyLogFileHanlder, passing None
            logs to stdout.
        date_format: the date format to add to the logfile name.
        date_sep: the separator to use between the logfile prefix and date.
        compress_after_days: after this many days old log files are compressed
            with bz2, use None to disable.
        max_history_days: after this many days old bz2 log files are removed,
            use None to disable.
        logger_name: name of the logger, None uses the name of the log file.
        logger_level: log level to set for the logger.
        logger_format: log format to use when writting.
        logger_date_format: date format to use in the log messages.
        mode: mode to use when opening logfile.
        encoding: text encoding to use when writing.
        delay: whether file opening is deferred until the first emit().
        errors: determines how encoding errors are handled.
        file_permission: permissions to set for the logs (default=0o640).
    returns:
        logging.Logger
    """
    if (logfile is None) or (not logfile):
        logger_name = Path(__file__).stem
        handler = logging.StreamHandler(sys.stdout)
    else:
        logger_name = logger_name or Path(logfile).stem
        handler = DailyLogFileHandler(
            logfile=logfile,
            date_format=date_format,
            date_sep=date_sep,
            compress_after_days=compress_after_days,
            max_history_days=max_history_days,
            mode=mode,
            encoding=encoding,
            delay=delay,
            errors=errors,
            file_permission=file_permission,
        )
    logger = logging.getLogger(logger_name)
    formatter = logging.Formatter(fmt=logger_format, datefmt=logger_date_format)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logger_level)
    return logger
