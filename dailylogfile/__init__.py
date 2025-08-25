import bz2
import datetime as dt
import logging
import shutil
from pathlib import Path


class DailyLogFileHandler(logging.FileHandler):
    """
    A simple log file handler that rotates at midnight, adds dates to log names,
    compresses old logs, and ages off log files.
    """
    def __init__(
        self, 
        logfile: str | Path,
        date_format: str='%Y-%m-%d',
        compress_after_days: int | None=2,
        max_history_days: int | None=30,
        mode: str='a',
        encoding: str|None=None,
        delay: bool=False,
        errors: str|None=None,
        ) -> None:
        """
        args:
            logfile: the path to the log file, a date will be inserted before
                the extension. If not extension is present, '.log' is added.
            date_format: the date format to add to the logfile name.
            compress_after_days: after this many days old log files are 
                compressed with bz2, use None to disable.
            max_history_days: after this many days old bz2 log files are
                removed, use None to disable.
            mode: mode to use when opening logfile.
            encoding: text encoding to use when writing.
            delay: whether file opening is deferred until the first emit().
            errors: determines how encoding errors are handled.
        """
        self.logfile = Path(logfile)
        self.date_format = date_format
        self.compress_after_days = compress_after_days
        self.max_history_days = max_history_days
        self._current_day = dt.date.today()
        self._logfile_prefix = self.logfile.with_suffix('')
        self._logfile_suffix = self.logfile.suffix or '.log'
        super().__init__(self._file_name(), mode, encoding, delay, errors)
        self._compress_old_logfiles()
        self._handle_ageoff()

    def _file_name(self) -> str:
        """
        Creates the file name based on the logfile prefix, date, and extension.
        """
        d = self._current_day.strftime(self.date_format)
        return f'{self._logfile_prefix}_{d}{self._logfile_suffix}'

    def _compress_old_logfiles(self) -> None:
        """
        Applies bz2 compression to the older log files.
        """
        if not self.compress_after_days:
            return
        today = dt.date.today()
        glob_pattern = f'{self._logfile_prefix}*{self._logfile_suffix}'
        for file in self.logfile.parent.glob(glob_pattern):
            creation = dt.datetime.fromtimestamp(file.stat().st_ctime).date()
            if (today - creation).days <= self.compress_after_days:
                continue
            outfile = file.with_suffix(f'{file.suffix}.bz2')
            with file.open('rb') as fpin, bz2.open(outfile, 'wb') as fpout:
                shutil.copyfileobj(fpin, fpout)
            file.unlink()

    def _handle_ageoff(self) -> None:
        """
        Removes old log files that are passed the age-off limit.
        """
        if not self.max_history_days:
            return
        today = dt.date.today()
        if self.compress_after_days:
            glob_pattern = f'{self._logfile_prefix}*{self._logfile_suffix}.bz2'
        else:
            glob_pattern = f'{self._logfile_prefix}*{self._logfile_suffix}'
        for file in self.logfile.parent.glob(glob_pattern):
            creation = dt.datetime.fromtimestamp(file.stat().st_ctime).date()
            if (today - creation).days > self.max_history_days:
                file.unlink()
    
    def _rollover(self) -> None:
        """
        Handles rollover of log files at midnight when a script is running.
        """
        new_day = dt.date.today()
        if new_day == self._current_day:
            # don't rollover is this is called by mistake
            return
        self._current_day = new_day
        if self.stream:
            self.stream.close()
        self.baseFilename = self._file_name()
        self.stream = self._open()
        self._compress_old_logfiles()
        self._handle_ageoff()


def setup_daily_logging(
    logfile: str,
    date_format: str='%Y-%m-%d',
    compress_after_days: int=2,
    max_history_days: int=30,
    logger_name: str|None = None,
    logger_level: int=logging.INFO,
    logger_format: str='[%(asctime)s] %(levelname)s - %(message)s',
    ) -> logging.Logger:
    """
    Sets up a daily logger using the supplied arguments.

    args:
        logfile: log file path to pass to the DailyLogFileHanlder
        date_format: the date format to add to the logfile name.
        compress_after_days: after this many days old log files are compressed
            with bz2, use None to disable.
        max_history_days: after this many days old bz2 log files are removed,
            use None to disable.
        logger_name: name of the logger, None uses the name of the log file.
        logger_level: log level to set for the logger.
        logger_format: log format to use when writting.
    returns:
        logging.Logger
    """
    logger_name = logger_name or Path(logfile).stem
    logger = logging.getLogger(logger_name)
    handler = DailyLogFileHandler(
        logfile=logfile,
        date_format=date_format,
        compress_after_days=compress_after_days,
        max_history_days=max_history_days,
    )
    formatter = logging.Formatter(logger_format)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logger_level)
    return logger
