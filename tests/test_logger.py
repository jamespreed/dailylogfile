import datetime as dt
import freezegun
import pytest
from pathlib import Path
from time import sleep
from typing import Generator
from dailylogfile import setup_daily_logger

LOG_NAME = "TESTLOG"
LOG_DIR = Path("../test_logs")


def setup_log_files() -> list[tuple[Path, dt.date]]:
    LOG_DIR.mkdir(exist_ok=True)
    today = dt.date.today()
    log_files_and_dates = []
    for i in range(6):
        log_date = today - dt.timedelta(days=i)
        log_file = LOG_DIR / f"{LOG_NAME}_{log_date.strftime('%Y-%m-%d')}.log"
        with log_file.open("w") as fp:
            fp.write(f"Log file for {log_date.strftime('%Y-%m-%d')}\n")
        log_files_and_dates.append((log_file, log_date))
    return log_files_and_dates


def teardown_log_dir() -> None:
    for file in LOG_DIR.glob("*"):
        file.unlink()
    LOG_DIR.rmdir()


def test_init_no_compress_no_ageoff():
    log_files_and_dates = setup_log_files()
    logger = setup_daily_logger(
        logfile=LOG_DIR / LOG_NAME,
        compress_after_days=None,
        max_history_days=None
    )
    for handler in logger.handlers:
        logger.removeHandler(handler)
        handler.close()
    for log_path, _ in log_files_and_dates:
        assert log_path.is_file()
    teardown_log_dir()


def test_init_compress_no_ageoff():
    log_files_and_dates = setup_log_files()
    logger = setup_daily_logger(
        logfile=Path(LOG_DIR) / LOG_NAME,
        compress_after_days=2,
        max_history_days=None
    )
    for handler in logger.handlers:
        logger.removeHandler(handler)
        handler.close()
    for _, log_date in log_files_and_dates:
        expected_name = f"{LOG_NAME}_{log_date.strftime('%Y-%m-%d')}.log"
        if (dt.date.today() - log_date).days > 2:
            expected_name += ".bz2"
        assert Path(LOG_DIR / expected_name).exists()
    teardown_log_dir()


def test_init_compress_ageoff():
    log_files_and_dates = setup_log_files()
    logger = setup_daily_logger(
        logfile=Path(LOG_DIR) / LOG_NAME,
        compress_after_days=2,
        max_history_days=4,
    )
    for _, log_date in log_files_and_dates:
        expected_name = f"{LOG_NAME}_{log_date.strftime('%Y-%m-%d')}.log"
        if (dt.date.today() - log_date).days > 2:
            expected_name += ".bz2"
        if (dt.date.today() - log_date).days > 2 and (dt.date.today() - log_date).days <= 4:
            assert Path(LOG_DIR / expected_name).exists()
        if (dt.date.today() - log_date).days > 4:
            assert not Path(LOG_DIR / expected_name).exists()
    for handler in logger.handlers:
        logger.removeHandler(handler)
        handler.close()
    teardown_log_dir()


def test_rollover():
    log_files_and_dates = setup_log_files()
    logger = setup_daily_logger(
        logfile=Path(LOG_DIR) / LOG_NAME,
        compress_after_days=2,
        max_history_days=4,
        logger_format='%(message)s',
    )
    
    logger.info('PRE-ROLLOVER-MESSAGE')
    with freezegun.freeze_time(dt.date.today() + dt.timedelta(days=1)):
        logger.info('POST-ROLLOVER-MESSAGE')
    new_log_name = f"{LOG_NAME}_{(dt.date.today()).strftime('%Y-%m-%d')}.log"
    new_log_path = Path(LOG_DIR / new_log_name)
    assert new_log_path.exists()
    for _, log_date in log_files_and_dates:
        expected_name = f"{LOG_NAME}_{log_date.strftime('%Y-%m-%d')}.log"
        if (dt.date.today() - log_date).days > 2:
            expected_name += ".bz2"
        if (dt.date.today() - log_date).days > 4:
            assert not Path(LOG_DIR / expected_name).exists()
    for handler in logger.handlers:
        logger.removeHandler(handler)
        handler.close()
    teardown_log_dir()
