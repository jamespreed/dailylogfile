import datetime as dt
from freezegun import freeze_time
from pathlib import Path
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


@freeze_time(dt.datetime(2025, 8, 10, 12, 12, 12))
def test_init_no_compress_no_ageoff():
    setup_log_files()
    logger = setup_daily_logger(
        logfile=LOG_DIR / LOG_NAME,
        compress_after_days=None,
        max_history_days=None
    )
    for handler in logger.handlers:
        logger.removeHandler(handler)
        handler.close()
    expected_files = [
        f"{LOG_NAME}_2025-08-05.log",
        f"{LOG_NAME}_2025-08-06.log",
        f"{LOG_NAME}_2025-08-07.log",
        f"{LOG_NAME}_2025-08-08.log",
        f"{LOG_NAME}_2025-08-09.log",
        f"{LOG_NAME}_2025-08-10.log",
    ]
    files = sorted(f.name for f in LOG_DIR.glob('*'))
    for expected, actual in zip(expected_files, files):
        assert expected == actual
    teardown_log_dir()


@freeze_time(dt.datetime(2025, 8, 10, 12, 12, 12))
def test_init_compress_no_ageoff():
    setup_log_files()
    logger = setup_daily_logger(
        logfile=Path(LOG_DIR) / LOG_NAME,
        compress_after_days=2,
        max_history_days=None
    )
    for handler in logger.handlers:
        logger.removeHandler(handler)
        handler.close()

    expected_files = [
        f"{LOG_NAME}_2025-08-05.log.bz2",
        f"{LOG_NAME}_2025-08-06.log.bz2",
        f"{LOG_NAME}_2025-08-07.log.bz2",
        f"{LOG_NAME}_2025-08-08.log",
        f"{LOG_NAME}_2025-08-09.log",
        f"{LOG_NAME}_2025-08-10.log",
    ]
    files = sorted(f.name for f in LOG_DIR.glob('*'))
    for expected, actual in zip(expected_files, files):
        assert expected == actual
    teardown_log_dir()


@freeze_time(dt.datetime(2025, 8, 10, 12, 12, 12))
def test_init_compress_ageoff():
    setup_log_files()
    logger = setup_daily_logger(
        logfile=Path(LOG_DIR) / LOG_NAME,
        compress_after_days=2,
        max_history_days=4,
    )
    for handler in logger.handlers:
        logger.removeHandler(handler)
        handler.close()

    expected_files = [
        f"{LOG_NAME}_2025-08-06.log.bz2",
        f"{LOG_NAME}_2025-08-07.log.bz2",
        f"{LOG_NAME}_2025-08-08.log",
        f"{LOG_NAME}_2025-08-09.log",
        f"{LOG_NAME}_2025-08-10.log",
    ]
    files = sorted(f.name for f in LOG_DIR.glob('*'))
    for expected, actual in zip(expected_files, files):
        assert expected == actual
    teardown_log_dir()


def test_rollover():
    with freeze_time(dt.datetime(2025, 8, 10, 12, 12, 12)):
        log_files_and_dates = setup_log_files()
        logger = setup_daily_logger(
            logfile=Path(LOG_DIR) / LOG_NAME,
            compress_after_days=2,
            max_history_days=4,
            logger_format='%(message)s',
        )
        logger.info('PRE-ROLLOVER-MESSAGE')
    with freeze_time(dt.datetime(2025, 8, 11, 0, 1, 1)):
        logger.info('POST-ROLLOVER-MESSAGE')

    for handler in logger.handlers:
        logger.removeHandler(handler)
        handler.close()

    expected_files = [
        f"{LOG_NAME}_2025-08-07.log.bz2",
        f"{LOG_NAME}_2025-08-08.log.bz2",
        f"{LOG_NAME}_2025-08-09.log",
        f"{LOG_NAME}_2025-08-10.log",
        f"{LOG_NAME}_2025-08-11.log",
    ]
    files = sorted(f.name for f in LOG_DIR.glob('*'))
    for expected, actual in zip(expected_files, files):
        assert expected == actual
    teardown_log_dir()
