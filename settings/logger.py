import logging
import sys


class StdoutFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno >= logging.WARNING:
            return False
        return True


class FormatterWithProcessName(logging.Formatter):
    def format(self, record):
        if not hasattr(record, 'proc'):
            record.proc = '----'
        return super().format(record)


def config_logger(config, logger_name) -> logging.Logger:
    _logger = logging.getLogger(logger_name)
    _logger.setLevel(config['DEFAULT']['log_level'])
    # noinspection SpellCheckingInspection
    formatter_with_proc_name = FormatterWithProcessName('%(asctime)-23s %(levelname)-8s %(proc)-8s %(message)s')
    stdout_filter = StdoutFilter()

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(30)
    stderr_handler.setFormatter(formatter_with_proc_name)
    _logger.addHandler(stderr_handler)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(config['DEFAULT']['log_level'])
    stdout_handler.setFormatter(formatter_with_proc_name)
    stdout_handler.addFilter(stdout_filter)
    _logger.addHandler(stdout_handler)

    return _logger
