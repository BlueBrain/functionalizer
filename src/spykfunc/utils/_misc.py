"""Miscelleneous utility functions."""

import logging as _logging
import sys

from .. import config


# -----------------------------------------------
# UI utils
# -----------------------------------------------
class ConsoleColors:
    """Helper class for coloring console output."""

    BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE, _, DEFAULT = range(10)
    NORMAL, BOLD, DIM, UNDERLINED, BLINK, INVERTED, HIDDEN = [a << 4 for a in range(7)]

    # These are the sequences need to get colored ouput
    _RESET_SEQ = "\033[0m"
    _CHANGE_SEQ = "\033[{}m"

    @classmethod
    def reset(cls):
        """Returns the color reset sequence."""
        return cls._RESET_SEQ

    @classmethod
    def set_text_color(cls, color):
        """Returns the appropriate color sequence for `color`."""
        return cls._CHANGE_SEQ.format(color + 30)

    @classmethod
    def format_text(cls, text, color, style=None):
        """Formats `text` with `color` and optionally `style`."""
        if color > 7:  # noqa: PLR2004
            style = color >> 4
            color = color & 0xF
        format_seq = "" if style is None else cls._CHANGE_SEQ.format(style)

        return format_seq + cls.set_text_color(color) + text + cls._RESET_SEQ


# ---
def format_cur_exception():
    """Convenience wrapper to format exceptions for logging."""
    import traceback

    if config.log_level == _logging.DEBUG:
        return traceback.format_exc()
    exc_type, exc_value, exc_traceback = sys.exc_info()
    exc_infos = traceback.format_exception(exc_type, exc_value, exc_traceback)
    return exc_infos[1] + "".join(exc_infos[-2:])


# -----------------------------------------------
# Logging
# -----------------------------------------------
class ColoredFormatter(_logging.Formatter):
    """Formatter for logging that colors output."""

    COLORS = {
        "WARNING": ConsoleColors.YELLOW,
        "INFO": ConsoleColors.DEFAULT + ConsoleColors.BOLD,
        "DEBUG": ConsoleColors.BLUE,
        "ERROR": ConsoleColors.RED,
        "CRITICAL": ConsoleColors.RED,
    }

    def format(self, record):
        """Formats the log entry `record`."""
        levelname = record.levelname
        msg = super().format(record)
        if levelname == "WARNING":
            msg = "[WARNING] " + msg
        if levelname in self.COLORS:
            msg = ConsoleColors.format_text(msg, self.COLORS[levelname])
        return msg


DefaultHandler = _logging.StreamHandler(sys.stdout)
DefaultHandler.setLevel(_logging.DEBUG)
DefaultHandler.setFormatter(ColoredFormatter("(%(asctime)s) %(message)s", "%H:%M:%S"))


class Singleton(type):
    """A simple singleton metaclass."""

    _instances = {}

    def __call__(cls, *args, **kwargs):
        """Returns a saved instance or creates a new one."""
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class Enforcer(_logging.Handler, metaclass=Singleton):
    """Enforces the occurence of no message for a chosen log level.

    If a message is emitted, this handler will raise an exception when the
    `check()` method is called.
    """

    def __init__(self, level=_logging.WARNING):
        """Creates a new handler.

        Args:
            level: the logging level at which to trigger
        """
        super().__init__(level)
        self._count = 0

    def emit(self, record):
        """Records an emitted message."""
        self._count += 1

    def check(self):
        """Raises if any messages above the chosen level have been emitted."""
        if self._count > 0:
            raise RuntimeError(f"Unsafe execution with {self._count} warnings.")


def get_logger(name):
    """Convenience wrapper to set up logging."""
    logger = _logging.getLogger(name)
    logger.propagate = False
    logger.setLevel(config.log_level)
    logger.addHandler(DefaultHandler)
    logger.addHandler(Enforcer())
    return logger
