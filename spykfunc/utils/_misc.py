from  __future__ import print_function
import logging as _logging
from future.builtins import input
from contextlib import contextmanager
from .. import config
import sys


# -----------------------------------------------
# dictionary/tuple utils
# -----------------------------------------------
def get_or_create(d, key, factory, factory_args):
    i = d.get(key)
    if not i:
        i = d[key] = factory(**factory_args)
    return i


def make_slices(length, total):
    min_n = length / total
    remainder = length % total
    offset = 0
    for cur_it in range(total):
        n = min_n
        if cur_it < remainder:
            n += 1
        yield slice(offset, offset + n)
        offset += n


# -----------------------------------------------
# Class utils
# -----------------------------------------------

class classproperty(property):
    def __get__(self, obj, objtype=None):
        return super(classproperty, self).__get__(objtype)

    def __set__(self, obj, value):
        super(classproperty, self).__set__(type(obj), value)

    def __delete__(self, obj):
        super(classproperty, self).__delete__(type(obj))


# -----------------------------------------------
# UI utils
# -----------------------------------------------
def query_yes_no(question, default=None):
    """Ask a yes/no question via standard input and return the answer.
    Returns:
        A bool indicating whether user has entered yes or no.
    """
    default_dict = {  # default => prompt default string
        None: "[y/n]",
        True: "[Y/n]",
        False: "[y/N]",
    }
    prompt_str = "%s %s " % (question, default_dict[default])

    while True:
        choice = input(prompt_str)
        if not choice:
            if default is not None:
                return default
            continue

        choice = choice[0].lower()
        if choice == "y":
            return True
        elif choice == "n":
            return False


class ConsoleColors:
    BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = range(8)

    # These are the sequences need to get colored ouput
    _RESET_SEQ = "\033[0m"
    _COLOR_SEQ = "\033[1;{}m"

    @classmethod
    def reset(cls):
        return cls._RESET_SEQ

    @classmethod
    def set_text_color(cls, color):
        return cls._COLOR_SEQ.format(30 + color)

    @classmethod
    def format_text(cls, text, color):
        return cls.set_text_color(color) + text + cls._RESET_SEQ


# ---
def format_cur_exception():
    import traceback
    if config.log_level == _logging.DEBUG:
        return traceback.format_exc()
    exc_type, exc_value, exc_traceback = sys.exc_info()
    exc_infos = traceback.format_exception(exc_type, exc_value, exc_traceback)
    return exc_infos[1] + "".join(exc_infos[-2:])


# -----------------------------------------------
# Logging
# -----------------------------------------------
class ErrorHandler(_logging.StreamHandler):
    def emit(self, record):
        super(ErrorHandler, self).emit(record)
        if not query_yes_no("An error occurred. Do you want to continue execution?"):
            print("Exiting...")
            _logging.shutdown()
            sys.exit(1)


class ColoredFormatter(_logging.Formatter):
    COLORS = {
        'WARNING': ConsoleColors.YELLOW,
        'INFO': ConsoleColors.WHITE,
        'DEBUG': ConsoleColors.BLUE,
        'ERROR': ConsoleColors.RED,
        'CRITICAL': ConsoleColors.RED
    }

    def format(self, record):
        levelname = record.levelname
        msg = super(ColoredFormatter, self).format(record)
        if levelname in self.COLORS:
            return ConsoleColors.format_text(msg, self.COLORS[levelname])
        return msg


ContinueAbortErrorLogHandler = ErrorHandler()
ContinueAbortErrorLogHandler.setLevel(_logging.ERROR)
DefaultHandler = _logging.StreamHandler()
DefaultHandler.setLevel(_logging.DEBUG)
DefaultHandler.setFormatter(ColoredFormatter('[%(levelname)s] %(name)s: %(message)s'))


def get_logger(name):
    logger = _logging.getLogger(name)
    logger.propagate = False
    logger.setLevel(config.log_level)
    logger.addHandler(DefaultHandler)
    # logger.addHandler(ContinueAbortErrorLogHandler)
    return logger


@contextmanager
def show_wait_message(mesg):
    print(mesg + " Please wait...", end="\r")
    sys.stdout.flush()
    yield
    print(" "*(len(mesg) + 15), end="\r")  # Clear