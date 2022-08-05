import sys
import logging as _logging
from functools import update_wrapper
from contextlib import contextmanager
from .. import config


# -----------------------------------------------
# string utils
# -----------------------------------------------
def to_native_str(text):
    return text if isinstance(text, str) else str(text.decode())


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
        return super().__get__(objtype)

    def __set__(self, obj, value):
        super().__set__(type(obj), value)

    def __delete__(self, obj):
        super().__delete__(type(obj))


# -----------------------------------------------
# Func utils
# -----------------------------------------------
def assign_to_property(prop_name, use_as_cache=False):
    def decorator(f):
        "Convenience docorator to assign results to a property of the instance"

        def newf(self, *args, **kw):
            if use_as_cache:
                val = getattr(self, prop_name)
                if val is not None:
                    return val
            val = f(self, *args, **kw)
            setattr(self, prop_name, val)
            return val

        return update_wrapper(newf, f)

    return decorator


# -----------------------------------------------
# UI utils
# -----------------------------------------------
class ConsoleColors:
    BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE, _, DEFAULT = range(10)
    NORMAL, BOLD, DIM, UNDERLINED, BLINK, INVERTED, HIDDEN = [a << 4 for a in range(7)]

    # These are the sequences need to get colored ouput
    _RESET_SEQ = "\033[0m"
    _CHANGE_SEQ = "\033[{}m"

    @classmethod
    def reset(cls):
        return cls._RESET_SEQ

    @classmethod
    def set_text_color(cls, color):
        return cls._CHANGE_SEQ.format(color + 30)

    @classmethod
    def format_text(cls, text, color, style=None):
        if color > 7:
            style = color >> 4
            color = color & 0xF
        format_seq = "" if style is None else cls._CHANGE_SEQ.format(style)

        return format_seq + cls.set_text_color(color) + text + cls._RESET_SEQ


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
class ColoredFormatter(_logging.Formatter):
    COLORS = {
        "WARNING": ConsoleColors.YELLOW,
        "INFO": ConsoleColors.DEFAULT + ConsoleColors.BOLD,
        "DEBUG": ConsoleColors.BLUE,
        "ERROR": ConsoleColors.RED,
        "CRITICAL": ConsoleColors.RED,
    }

    def format(self, record):
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
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class Enforcer(_logging.Handler, metaclass=Singleton):
    def __init__(self, level=_logging.WARNING):
        super().__init__(level)
        self._count = 0

    def emit(self, record):
        self._count += 1

    def check(self):
        if self._count > 0:
            raise RuntimeError(f"Unsafe execution with {self._count} warnings.")


def get_logger(name):
    logger = _logging.getLogger(name)
    logger.propagate = False
    logger.setLevel(config.log_level)
    logger.addHandler(DefaultHandler)
    logger.addHandler(Enforcer())
    return logger


@contextmanager
def show_wait_message(mesg):
    print(mesg + " Please wait...", end="\r")
    sys.stdout.flush()
    yield
    print(" " * (len(mesg) + 15), end="\r")  # Clear
