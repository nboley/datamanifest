"""Logger with nice formatting and file & stderr output handlers."""
import sys
import logging
from threading import Thread
import tempfile
import time
from io import FileIO, BytesIO
import argparse

import logging
from logging import CRITICAL, ERROR, WARNING, INFO, DEBUG, NOTSET  # noqa: F401

# capture warnings from the warnings module in logging
logging.captureWarnings(True)

LOG_LEVELS = {
    "CRITICAL": 50,
    "ERROR": 40,
    "WARNING": 30,
    "INFO": 20,
    "DEBUG": 10,
    "NOTSET": 0,
}
# make sure that these levels all exist
assert all(logging.getLevelName(value) == level for level, value in LOG_LEVELS.items())

DEFAULT_FILE_DESCRIPTOR_LOG_LEVEL = LOG_LEVELS["INFO"]
DEFAULT_LOG_FORMAT = "[%(name)s : %(asctime)-15s %(filename)s - %(funcName)s() ] %(message)s"


def logger_is_verbose(logger):
    return logger.getEffectiveLevel() <= LOG_LEVELS["INFO"]


class FileDescriptorLogger(int):
    """Effectively a logger sub-class that returns a file descriptor rather than a logger.

    This is useful when one wants to write subprocess output to a logger instance.
    All messages written to the FD will be logged at the same level.
    """

    def __new__(cls, *args, **kwargs):
        writer = tempfile.NamedTemporaryFile(delete=True)
        rv = int.__new__(FileDescriptorLogger, writer.fileno())
        rv.writer = writer
        return rv

    def __init__(self, logger, level=DEFAULT_FILE_DESCRIPTOR_LOG_LEVEL):
        int.__init__(self)
        self.logger = logger
        self.level = level
        # writer is set in the __new__ method
        self.reader = FileIO(self.writer.name, "r+b")
        self.start_worker_thread(self.reader, self.logger, self.level)

    @staticmethod
    def iter_over_lines(reader, return_when_empty):
        """Read bytes from an IO stream until we hit a new line, and then return it"""
        line = ""
        while True:
            char = reader.read(1).decode()
            if char == "":
                if return_when_empty is True:
                    return
                else:
                    time.sleep(0.1)
                    continue
            if char == "\n":
                yield line.strip()
                line = ""
            else:
                line += char
        assert False, "Unreachable"

    @staticmethod
    def write_data_to_logger(reader, logger, level, return_when_empty):
        """Log data in fd to logger.
        Args:
            reader: readable object
            logger: logger target for writing
            level: logging level
            return_when_empty: return as soon as the log buffer is empty
        Returns:
            None
        """
        try:
            for line in FileDescriptorLogger.iter_over_lines(reader, return_when_empty):
                logger.log(level, line)
        except ValueError:
            # make sure that the reader is closed
            assert reader.closed is True
            return

    @staticmethod
    def start_worker_thread(reader, logger, level):
        def process_data_and_exit_thread(reader, logger, level):
            FileDescriptorLogger.write_data_to_logger(reader, logger, level, False)
            sys.exit()

        t = Thread(target=process_data_and_exit_thread, args=(reader, logger, level))
        t.start()

    def __del__(self):
        # flush buffers from (move data from writer -> reader)
        self.writer.flush()
        # close the writer
        self.writer.close()
        # flush any remaining data in reader
        self.write_data_to_logger(self.reader, self.logger, self.level, True)
        # finally close the reader
        self.reader.close()

    def close(self):
        self.__del__()

    def flush(self):
        self.writer.flush()
        self.reader.flush()

    def write(self, s):
        if not s.endswith("\n"):
            raise ValueError("Lines to write must end in newline.")
        self.start_worker_thread(BytesIO(s.encode("utf-8")), self.logger, self.level)


class Logger(logging.getLoggerClass()):
    def getFDLogger(self, level):
        return FileDescriptorLogger(level=level, logger=self)


# set the logger class so that we can use getFDLogger
logging.setLoggerClass(Logger)


def configure_root_logger(
    filename=None,
    file_level=LOG_LEVELS["NOTSET"],
    file_format=DEFAULT_LOG_FORMAT,
    stream_level=LOG_LEVELS["WARNING"],
    stream_format=DEFAULT_LOG_FORMAT,
    output_to_stream=True,
    **kwargs,
):
    handlers = []
    if filename is not None:
        # Create a file logging handler
        file_handler = logging.FileHandler(filename=filename)
        file_handler.setLevel(file_level)
        file_handler.setFormatter(logging.Formatter(file_format))
        handlers.append(file_handler)

    if output_to_stream is True:
        # basicConfig ignores `level` for the stderr stream unless it is a handler passed to
        # basicConfig.
        # Create a stream handler
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(stream_level)
        stream_handler.setFormatter(logging.Formatter(stream_format))
        handlers.append(stream_handler)

    root_logger = logging.getLogger()
    root_logger_level = stream_level
    if filename:
        root_logger_level = min(stream_level, file_level)
    # set the level to stream so that we can use getEffectiveLevel
    root_logger.setLevel(root_logger_level)
    # remove any existing loggers
    del root_logger.handlers[:]
    for handler in handlers:
        root_logger.addHandler(handler)


def configure_root_logger_from_args(args):
    # set the logging level baed upon flag
    stream_level = logging.WARNING
    # note that we add the asserts because those arguments
    # are mutually exclusive in the argumnet parser setup
    if args.quiet is True:
        stream_level = logging.ERROR
        assert args.verbose is False
        assert args.debug is False
    if args.verbose is True:
        stream_level = logging.INFO
        assert args.quiet is False
        assert args.debug is False
    if args.debug is True:
        stream_level = logging.DEBUG
        assert args.quiet is False
        assert args.verbose is False

    configure_root_logger(
        filename=args.log_filename,
        file_level=args.log_file_verbosity_level,
        file_format=args.log_format,
        format=args.log_format,
        stream_level=stream_level,
        level=0,  # we always set the level to 0 so that the stream handlers control the level
    )


class SetLevelAction(argparse.Action):
    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        if nargs is not None:
            raise ValueError("nargs not allowed")
        super(SetLevelAction, self).__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, log_level, option_string=None):
        assert isinstance(log_level, str)
        setattr(namespace, self.dest, LOG_LEVELS[log_level])


def build_log_parser():
    parser = argparse.ArgumentParser(add_help=False)
    log_parser = parser.add_argument_group("logging")
    verbosity_flags = log_parser.add_mutually_exclusive_group(required=False)
    verbosity_flags.add_argument(
        "--quiet",
        "-q",
        default=False,
        action="store_true",
        help="Only output error log messages (and above) to the output stream.",
    )
    verbosity_flags.add_argument(
        "--verbose",
        "-v",
        default=False,
        action="store_true",
        help="Output info level log messages (and above) to the output stream.",
    )
    verbosity_flags.add_argument(
        "--debug",
        default=False,
        action="store_true",
        help="Output debug level log messages (and above) to the output stream.",
    )
    log_parser.add_argument(
        "--log-format", type=str, default=DEFAULT_LOG_FORMAT, help="Format string to use for log messages.",
    )

    log_parser.add_argument(
        "--log-filename",
        type=str,
        default=None,
        help="Write log messages to both the default handler and --log-filename. "
        "(default: do not write messages to a log file)",
    )
    log_parser.add_argument(
        "--log-file-verbosity-level",
        type=str,
        choices=list(LOG_LEVELS),
        action=SetLevelAction,  # set the log level value
        default="NOTSET",
        help="Logging level to use for the log file handler. (default: log all messages)",
    )
    log_parser.add_argument(
        "--log-file-format",
        type=str,
        default=DEFAULT_LOG_FORMAT,
        help="Format string to use for log messages written to file (see LogRecord in the logging module docs).",
    )

    # verify that the log file arguments are consistent
    args, _ = parser.parse_known_args()
    # make sure that --log-file-verbosity-level and --log-file-format are not set
    # if --log-file is not specified
    if args.log_filename is None:
        if args.log_file_verbosity_level != log_parser.get_default("log_file_verbosity_level"):
            log_parser.error(
                "It is not sensible to set --log-file-verbosity-level without specifying --log-filename"
            )
        if args.log_file_format != log_parser.get_default("log_file_format"):
            log_parser.error("It is not sensible to set --log-file-format without specifying --log-filename")

    return parser


# move this into the namespace
getLogger = logging.getLogger
