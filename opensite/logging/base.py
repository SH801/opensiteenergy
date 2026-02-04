import logging
import sys
import multiprocessing
from opensite.constants import OpenSiteConstants
from colorama import Fore, Style, init

init()

class ColorFormatter(logging.Formatter):
    """Custom Formatter to add colors to log levels for Terminal only."""
    
    FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

    LEVEL_COLORS = {
        logging.DEBUG:      Fore.WHITE,
        logging.INFO:       Fore.BLUE,
        logging.WARNING:    Fore.YELLOW,
        logging.ERROR:      Fore.RED,
    }

    def format(self, record):
        color = self.LEVEL_COLORS.get(record.levelno, Style.RESET_ALL)
        log_fmt = f"{color}{self.FORMAT}{Style.RESET_ALL}"
        # We create a temporary formatter with the colorized string
        formatter = logging.Formatter(log_fmt, datefmt='%H:%M:%S')
        return formatter.format(record)

class LoggingBase:
    def __init__(self, name: str, level=logging.DEBUG, lock: multiprocessing.Lock = None):
        self.mark_counter = 1
        self.logger = logging.getLogger(name.ljust(21))
        self.logger.setLevel(level)
        self.lock = lock
        self.logger.propagate = False
        
        if not self.logger.handlers:
            # Terminal handler - with colors
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(ColorFormatter())
            self.logger.addHandler(console_handler)
            
            # File handler - clean text, no colors
            # This ensures opensite.log remains human-readable
            file_handler = logging.FileHandler('opensite.log')
            clean_formatter = logging.Formatter(
                '%(asctime)s [%(levelname)-8s] %(name)s: %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            file_handler.setFormatter(clean_formatter)
            self.logger.addHandler(file_handler)

    def mark(self):
        """General mark function to indicate place in code reached"""
        self.error(f"{self.mark_counter} reached")
        self.mark_counter += 1
        
    def debug(self, msg: str):
        if self.lock:
            with self.lock:
                self.logger.debug(msg)
        else:
            self.logger.debug(msg)

    def info(self, msg: str):
        if self.lock:
            with self.lock:
                self.logger.info(msg)
        else:
            self.logger.info(msg)

    def warning(self, msg: str):
        if self.lock:
            with self.lock:
                self.logger.warning(msg)
        else:
            self.logger.warning(msg)

    def error(self, msg: str):
        if self.lock:
            with self.lock:
                self.logger.error(msg)
        else:
            self.logger.error(msg)

