import sys
from .modiuls import *

def format_logger(logger):
    logger.remove()
    logger.add(sys.stderr, format="<lvl>{time:YYYY-MM-DD at HH:mm:ss} | {level:10s}| {message}</lvl>", level=0, colorize=True)

