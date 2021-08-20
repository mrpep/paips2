import sys
from .modiuls import *
import datetime

def format_logger(logger):
    logger.remove()
    logger.add(sys.stderr, format="<lvl>{time:YYYY-MM-DD at HH:mm:ss} | {level:10s}| {message}</lvl>", level=0, colorize=True)

def init_backend(backend):
    if backend == 'ray':
        import ray
        ray.init()

def shutdown_backend(backend):
    if backend == 'ray':
        import ray
        ray.shutdown()

def add_arguments(argparser):
    date_time = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    argparser.add_argument('config_path', help='Path to YAML config file for running experiment')
    argparser.add_argument('--experiment_name', type=str,
                           help='Name for the experiment', default=date_time)
    argparser.add_argument('--output_path', type=str, help='Output directory for symbolic links of cache',
                           default='experiments/{}'.format(date_time))
    argparser.add_argument('--no-caching', dest='cache',
                           help='Run all', action='store_false', default=True)
    argparser.add_argument('--mods', dest='mods', type=str,
                           help='Modifications to config file')
    argparser.add_argument('--simulate', dest='simulate',
                           help='Just build graph without executing it', action='store_true', default=False)
    argparser.add_argument('--backend', dest='backend',
                           type=str, help='[ray][none]', default='ray')