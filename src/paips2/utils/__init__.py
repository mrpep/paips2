import sys
from .modiuls import *
from .plots import *
import datetime

def format_logger(logger, level=30):
    logger.remove()
    logger.add(sys.stderr, format="<lvl>{time:YYYY-MM-DD at HH:mm:ss} | {level:10s}| {message}</lvl>", level=level, colorize=True)

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
    argparser.add_argument('-v', action='store_true', default=False,help='Verbosity level 20')
    argparser.add_argument('-vv', action='store_true', default=False,help='Verbosity level 10')
    argparser.add_argument('-vvv', action='store_true', default=False,help='Verbosity level 0')
    argparser.add_argument('--silent', action='store_true', default=False,help='Deactivate logging')
    argparser.add_argument('--experiment_name', type=str,
                           help='Name for the experiment', default=date_time)
    argparser.add_argument('--experiment_path', type=str, help='Output directory for symbolic links of cache',
                           default='experiments/{}'.format(vars(argparser.parse_args())['experiment_name']))
    argparser.add_argument('--no-caching', dest='cache',
                           help='Run all', action='store_false', default=True)
    argparser.add_argument('--mods', dest='mods', type=str,
                           help='Modifications to config file')
    argparser.add_argument('--simulate', dest='simulate',
                           help='Just build graph without executing it', action='store_true', default=False)
    argparser.add_argument('--backend', dest='backend',
                           type=str, help='[sequential][ray]', default='sequential')
    argparser.add_argument('--explore', dest='explore',
                           type=str, help='Path to exploration python file', default=None)

def fast_kahnfigh_access(c,key):
    r = c.store
    for k in key.split('/'):
        try:
            k = int(k)
        except:
            pass
        r = r[k]
    return r

def fast_kahnfigh_assign(c,key,value):
    r = c.store
    k_parts = key.split('/')
    for k in k_parts[:-1]:
        try:
            k = int(k)
        except:
            pass
        r = r[k]
    r[k_parts[-1]] = value

def tree_iterate(e, e_path, shallow_d):
    if isinstance(e,list):
        for i,ei in enumerate(e):
            tree_iterate(ei,'{}/{}'.format(e_path,i),shallow_d)
    elif isinstance(e,dict):
        for k,v in e.items():
            tree_iterate(v,'{}/{}'.format(e_path,k),shallow_d)
    else:
        shallow_d[e_path[1:]] = e

def fast_kahnfigh_to_shallow(c):
    shallow_d = {}
    tree_iterate(c.store,'',shallow_d)
    return shallow_d