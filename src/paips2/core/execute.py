from paips2.utils import format_logger, init_backend
from paips2.core import Graph
from kahnfigh import Config
from kahnfigh.utils import IgnorableTag
from paips2.core.compose import process_config
from paips2.core.settings import ignorable_tags
from pathlib import Path

import warnings
import numpy as np

import datetime

warnings.simplefilter(action='ignore', category=FutureWarning)

def execute_graph(config_path, logger=None, logging_level=20, mods=None, 
                  experiment_path='temp', experiment_name=str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")), 
                  backend='sequential', cache=False):
    if logger is not None:
        format_logger(logger, level=logging_level)
    config = Config(config_path,
              yaml_tags=[IgnorableTag('!{}'.format(tag)) for tag in ignorable_tags])
    config['class'] = 'Graph'
    config, global_config, default_config = process_config(config, mods=mods,logger=logger)
    task_modules = config.get('task_modules',[])
    global_flags = dict(
        task_modules = task_modules,
        backend = backend,
        experiment_path = experiment_path,
        experiment_name = experiment_name,
        mods = mods,
        cache = cache

    )
    if not Path(experiment_path).exists():
            Path(experiment_path).mkdir(parents=True)
    if logger is not None:
        sink_id = logger.add(Path(experiment_path,'logs'))
        logger.info('Using backend {}'.format(backend))
        init_backend(backend)
        logger.info('Running experiment {}. Outputs will be saved in {}'.format(experiment_name, experiment_path))
    graph = Graph(config=config, name='main_graph',logger=logger, global_flags=global_flags, main=True)
    graph.cacheable = False
    outs = graph.run()
    return outs
