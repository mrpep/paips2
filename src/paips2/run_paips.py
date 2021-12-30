import argparse
from loguru import logger

from paips2.utils import format_logger, init_backend, shutdown_backend, add_arguments
from paips2.core import Graph
from kahnfigh import Config
from kahnfigh.utils import IgnorableTag
from paips2.core.compose import process_config
from paips2.core.settings import ignorable_tags
from pathlib import Path

import warnings
import numpy as np

def main():
    warnings.simplefilter(action='ignore', category=FutureWarning)
    argparser = argparse.ArgumentParser(description='Run pipeline from configs')
    add_arguments(argparser)
    args = vars(argparser.parse_args())

    if args['vv']:
        logging_level = 0
    elif args['v']:
        logging_level = 10
    elif args['silent']:
        logging_level = 100
    else:
        logging_level = 20
        
    format_logger(logger, level=logging_level)

    config = Config(args['config_path'],
              yaml_tags=[IgnorableTag('!{}'.format(tag)) for tag in ignorable_tags])
    config['class'] = 'Graph'
    config, global_config, default_config = process_config(config, mods=args['mods'],logger=logger)
    args['task_modules'] = config.get('task_modules',[])
    if args['explore'] is not None:
        from paips2.utils import module_from_file
        explore_module = module_from_file('exploration_script',args['explore'])
        modded_configs, modded_args = explore_module.get_configs(config,args)
        for i,(c,a) in enumerate(zip(modded_configs, modded_args)):
            if not Path(a['experiment_path']).exists():
                Path(a['experiment_path']).mkdir(parents=True)
            sink_id = logger.add(Path(a['experiment_path'],'logs'))
            logger.info('Running exploration. Experiment {}/{}: {}. Outputs will be saved in {}'.format(i+1,len(modded_configs),a['experiment_name'],a['experiment_path']))
            logger.info('Using backend {}'.format(a['backend']))
            init_backend(a['backend'])
            graph = Graph(config=c, name='main_graph',logger=logger, global_flags=a, main=True)
            graph.cacheable = False
            graph.run()
            logger.remove(sink_id) 
    else:
        if not Path(args['experiment_path']).exists():
            Path(args['experiment_path']).mkdir(parents=True)
        sink_id = logger.add(Path(args['experiment_path'],'logs'))
        logger.info('Using backend {}'.format(args['backend']))
        init_backend(args['backend'])
        logger.info('Running experiment {}. Outputs will be saved in {}'.format(args['experiment_name'], args['experiment_path']))
        graph = Graph(config=config, name='main_graph',logger=logger, global_flags=args, main=True)
        graph.cacheable = False
        graph.run()
    shutdown_backend(args['backend'])

if __name__ == '__main__':
    main()
