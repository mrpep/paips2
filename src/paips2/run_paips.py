import argparse
from loguru import logger

from paips2.utils import format_logger, init_backend, shutdown_backend, add_arguments
from paips2.core import Graph
from kahnfigh import Config

def main():
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

    logger.info('Using backend {}'.format(args['backend']))
    init_backend(args['backend'])
    logger.info('Running experiment {}. Outputs will be saved in {}'.format(args['experiment_name'], args['experiment_path']))
    config = Config(args['config_path'])
    config['class'] = 'Graph'
    graph = Graph(config=config, name='main_graph',logger=logger, global_flags=args, main=True)
    graph.cacheable = False
    graph.run()
    shutdown_backend(args['backend'])

if __name__ == '__main__':
    main()
