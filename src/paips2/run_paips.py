import argparse
from loguru import logger

from paips2.utils import format_logger, init_backend, shutdown_backend, add_arguments
from paips2.core import Graph
from kahnfigh import Config

def main():
    argparser = argparse.ArgumentParser(description='Run pipeline from configs')
    add_arguments(argparser)

    args = vars(argparser.parse_args())

    init_backend(args['backend'])
    config = Config(args['config_path'])
    
    format_logger(logger)
    graph = Graph(config=config, name='main_graph',logger=logger, global_flags=args, main=True)
    graph.cacheable = False
    graph.run()
    shutdown_backend(args['backend'])

if __name__ == '__main__':
    main()
