from paips2.utils import format_logger
from paips2.core import Graph
from kahnfigh import Config
import ray

def test_logger():
    from loguru import logger
    format_logger(logger)
    logger.info('Running logger test')
    logger.warning('There might be some problems')
    logger.debug('It seems the errors are caused by this')
    logger.critical('There are clearly critical errors')
    
def test_dependencies():
    ray.init()
    config = Config('configs/ex1.yaml')
    from loguru import logger
    format_logger(logger)
    graph = Graph(config=config,name='test_dependencies',logger=logger)
    graph.run()


test_logger()
test_dependencies()