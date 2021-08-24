from paips2.core import Task, Graph, TaskIO
from paips2.core.settings import symbols

from kahnfigh import Config

class GraphModule(Task):
    def get_valid_parameters(self):
        return ['graph'], []

    def get_output_names(self):
        return self.get_child_graph()[-1].get_output_names()

    def get_child_graph(self):
        graph_config = self.config.get('graph')
        if isinstance(graph_config,str):
            graph_config = Config(graph_config)
        graph_name = list(graph_config.keys())[0]
        graph_config = graph_config[graph_name]
        graph_task = Graph(graph_config,graph_name,self.logger,self.global_flags)
        return graph_name, graph_task

    def process(self):
        graph_name, graph_task = self.get_child_graph()
        ins = {k: TaskIO(v,self._hash_config['in'][k],name=k,storage_device='memory') for k,v in self.config['in'].items()}
        graph_task.config['in'].update(ins)
        graph_task.in_memory = True
        outs = graph_task.run()
        out_names = graph_task.get_output_names()
        return tuple([outs['{}{}{}'.format(graph_name,symbols['membership'],k)].load() for k in out_names])
