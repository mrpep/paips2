from ray import worker
from paips2.core import Task, Graph, TaskIO
from paips2.core.settings import symbols
from tqdm import tqdm
import copy

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
        if not isinstance(out_names, list):
            out_names = [out_names]

        return tuple([outs['{}{}{}'.format(graph_name,symbols['membership'],k)].load() for k in out_names])

class MapGraph(Task):
    def get_valid_parameters(self):
        return ['graph','in'], ['n_workers', 'chunk_size']

    def get_output_names(self):
        return self.get_child_graph()[-1].get_output_names()

    def get_child_graph(self):
        graph_config = self.config.get('graph')
        if isinstance(graph_config,str):
            graph_config = Config(graph_config)
        graph_name = list(graph_config.keys())[0]
        graph_config = graph_config[graph_name]
        graph_task = Graph(graph_config,graph_name,None,self.global_flags)
        return graph_name, graph_task

    def map_process(self, data, graph_name, graph_task):
        out_names = graph_task.get_output_names()
        data_len = max([len(v) for k,v in data.items()])
        graph_ins = [{k: TaskIO(v[i], self._hash_config['in'][k]) for k,v in data.items()} for i in range(data_len)]
        map_outs = []
        config_in = copy.deepcopy(graph_task.config)
        for d in tqdm(graph_ins):
            graph_task.config = copy.deepcopy(config_in)
            graph_task.config['in'].update(d)
            out_i = graph_task.run()
            out_i = [out_i['{}{}{}'.format(graph_name,symbols['membership'],k)].load() for k in out_names]
            map_outs.append(out_i)

        return tuple([[m[i] for m in map_outs] for i,k in enumerate(out_names)])

    def process(self):       
        graph_name, graph_task = self.get_child_graph()
        graph_task.in_memory = True
        n_workers = self.config.get('n_workers',1)
        if n_workers <= 1:
            return self.map_process(self.config['in'], graph_name, graph_task)
        else:
            import ray
            ray.init(ignore_reinit_error=True)

            data_len = max([len(v) for k,v in self.config['in'].items()])
            worker_chunksize = data_len//n_workers
            workers_data = [{k: v[i*worker_chunksize:(i+1)*worker_chunksize] if i < n_workers - 1 else v[i*worker_chunksize:] for k,v in self.config['in'].items()} for i in range(n_workers)]
            def worker_map(self_, data):
                def worker_fn():
                    return self_.map_process(data,graph_name,graph_task)
                return worker_fn
            workers = [ray.remote(worker_map(self,w_data)).remote() for w_data in workers_data]
            workers_out = ray.get(workers)
            return tuple([[o for w in workers_out for o in w[i]] for i in range(len(workers_out[0]))])

        #ins = {k: TaskIO(v,self._hash_config['in'][k],name=k,storage_device='memory') for k,v in self.config['in'].items()}

        
        
        
        
        #return tuple([outs['{}{}{}'.format(graph_name,symbols['membership'],k)].load() for k in out_names])
