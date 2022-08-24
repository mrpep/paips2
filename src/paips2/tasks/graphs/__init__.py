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

    def get_dependencies(self):
        dependencies = []
        if 'in' in self.config:
            for k,v in Config(self.config['in']).to_shallow().items():
                if isinstance(v,str) and symbols['membership'] in v:
                    dependencies.append(v.split(symbols['membership'])[0])         
        return dependencies

    def get_child_graph(self):
        graph_config = self.config.get('graph')
        graph_config = Config(graph_config)
        graph_name = list(graph_config.keys())[0]
        graph_config = graph_config[graph_name]
        graph_task = Graph(graph_config,graph_name,self.logger,self.global_flags)
        return graph_name, graph_task

    def process(self):
        graph_name, graph_task = self.get_child_graph()
        if 'in' in self.config:
            ins = {k: TaskIO(v,self._hash_config['in'][k],name=k,storage_device='memory') for k,v in self.config['in'].items()}
            graph_task.config['in'].update(ins)
        if self.export_path is not None:
            graph_task.export_path = self.export_path + '/{}'.format(self.name)
        else:
            graph_task.export_path = None
        graph_task.in_memory = True
        graph_task.simulate = self.simulate
        if self.simulate:
            if hasattr(self, 'simulation_result'):
                graph_task.simulation_result = self.simulation_result
            else:
                graph_task.simulation_result = {}

        prefix_nck = 'graph/{}/'.format(graph_task.name)
        child_nck = [nck.split(prefix_nck)[-1] if nck.startswith(prefix_nck) else nck for nck in self.not_cachable_keys]
        graph_task.not_cachable_keys.extend(child_nck)
        for k in graph_task.not_cachable_keys:
            if k in graph_task._hash_config:
                graph_task._hash_config.pop(k)

        outs = graph_task.run()
        out_names = graph_task.get_output_names()
        if not isinstance(out_names, list):
            out_names = [out_names]

        return tuple([outs['{}{}{}'.format(graph_name,symbols['membership'],k)].load() for k in out_names])

class MapGraph(Task):
    def get_valid_parameters(self):
        return ['graph','map_in'], ['n_workers', 'chunk_size','in']

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

    def get_dependencies(self):
        dependencies = []
        if 'in' in self.config:
            for k,v in Config(self.config['in']).to_shallow().items():
                if isinstance(v,str) and symbols['membership'] in v:
                    dependencies.append(v.split(symbols['membership'])[0])
        if 'map_in' in self.config:
            for k,v in Config(self.config['map_in']).to_shallow().items():
                if isinstance(v,str) and symbols['membership'] in v:
                    dependencies.append(v.split(symbols['membership'])[0])            
        return dependencies

    def map_process(self, data, graph_name, graph_task):
        out_names = graph_task.get_output_names()
        map_ins = self.config['map_in']
        map_lens = [len(v) for k,v in map_ins.items()]
        #Check that all lens are equal:
        if not all([l == map_lens[0] for l in map_lens]):
            raise ValueError('Map inputs must have equal length')
            
        graph_ins = []
        for i in range(map_lens[0]):
            g_in = {}
            for k,v in map_ins.items():
                if self._hash_config['map_in'][k] == v:
                    hash_val = v[i]
                else:
                    hash_val = self._hash_config['map_in'][k] + '_{}'.format(i)
                g_in[k] = TaskIO(v[i], hash_val)
            graph_ins.append(g_in)
        #graph_ins = [{k: TaskIO(v[i], self._hash_config['map_in'][k] + '_{}'.format(i)) } for i in range(map_lens[0])]
        for k, v in self.config.get('in',{}).items():
            for g_in in graph_ins:
                g_in[k] = TaskIO(copy.deepcopy(v), self._hash_config['in'][k])

        map_outs = []
        config_in = copy.deepcopy(graph_task.config)
        for i,d in enumerate(tqdm(graph_ins)):
            graph_task.config = copy.deepcopy(config_in)
            graph_task.config['in'].update(d)
            graph_task.export_path = self.export_path + '/{}/{}'.format(self.name,i)
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