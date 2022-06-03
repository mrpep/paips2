from .task import Task
from .io import TaskIO
from .graph_func import enqueue_tasks, wait_task_completion, run_next_task, gather_tasks
from paips2.utils import sankey_plot
from pathlib import Path
from .settings import *
from kahnfigh import Config
import copy
import time
import random

class Graph(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.backend = self.config.get('backend',self.global_flags.get('backend','sequential'))
        self.children_backend = self.config.get('children_backend',self.backend)
        self.plot_graph = self.config.get('plot_graph',True)
        self.compiled = False

    def get_valid_parameters(self):
        return ['tasks'], ['task_modules','out','children_backend','persist_input', 'recompile_graph', 'probability']

    def get_dependencies(self):
        dependencies = []
        if 'in' in self.config:
            for k,v in Config(self.config['in']).to_shallow().items():
                if isinstance(v,str) and symbols['membership'] in v:
                    dependencies.append(v.split(symbols['membership'])[0])
        return dependencies

    def get_output_names(self):
        if self.is_lazy:
            return ['out']
        else:
            graph_outs = self.config.get('out')
            if graph_outs is not None:
                return list(graph_outs.keys())
            else:
                return ['out']

    def make_dag(self):
        self.tasks = gather_tasks(self.config, self.logger, self.global_flags) #Arma el diccionario de tareas a partir del archivo de configuracion
        for k,v in self.tasks.items():
            v.calculate_hashes = self.calculate_hashes
            if (not self.is_main) and (self.export_path is not None):
                v.export_path = self.export_path + '/{}'.format(self.name)
        if self.plot_graph:
            if self.is_main:
                sankey_plot(self.tasks, Path(self.export_path,'main_graph.html')) #Plotea el grafo y lo guarda en un html
            else:
                sankey_plot(self.tasks, Path(self.export_path,self.name,'graph.html'))

    def run_through_graph(self):
        start = time.time()
        to_do_tasks = list(self.tasks.keys())
        done_tasks = ['self']
        available_tasks = enqueue_tasks(self.tasks,to_do_tasks,done_tasks) #Se fija cuales ya se pueden ejecutar
        queued_tasks = {}
        tasks_info = {k: {} for k in to_do_tasks} #Information about execution time and other stuff
        tasks_io = {}

        graph_ins = self.config.get('in')
        if graph_ins:
            for k,v in graph_ins.items():
                tasks_io['self{}{}'.format(symbols['membership'],k)] = TaskIO(v,self._hash_config['in'][k],name=k,storage_device='memory')
        
        while (len(available_tasks) > 0) or (len(queued_tasks) > 0): #Mientras hayan tareas ejecutandose o para hacer
            if len(available_tasks) > 0: #Si hay para hacer entonces manda una (la de mayor prioridad) a ray
                task_output, task_backend = run_next_task(self.logger,self.tasks,to_do_tasks,done_tasks,available_tasks,queued_tasks,tasks_info,tasks_io,mode=self.children_backend)
                if task_backend == 'sequential':
                    tasks_io.update(task_output)
                    available_tasks = enqueue_tasks(self.tasks,to_do_tasks,done_tasks)
            elif len(queued_tasks)>0: #Si no hay mas tareas disponibles pero hay tareas ejecutandose, chequear esperar a que alguna termine
                task_output = wait_task_completion(self.logger,self.tasks,to_do_tasks,done_tasks,available_tasks,queued_tasks,tasks_info,mode=self.children_backend)
                tasks_io.update(task_output)
                available_tasks = enqueue_tasks(self.tasks,to_do_tasks,done_tasks)
        graph_outs = self.config.get('out')
        try:
            if graph_outs is not None:
                return tuple([tasks_io[graph_outs[out_name]].load() for out_name in self.get_output_names()])
        except:
            from IPython import embed; embed()
        

    def process(self):
        if self.config.get('recompile_graph',True) or (not self.compiled):
            self.make_dag()
            #self.dag_config = copy.deepcopy(self.config)
            if self.config.get('probability', None) is not None:
                dice = random.uniform(0,1)
                if dice < self.config['probability']:
                    result = self.run_through_graph()
                else:
                    result = tuple(self.config['in'].values())
            else:
                result = self.run_through_graph()
            self.compiled = True
        else:
            #self.reset(self.dag_config,replace_only_dependencies=True)
            if self.config.get('probability', None) is not None:
                dice = random.uniform(0,1)
                if dice < self.config['probability']:
                    result = self.run_through_graph()
                else:
                    result = tuple(self.config['in'].values())
            else:
                result = self.run_through_graph()
        return result

    def reset(self,config=None, replace_only_dependencies=False, persist_input=False):
        if config is None:
            config = self.original_config
        super().reset(config, replace_only_dependencies=replace_only_dependencies, persist_input=self.config.get('persist_input',False))

        #if hasattr(self,'tasks'):
        #    for t_name,t in self.tasks.items():
        #        t.reset(Config(config['tasks'][t_name]),replace_only_dependencies=replace_only_dependencies)
        
