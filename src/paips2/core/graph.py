from .task import Task
from .io import TaskIO
from .graph_func import enqueue_tasks, wait_task_completion, run_next_task, gather_tasks
from paips2.utils import sankey_plot
from pathlib import Path
from .settings import *
from kahnfigh import Config

class Graph(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.backend = self.config.get('backend',self.global_flags.get('backend','ray'))

    def get_valid_parameters(self):
        return ['tasks'], ['task_modules','out']

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
        if self.is_main:
            sankey_plot(self.tasks, Path(self.export_path,'main_graph.html')) #Plotea el grafo y lo guarda en un html
        else:
            sankey_plot(self.tasks, Path(self.export_path,self.name,'graph.html'))

    def run_through_graph(self):
        to_do_tasks = list(self.tasks.keys())
        done_tasks = ['self']
        available_tasks = enqueue_tasks(self.tasks,to_do_tasks,done_tasks) #Se fija cuales ya se pueden ejecutar
        queued_tasks = {}
        tasks_info = {k: {} for k in to_do_tasks} #Information about execution time and other stuff
        tasks_io = {}

        graph_ins = self.config.get('in')
        if graph_ins:
            for k,v in graph_ins.items():
                tasks_io['self{}{}'.format(symbols['membership'],k)] = v

        while (len(available_tasks) > 0) or (len(queued_tasks) > 0): #Mientras hayan tareas ejecutandose o para hacer
            if len(available_tasks) > 0: #Si hay para hacer entonces manda una (la de mayor prioridad) a ray
                task_output = run_next_task(self.logger,self.tasks,to_do_tasks,done_tasks,available_tasks,queued_tasks,tasks_info,tasks_io,mode=self.backend)
                if self.backend == 'sequential':
                    tasks_io.update(task_output)
                    available_tasks = enqueue_tasks(self.tasks,to_do_tasks,done_tasks)
            elif len(queued_tasks)>0: #Si no hay mas tareas disponibles pero hay tareas ejecutandose, chequear esperar a que alguna termine
                task_output = wait_task_completion(self.logger,self.tasks,to_do_tasks,done_tasks,available_tasks,queued_tasks,tasks_info,mode=self.backend)
                tasks_io.update(task_output)
                available_tasks = enqueue_tasks(self.tasks,to_do_tasks,done_tasks)
        
        graph_outs = self.config.get('out')
        if graph_outs is not None:
            return tuple([tasks_io[graph_outs[out_name]].load() for out_name in self.get_output_names()])

    def process(self):
        self.make_dag()
        self.run_through_graph()
                
