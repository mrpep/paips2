from .task import Task
from .graph_func import enqueue_tasks, wait_task_completion, run_next_task, gather_tasks

class Graph(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.backend = self.config.get('backend',self.global_flags.get('backend','ray'))

    def get_valid_parameters(self):
        return ['tasks'], ['task_modules']

    def process(self):
        tasks = gather_tasks(self.config, self.logger, self.global_flags) #Arma el diccionario de tareas a partir del archivo de configuracion
        to_do_tasks = list(tasks.keys())
        done_tasks = []
        available_tasks = enqueue_tasks(tasks,to_do_tasks,done_tasks) #Se fija cuales ya se pueden ejecutar
        queued_tasks = {}
        tasks_info = {k: {} for k in to_do_tasks} #Information about execution time and other stuff
        tasks_io = {}
        while (len(available_tasks) > 0) or (len(queued_tasks) > 0): #Mientras hayan tareas ejecutandose o para hacer
            if len(available_tasks) > 0: #Si hay para hacer entonces manda una (la de mayor prioridad) a ray
                task_output = run_next_task(self.logger,tasks,to_do_tasks,done_tasks,available_tasks,queued_tasks,tasks_info,tasks_io,mode=self.backend)
                if self.backend == 'sequential':
                    tasks_io.update(task_output)
                    available_tasks = enqueue_tasks(tasks,to_do_tasks,done_tasks)
            elif len(queued_tasks)>0: #Si no hay mas tareas disponibles pero hay tareas ejecutandose, chequear esperar a que alguna termine
                task_output = wait_task_completion(self.logger,tasks,to_do_tasks,done_tasks,available_tasks,queued_tasks,tasks_info,mode=self.backend)
                tasks_io.update(task_output)
                available_tasks = enqueue_tasks(tasks,to_do_tasks,done_tasks)