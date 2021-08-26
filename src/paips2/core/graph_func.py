import heapq
from paips2.utils import get_modules, get_classes_in_module

def enqueue_tasks(tasks, to_do_tasks, done_tasks):
    # Recibe un diccionario tasks <nombre, task>, 
    # una lista con los nombres de las tareas a realizar,
    # y una lista con los nombres de las tareas ya terminadas

    #Devuelve una lista con las tareas que ya se pueden realizar (porque sus dependencias fueron ejecutadas)
    #La lista esta ordenada por priority (se usa internamente un heap)

    available_tasks = []
    for task_name in to_do_tasks:
        enqueue = True
        for dependency in tasks[task_name].get_dependencies():
            if dependency not in done_tasks:
                enqueue = False
        if enqueue:
            heapq.heappush(available_tasks,(tasks[task_name].priority,task_name))
    available_tasks = [heapq.heappop(available_tasks)[1] for i in range(len(available_tasks))]
    return available_tasks

def gather_tasks(config, logger, global_flags):
    task_modules = get_modules(global_flags.get('task_modules',[]))
    tasks = {}
    for task_name, task_config in config['tasks'].items():
        task_class = task_config['class']
        task_obj = [getattr(module,task_class) for module in task_modules if task_class in get_classes_in_module(module)]
        if len(task_obj) == 0:
            if logger is not None:
                logger.critical('{} not recognized as a task'.format(task_class))
            raise SystemExit(-1)
        elif len(task_obj) > 1:
            if logger is not None:
                logger.critical('{} found in multiple task modules. Rename the task in your module to avoid name collisions'.format(task_class))
            raise SystemExit(-1)
        task_obj = task_obj[0]
        tasks[task_name] = task_obj(task_config,task_name,logger,global_flags=global_flags)
    return tasks

def run_next_task(logger, tasks, to_do_tasks, done_tasks, available_tasks, queued_tasks, tasks_info, tasks_io, mode='ray'):
    task_name = available_tasks[0]
    task = tasks[task_name]
    task.send_dependency_data(tasks_io)
    if mode == 'ray':
        import ray
        def run_task(task):
            return task.run()
        out = ray.remote(run_task).remote(task)
        queued_tasks[out] = task_name
    elif mode == 'sequential':
        out = task.run()
        done_tasks.append(task_name)
    #Se actualizan las listas de tareas a realizar/disponibles, 
    #y se agrega la referencia de ray a un diccionario de tareas encoladas
    to_do_tasks.remove(task_name)
    available_tasks.remove(task_name)
    return out
    
def wait_task_completion(logger, tasks, to_do_tasks, done_tasks, available_tasks, queued_tasks, tasks_info, mode='ray'):
    if mode == 'ray':
        import ray
        done_ids, _ = ray.wait(list(queued_tasks.keys()))
        task_result = ray.get(done_ids)
        tasks_io = {}
        for i,done_id in enumerate(done_ids):
            done_tasks.append(queued_tasks[done_id])
            done_task_name = queued_tasks[done_id]
            queued_tasks.pop(done_id)
            tasks_io.update(task_result[i])

        return tasks_io