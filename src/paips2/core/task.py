from kahnfigh import Config
from .settings import *
from .io import TaskIO, PaipFile
import copy
import fnmatch
import time

class Task:
    def __init__(self, config, name=None, logger=None, global_flags={}):
        self.config = Config(config)
        self.global_flags = global_flags
        self.cacheable = self.config.get('cache',global_flags.get('cache',True))
        self.in_memory = self.config.get('in_memory',global_flags.get('in_memory',False))
        self.cache_path = self.global_flags.get('cache_path','cache')
        self._hash_config = copy.deepcopy(self.config)
        self.priority = -self.config.get('priority',20)
        self.name = name
        self.logger = logger

    def format_outputs(self,outs,output_names,hash,storage_device='memory'):
        if not isinstance(outs,tuple):
            outs = (outs,)
        return {'{}{}{}'.format(self.name,symbols['membership'],k): TaskIO(v,hash,name=k,storage_device=storage_device) for k,v in zip(output_names,outs)}

    def get_hash(self):
        return self._hash_config.hash()

    def get_output_names(self):
        #Overrideable
        return self.config.get('output_names',['out'])

    def get_dependencies(self):
        dependencies = []
        for k,v in self.config.to_shallow().items():
            if isinstance(v,str) and symbols['membership'] in v:
                dependencies.append(v.split(symbols['membership'])[0])
        return dependencies

    def on_cache(self, cache_files, task_hash, output_names):
        return tuple(c for out_name,c in zip(output_names,cache_files))

    def process(self):
        #Overrideable
        pass
    
    def run(self):
        output_names = self.get_output_names()
        task_hash = self.get_hash()
        self.logger.debug('Task hash: {}'.format(task_hash))
        cache_results = self.search_cache(task_hash,output_names)
        if (cache_results is not None) and self.cacheable:
            extra_msg = '' if len(cache_results) == 1 else ' and {} more files'.format(len(cache_results) - 1)
            process_out = self.on_cache(cache_results,task_hash,output_names)
            storage_device = 'disk'
            self.logger.success('Cached task: {} from {}{}'.format(self.name, cache_results[0], extra_msg))
        else:
            self.logger.info('Running task: {}'.format(self.name))
            task_start = time.time()
            process_out = self.process()
            task_end = time.time()
            execution_time = task_end - task_start
            storage_device = 'memory'
            self.logger.success('Finished task: {} in {:.2f} s.'.format(self.name,execution_time))

        outs = self.format_outputs(process_out, output_names, task_hash, storage_device=storage_device)
        if (not self.in_memory) and (cache_results is None):
            outs = {k: v.save(self.cache_path) for k,v in outs.items()}
        return outs

    def search_cache(self,task_hash,output_names):
        process_out = []
        for out_name in output_names:
            cache_file = PaipFile(self.cache_path,task_hash,out_name)
            if cache_file.exists():
                process_out.append(cache_file)
            else:
                return None
        return process_out

    def send_dependency_data(self,data):
        #If glob patterns, replace by dependencies names
        glob_keys = self.config.find_path('*',mode='contains',action=lambda x: fnmatch.filter(list(data.keys()),x) if symbols['membership'] in x else x)
        glob_keys = self._hash_config.find_path('*',mode='contains',action=lambda x: fnmatch.filter(list(data.keys()),x) if symbols['membership'] in x else x)
        
        for k,v in data.items():
            paths = self._hash_config.find_path(k,action=lambda x: v.hash)
            if len(paths) > 0:
                self.config.find_path(k,action=lambda x: v.load())
            #else:
            #    if self.simulate and not k.startswith('self'):
            #        k_ = k.split('->')[0]+'->'
            #        paths = self._hash_dict.find_path(k_,action=lambda x: v.hash,mode='startswith')


