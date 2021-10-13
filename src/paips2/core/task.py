from kahnfigh import Config
from .settings import *
from .io import TaskIO, PaipFile
import copy
import fnmatch
import time
import os
from pathlib import Path
from ruamel.yaml import YAML

class Task:
    def __init__(self, config, name=None, logger=None, global_flags={}, main=False):
        self.logger = logger

        self.config = Config(config)
        self.original_config = copy.deepcopy(self.config)

        self.priority = -self.config.get('priority',20)
        self.name = name
        self.is_main = main
        self.global_flags = global_flags
        self.cacheable = self.config.get('cache',global_flags.get('cache',True))
        self.in_memory = self.config.get('in_memory',global_flags.get('in_memory',False))
        self.cache_path = self.global_flags.get('cache_path','cache')
        self.export_path = self.global_flags.get('experiment_path',self.global_flags.get('experiment_name'))
        self.do_export = self.config.get('export',self.global_flags.get('export',True))
        self.is_lazy = self.config.get('lazy',False)
        self.backend = self.config.get('backend',None)
        self.calculate_hashes = True

        self._check_parameters()
        self._make_hash_config()

        self._dependency_paths = {k: v for k,v in self.config.to_shallow().items() if isinstance(v,str) and symbols['membership'] in v}

    def reset(self, config=None):
        if config is None:
            config = self.original_config
        self.config = copy.deepcopy(config)

    def _check_parameters(self):
        required_params, optional_params = self.get_valid_parameters()
        required_params += common_required_params
        optional_params += common_optional_params

        missing_params = [param for param in required_params if param not in self.config]
        if len(missing_params)>0:
            if self.logger is not None:
                self.logger.critical('Missing required parameter{} in task {}: {}'.format('s' if len(missing_params)>1 else '', self.name, missing_params))
            raise SystemExit(-1)
        unrecognized_params = [param for param in self.config if param not in required_params+optional_params]
        if len(unrecognized_params)>0:
            if self.logger is not None:
                self.logger.critical('Unrecognized parameter{} in task {}: {}'.format('s' if len(unrecognized_params)>1 else '', self.name, unrecognized_params))
            raise SystemExit(-1)

    def export(self,outs):
        self.on_export(outs)
        for k,v in outs.items():
            if v.storage_device == 'disk':
                if isinstance(v.data, str):
                    data_address = Path(v.data)
                else:
                    data_address = Path(v.data.local_filename)
                if self.is_main:
                    destination_path = Path(self.export_path,k.split(symbols['membership'])[-1])
                else:
                    destination_path = Path(self.export_path,self.name,k.split(symbols['membership'])[-1])
                if not destination_path.parent.exists():
                    destination_path.parent.mkdir(parents=True)

                equal_version_found = False
                version_number = 2
                while destination_path.is_symlink() and not equal_version_found:
                    existing_target_path = Path(destination_path).resolve()
                    if data_address.absolute() != existing_target_path.absolute():
                        destination_path = Path(destination_path.parent,k.split(symbols['membership'])[-1] + '_v{}'.format(version_number))
                        version_number += 1
                    else:
                        equal_version_found = True
                if version_number > 2 and not equal_version_found:
                    if self.logger is not None:
                        self.logger.warning('An exported file already exists in {}. Saved as version {}'.format(destination_path, version_number-1),enqueue=True)
                if not equal_version_found:
                    os.symlink(str(data_address.absolute()), str(destination_path.absolute()))
                    if version_number == 2:
                        v = ''
                    else:
                        v = '_v{}'.format(version_number-1)
                    self.original_config.save(Path(destination_path.parent,'config{}.yaml'.format(v)))

    def on_export(self, outs):
        #Overrideable to save custom files like .csv, plots, etc...
        pass

    def format_outputs(self,outs,output_names,hash,storage_device='memory'):
        if not isinstance(outs,tuple):
            outs = (outs,)
        return {'{}{}{}'.format(self.name,symbols['membership'],k): TaskIO(v,hash,name=k,storage_device=storage_device) for k,v in zip(output_names,outs)}

    def get_dependencies(self):
        dependencies = []
        for k,v in self.config.to_shallow().items():
            if isinstance(v,str) and symbols['membership'] in v:
                dependencies.append(v.split(symbols['membership'])[0])
        return dependencies

    def get_hash(self):
        return self._hash_config.hash()

    def get_output_names(self):
        #Overrideable
        return self.config.get('output_names',['out'])

    def get_valid_parameters(self):
        #Each task should override this method
        return common_required_params, common_optional_params

    def _make_hash_config(self):
        self._hash_config = copy.deepcopy(self.config)
        for param in not_cacheable_params:
            if param in self._hash_config:
                self._hash_config.pop(param)

        yaml_processor = YAML()
        for k,v in self.config.to_shallow().items():
            if isinstance(v,str) and v.startswith('!no-cache'):
                self._hash_config.pop(k)
                self.config[k] = yaml_processor.load(v.split('!no-cache ')[-1])

    def on_cache(self, cache_files, task_hash, output_names):
        #This can be overriden
        return tuple(c for out_name,c in zip(output_names,cache_files))

    def process(self):
        #Overrideable
        pass
    
    def run(self):
        if self.is_lazy:
            output_names = ['out']
        else:
            output_names = self.get_output_names()
        task_hash = self.get_hash()
        if self.logger is not None:
            self.logger.debug('Task hash: {}'.format(task_hash),enqueue=True)
        cache_results = self.search_cache(task_hash,output_names)
        if (cache_results is not None) and self.cacheable: #Cache if possible
            extra_msg = '' if len(cache_results) == 1 else ' and {} more files'.format(len(cache_results) - 1)
            process_out = self.on_cache(cache_results,task_hash,output_names)
            storage_device = 'disk'
            if self.logger is not None:
                self.logger.success('Cached task: {} from {}{}'.format(self.name, cache_results[0], extra_msg),enqueue=True)
        else: #Run task if not possible
            if self.logger is not None:
                self.logger.info('Running task: {}'.format(self.name),enqueue=True)
            task_start = time.time()
            if self.is_lazy:
                self.is_lazy = False
                process_out = self
            else:
                process_out = self.process()
            task_end = time.time()
            execution_time = task_end - task_start
            storage_device = 'memory'
            if self.logger is not None:
                self.logger.success('Finished task: {} in {:.2f} s.'.format(self.name,execution_time),enqueue=True)

        outs = self.format_outputs(process_out, output_names, task_hash, storage_device=storage_device)
        if (not self.in_memory) and (cache_results is None): #Save outputs if not caching
            outs = {k: v.save(self.cache_path,
                              export_path=self.export_path,
                              compression_level=self.config.get('cache_compression', self.global_flags.get('cache_compression',0)),
                              export=True,
                              cache_db=self.global_flags.get('cache_db')
                              ) for k,v in outs.items()}
        if self.do_export:
            self.export(outs)
        return outs

    def search_cache(self,task_hash,output_names):
        #This can be overriden
        process_out = []
        for out_name in output_names:
            cache_file = PaipFile(self.cache_path,task_hash,out_name)
            if cache_file.exists():
                process_out.append(cache_file)
            else:
                return None
        return process_out

    def send_dependency_data(self,data):
        for k,v in self._dependency_paths.items():
            if v in data:
                self.config[k] = data[v].load()
                if self.calculate_hashes:
                    self._hash_config[k] = data[v].hash

    def __getstate__(self):
        #Problems for serializing lazy tasks as logger is unpickleable
        self.__dict__['logger'] = None
        return self.__dict__
