from paips2.core import Task, TaskIO,settings
import numpy as np
import copy
from torch.utils.data import Dataset, DataLoader
from paips2.core.compose import apply_mods, replace_var_dollars

import time

class TorchDataset(Dataset):
    def __init__(self, data = None, extra_data=None, data_processing_task = None, data_processing_mods = None, x_names=None,y_names=None):
        self._data, self.extra_data, self.data_processing_task = data, extra_data, data_processing_task
        if self.data_processing_task is not None:
            self.data_processing_task.in_memory = True
            self.data_processing_task.logger = None
            self.data_processing_task.do_export=False
            self.data_processing_task.cacheable=False
            self.data_processing_task.make_dag()
            self.original_data_processing_config = copy.deepcopy(self.data_processing_task.config)
            if data_processing_mods is not None:
                for mod_k, mod_v in data_processing_mods.items():
                    mod_k = mod_k.replace('.','/')
                    self.original_data_processing_config[mod_k] = mod_v
                #apply_mods(self.original_data_processing_config, data_processing_mods)
            self.data_processing_task.plot_graph = False
            self.data_processing_task.calculate_hashes = False

        if self.extra_data is None:
            self.extra_data = {}
        self.x_names, self.y_names = x_names, y_names
        if not isinstance(self.x_names,list):
            self.x_names = [self.x_names]  
        if not isinstance(self.y_names,list):
            self.y_names = [self.y_names]

    def __getitem__(self,step):
        batch_data = self._data.iloc[step]
        if self.data_processing_task is not None:
            #ins = {k: TaskIO(batch_data[k],'0',name='batch_{}'.format(k),storage_device='memory') if k in batch_data else TaskIO(self.extra_data[k],'0',name='batch_{}'.format(k),storage_device='memory') for k in self.data_processing_task.config['in'].keys()}
            #self.data_processing_task.reset(copy.deepcopy(self.original_data_processing_config))
            #self.data_processing_task.config['in'].update(ins)
            #outs = self.data_processing_task.run()
            ins = {k: batch_data[k] if k in batch_data else self.extra_data[k] for k in self.data_processing_task.config['in'].keys()}
            self.data_processing_task.config['in'].update(ins)
            outs = self.data_processing_task.run_through_graph()
            self.data_processing_task.reset(self.original_data_processing_config,replace_only_dependencies=True)
            
            out_names = self.data_processing_task.get_output_names()
            if not isinstance(out_names, list):
                out_names = [out_names]
                
            #xs = [outs['{}{}{}'.format(self.data_processing_task.name,settings.symbols['membership'],k)].load() for k in self.x_names]
            #ys = [outs['{}{}{}'.format(self.data_processing_task.name,settings.symbols['membership'],k)].load() for k in self.y_names]
            xs = [outs[out_names.index(xi)] for xi in self.x_names]
            ys = [outs[out_names.index(xi)] for xi in self.y_names]
            
            if len(ys) == 1:
                ys = ys[0]
            if len(xs) == 1:
                xs = xs[0]

            return xs, ys, step
        else:
            return batch_data[self.x_names], batch_data[self.y_names], step
    
    def __len__(self):
        return len(self._data)

class TorchGenerator(Task):
    def get_valid_parameters(self):
        return ['data'], ['extra_data','shuffle', 'batch_size', 'data_processing_task','x_names','y_names','num_workers','data_processing_mods', 'vars', 'collate_fn']

    def process(self):
        if 'vars' in self.config:
            replace_var_dollars(self.config.get('data_processing_task').config, self.config['vars'], self.config['vars'])

        dataset = TorchDataset(self.config['data'],
                               self.config.get('extra_data'),
                               self.config.get('data_processing_task'),
                               self.config.get('data_processing_mods'),
                               self.config.get('x_names','x'),
                               self.config.get('y_names','y'))
        collate_fn = self.config.get('collate_fn',None)
        if collate_fn is not None:
            if collate_fn == 'identity':
                def collate_fn(x):
                    return x
                    
        dataloader = DataLoader(dataset,
                                self.config.get('batch_size',1),
                                shuffle=self.config.get('shuffle',True),
                                num_workers=self.config.get('num_workers',0),
                                collate_fn=collate_fn)

        return dataloader
