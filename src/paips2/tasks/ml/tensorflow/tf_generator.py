import tensorflow as tf
from paips2.core import Task, TaskIO,settings
import numpy as np
import copy

class gen(tf.keras.utils.Sequence):
    def __init__(self, data = None, shuffle = True, batch_size = 32, data_processing_task = None,x_names=None,y_names=None):
        self._data, self.shuffle, self.batch_size, self.data_processing_task = data, shuffle, batch_size, data_processing_task
        if self._data is not None:
            self._index = np.array(self._data.index)
            if self.shuffle:
                self._index = np.random.permutation(self._index)
        if self.data_processing_task is not None:
            self.data_processing_task.in_memory = True
            self.data_processing_task.logger = None
            self.original_data_processing_config = copy.deepcopy(self.data_processing_task.config)
        self.x_names, self.y_names = x_names, y_names
        if not isinstance(self.x_names,list):
            self.x_names = [self.x_names]  
        if not isinstance(self.y_names,list):
            self.y_names = [self.y_names]

    def __getitem__(self,step):
        batch_idxs = np.take(self._index,np.arange(int(step*self.batch_size),int((step+1)*self.batch_size)),mode='wrap')
        batch_data = self._data.loc[batch_idxs]
        if self.data_processing_task is not None:
            ins = {'data': TaskIO(batch_data,'0',name='batch_data',storage_device='memory')}
            self.data_processing_task.reset(copy.deepcopy(self.original_data_processing_config))
            self.data_processing_task.config['in'].update(ins)
            outs = self.data_processing_task.run()
            out_names = self.data_processing_task.get_output_names()
            if not isinstance(out_names, list):
                out_names = [out_names]
            xs = [outs['{}{}{}'.format(self.data_processing_task.name,settings.symbols['membership'],k)].load() for k in self.x_names]
            ys = [outs['{}{}{}'.format(self.data_processing_task.name,settings.symbols['membership'],k)].load() for k in self.y_names]
            if len(ys) == 1:
                ys = ys[0]
            if len(xs) == 1:
                xs = xs[0]
            #print('Nans: {}, Range: [{},{}]'.format(np.isnan(xs).sum(),np.min(xs),np.max(xs)))
            #print(ys)
            return xs, ys
    
    def __len__(self):
        return len(self._index)//self.batch_size + 1


class TFGenerator(Task):
    def get_valid_parameters(self):
        return [], ['data', 'shuffle', 'batch_size', 'data_processing_task']

    def process(self):
        data, shuffle, batch_size, data_processing_task = self.config.get('data'), self.config.get('shuffle',True), self.config.get('batch_size',32), self.config.get('data_processing_task')
        x_names, y_names = self.config.get('x_names','x'), self.config.get('y_names','y')
        return gen(data,shuffle,batch_size,data_processing_task,x_names=x_names,y_names=y_names)
