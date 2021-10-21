from paips2.core import Task
import numpy as np

class NPSqueeze(Task):
    def get_valid_parameters(self):
        return ['in'], ['axis']
    def process(self):
        return np.squeeze(self.config['in'],axis=self.config.get('axis'))

class ToArray(Task):
    def get_valid_parameters(self):
        return ['in'], []

    def process(self):
        return np.array(self.config['in'])

class Pad(Task):
    def get_valid_parameters(self):
        return ['in', 'target_size'], ['pad_value','pad_axis']

    def get_output_names(self):
        return ['out', 'unpadded_len']

    def process(self):
        pad_axis = self.config.get('pad_axis',0)
        data = self.config['in']
        if data.shape[pad_axis] < self.config['target_size']:
            pad_frames = self.config['target_size'] - data.shape[pad_axis]
            unpadded_len = data.shape[pad_axis]
            data_shape = list(data.shape)
            data_shape[pad_axis] = pad_frames
            pad_data = np.zeros(data_shape)
            y = np.concatenate([data, pad_data],axis=pad_axis)
        else:
            y = data
            unpadded_len = self.config['target_size']
        return y, unpadded_len
    
