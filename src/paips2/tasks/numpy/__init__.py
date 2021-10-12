from paips2.core import Task
import numpy as np

class NPSqueeze(Task):
    def get_valid_parameters(self):
        return ['in'], ['axis']
    def process(self):
        return np.squeeze(self.config['in'],axis=self.config.get('axis'))