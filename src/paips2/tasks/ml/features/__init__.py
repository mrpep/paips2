from .spectral import *
from paips2.core import Task
import numpy as np

class Normalize(Task):
    def get_valid_parameters(self):
        return ['in'], ['mvn', 'mean', 'std']

    def process_one(self,x):
        if self.config.get('mvn',False):
            x = (x - np.mean(x,axis=-1,keepdims=True))/np.std(x,axis=-1,keepdims=True)
        x = (x - self.config.get('mean',0))/self.config.get('std',1)
        return x

    def process(self):
        x = self.config['in']
        if not isinstance(x,list):
            x = [x]
        y = [self.process_one(xi) for xi in x]

        return y

class CutPad(Task):
    def get_valid_parameters(self):
        return ['in'], ['fixed_len','max_len']

    def process_one(self, x):
        if self.config.get('fixed_len') is not None:
            if x.shape[-1] > self.config.get('fixed_len'):
                i = np.random.randint(0,x.shape[-1] - self.config.get('fixed_len'))
                x = x[:,i:i+self.config.get('fixed_len')]
            else:
                x = np.pad(x,((0,0),(0,self.config.get('fixed_len')-x.shape[-1])),'constant')
        elif self.config.get('max_len') is not None:
            if x.shape[-1] > self.config.get('max_len'):
                i = np.random.randint(0,x.shape[-1] - self.config.get('max_len'))
                x = x[:,i:i+self.config.get('max_len')]    

        return x

    def process(self):
        x = self.config['in']
        if not isinstance(x,list):
            x = [x]
        y = [self.process_one(xi) for xi in x]

        return y    