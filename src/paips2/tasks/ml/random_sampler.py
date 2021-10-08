from paips2.core import Task
import numpy as np
import re

class RandomSampler(Task):
    def get_valid_parameters(self):
        return ['sample'], []

    def get_output_names(self):
        return list(self.config['sample'].keys())

    def process(self):
        sampled = []
        for k in self.get_output_names():
            spec = self.config['sample'][k]
            spec = re.match('(?P<dist>.*?)\((?P<params>.*?)\)',spec).groupdict()
            if spec['dist'] == 'uniform':
                low, high = spec['params'].split(',')
                sampled.append(np.random.uniform(low=float(low),high=float(high)))
        return tuple(sampled)