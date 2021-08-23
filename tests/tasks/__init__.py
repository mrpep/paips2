from paips2.core import Task
import numpy as np
import time

class GenerateRandomData(Task):
    def process(self):
        np.random.seed(self.config.get('seed',1234))
        return np.random.uniform(size=(self.config['len'],))
    def get_valid_parameters(self):
        return ['len'], ['seed']

class Add(Task):
    def process(self):
        result = 0
        for a_i in self.config['addends']:
            result += a_i
        return result
    def get_valid_parameters(self):
        return ['addends'], []

class Delay(Task):
    def process(self):
        time.sleep(self.config['delay'])
        return self.config.get('in')
    def get_valid_parameters(self):
        return ['delay'], []