from paips2.core import Task
import soundfile as sf
import numpy as np

class ReadAudio(Task):
    def get_valid_parameters(self):
        return ['in'], ['target_fs', 'mono', 'start', 'end', 'fixed_size']
    
    def process(self):
        x,fs = sf.read(self.config['in'])
        return x