from paips2.core import Task
import soundfile as sf
import numpy as np
import pedalboard
from paips2.utils import get_classes_in_module
from pathlib import Path

class ReadAudio(Task):
    def get_valid_parameters(self):
        return ['in'], ['target_fs', 'mono', 'start', 'end', 'fixed_size']
    
    def process(self):
        x,fs = sf.read(self.config['in'])
        fixed_size = self.config.get('fixed_size')
        if fixed_size is not None:
            if len(x) < fixed_size:
                x = np.pad(x,(0,fixed_size - len(x)))
            else:
                x = x[:fixed_size]
        return x

class Pedalboard(Task):
    def get_valid_parameters(self):
        return ['in', 'pedals'], ['sr']
    
    def process(self):
        wav = self.config['in']
        sr = self.config.get('sr',44100)
        available_effects = get_classes_in_module(pedalboard)
        pedals_config = self.config['pedals']
        pedals_config = [{list(p.keys())[0]: {k: v if k != 'mode' else getattr(available_effects[list(p.keys())[0]].Mode, v) for k,v in p[list(p.keys())[0]].items()}} for p in pedals_config]
        effects_pedalboard = pedalboard.Pedalboard([available_effects[list(p.keys())[0]](**p[list(p.keys())[0]]) for p in pedals_config], sample_rate=sr)
        return effects_pedalboard(wav)

    def on_export(self,outs):
        sf.write(str(Path(self.export_path,'out.wav').absolute()),outs[self.name + '->out'].load(),samplerate=self.config.get('sr',44100))