from paips2.core import Task
import soundfile as sf
import numpy as np
import pedalboard
from paips2.utils import get_classes_in_module
from pathlib import Path

class ReadAudio(Task):
    def get_valid_parameters(self):
        return ['in'], ['target_fs', 'mono', 'start', 'end', 'fixed_size','dtype']
    
    def process(self):
        x,fs = sf.read(self.config['in'],start=self.config.get('start',0),stop=self.config.get('end'))
        fixed_size = self.config.get('fixed_size')
        if isinstance(fixed_size,list):
            fixed_size = fixed_size[0]
        if fixed_size is not None:
            if len(x) < fixed_size:
                x = np.pad(x,(0,fixed_size - len(x)))
            else:
                x = x[:fixed_size]
        dtype = self.config.get('dtype','float32')
        return x.astype(dtype)

class Pedalboard(Task):
    def get_valid_parameters(self):
        return ['in', 'pedals'], ['sr','probability']
    
    def process(self):
        p = self.config.get('probability',1.0)
        wav = self.config['in']
        sr = self.config.get('sr',44100)
        p_ = np.random.uniform(low=0,high=1)
        if p_ <= p:
            available_effects = get_classes_in_module(pedalboard)
            pedals_config = self.config['pedals']
            pedals_config = [{list(p.keys())[0]: {k: v if k != 'mode' else getattr(available_effects[list(p.keys())[0]].Mode, v) for k,v in p[list(p.keys())[0]].items()}} for p in pedals_config]
            effects_pedalboard = pedalboard.Pedalboard([available_effects[list(p.keys())[0]](**p[list(p.keys())[0]]) for p in pedals_config], sample_rate=sr)
            return effects_pedalboard(wav)
        else:
            return wav

    def on_export(self,outs):
        sf.write(str(Path(self.export_path,'out.wav').absolute()),outs[self.name + '->out'].load(),samplerate=self.config.get('sr',44100))