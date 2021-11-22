from paips2.core import Task
import soundfile as sf
import numpy as np
import pedalboard
from paips2.utils import get_classes_in_module
from pathlib import Path
import librosa

class ReadAudio(Task):
    def get_valid_parameters(self):
        return ['in'], ['target_fs', 'mono', 'start', 'end', 'fixed_size','dtype','max_size']
    
    def process(self):
        if self.config.get('start') is None:
            self.config['start'] = 0
        if self.config.get('max_size') is not None:
            audio_frames = sf.info(self.config['in']).frames
            if audio_frames > self.config['max_size'] + self.config.get('start',0):
                stop = self.config.get('start',0)+self.config['max_size']
            else:
                stop = self.config.get('end')
        else:
            stop = self.config.get('end')
        if self.config['in'].endswith('wav'):
            x,fs = sf.read(self.config['in'],start=self.config.get('start',0),stop=stop)
        else:
            if (self.config.get('start') is not None) and (stop is not None):
                start = self.config.get('start',0)
                start = start/self.config.get('target_fs',44100)
                duration = stop-self.config.get('start',0)
                duration = duration/self.config.get('target_fs',44100)
            else:
                duration = None
                start = 0
            x,fs = librosa.core.load(self.config['in'],sr=self.config.get('target_fs',None),mono=self.config.get('mono',True),offset=start,duration=duration)
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