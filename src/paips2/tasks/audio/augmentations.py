from paips2.core import Task
from pathlib import Path
import random
import pedalboard
import soundfile as sf
import numpy as np

class AudioReverb(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ir_files = sum([list(Path(f).rglob('*.wav')) for f in self.config['conv_params']['ir_folders']],[])

    def get_valid_parameters(self):
        return ['prob', 'room_prob', 'conv_prob', 'conv_params', 'room_params'], ['fs']

    def process_one(self, x):
        dice = random.uniform(0,1)
        if dice < self.config['prob']:
            reverb_type = random.choices(['room','conv'],weights=[self.config['room_prob'], self.config['conv_prob']])[0]
            if reverb_type == 'conv':
                ir_file = random.choice(self.ir_files)
                conv_mix = random.uniform(*self.config['conv_params']['mix'])
                pedal = pedalboard.Convolution(str(ir_file.resolve()), mix=conv_mix)
            elif reverb_type == 'room':
                rs = random.uniform(*self.config['room_params']['room_size'])
                damp = random.uniform(*self.config['room_params']['damping'])
                wet = random.uniform(*self.config['room_params']['wet_level'])
                pedal = pedalboard.Reverb(room_size=rs,
                                          damping = damp,
                                          wet_level = wet)
            return pedal(x, self.config.get('fs', 16000))
        else:
            return x
    
    def process(self):
        x = self.config['in']
        if not isinstance(x,list):
            x = [x]
        y = [self.process_one(xi) for xi in x]

        return y

class AudioCompressor(Task):
    def get_valid_parameters(self):
        return ['prob','threshold_db','ratio'], ['fs']

    def process_one(self,x):
        dice = random.uniform(0,1)
        if dice < self.config['prob']:
            th = random.uniform(*self.config['threshold_db'])
            r = random.uniform(*self.config['ratio'])
            pedal = pedalboard.Compressor(threshold_db = th, ratio = r, attack_ms=1.0, release_ms=100)
            return pedal(x, self.config.get('fs', 16000))
        else:
            return x

    def process(self):
        x = self.config['in']
        if not isinstance(x,list):
            x = [x]
        y = [self.process_one(xi) for xi in x]

        return y

class AudioAdditiveNoise(Task):
    def __init__(self,*args, **kwargs):
        super().__init__(*args, **kwargs)
        self.noise_files = sum([list(Path(f).rglob('*.wav')) for f in self.config['noise_folders']],[])

    def get_valid_parameters(self):
        return ['noise_folders', 'amplitude', 'prob'], ['fs']

    def process_one(self,x):
        dice = random.uniform(0,1)
        if dice < self.config['prob']:
            noise_file = random.choice(self.noise_files)
            noise_x, noise_fs = sf.read(noise_file)
            noise_amp = random.uniform(*self.config['amplitude'])
            if noise_x.ndim > 1:
                noise_x = np.mean(noise_x,axis=1)
            if noise_x.shape[0] > x.shape[0]:
                noise_x = noise_x[:x.shape[0]]
                x += noise_x*noise_amp
            elif noise_x.shape[0] < x.shape[0]:
                start = random.randint(0,x.shape[0] - noise_x.shape[0])
                x[start:start+noise_x.shape[0]] += noise_x*noise_amp
            return x
        else:
            return x

    def process(self):
        x = self.config['in']
        if not isinstance(x,list):
            x = [x]
        y = [self.process_one(xi) for xi in x]

        return y
        
class AudioFilter(Task):
    def get_valid_parameters(self):
        return ['lp_prob','lp_freq','hp_prob','hp_freq'], ['fs']

    def process_one(self,x):
        lp_dice = random.uniform(0,1)
        if lp_dice < self.config['lp_prob']:
            lpf = random.uniform(*self.config['lp_freq'])
            pedal = pedalboard.LowpassFilter(lpf)
            x = pedal(x, self.config.get('fs', 16000))
        hp_dice = random.uniform(0,1)
        if hp_dice < self.config['hp_prob']:
            hpf = random.uniform(*self.config['hp_freq'])
            pedal = pedalboard.HighpassFilter(hpf)
            x = pedal(x, self.config.get('fs', 16000))
        return x

    def process(self):
        x = self.config['in']
        if not isinstance(x,list):
            x = [x]
        y = [self.process_one(xi) for xi in x]

        return y