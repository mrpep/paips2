from .torch_generator import TorchGenerator
from .torch_trainer import TorchTrainer
from .torch_predictor import TorchPredictor

from paips2.core import Task
from pathlib import Path

import re

class TorchSWAModel(Task):
    def get_output_names(self):
        return ['out']

    def get_valid_parameters(self):
        return ['checkpoint_dir'], ['n_checkpoints','from_epoch','to_epoch']

    def process(self):
        available_ckpts = [str(ckpt) for ckpt in Path(self.config['checkpoint_dir']).glob('*.ckpt')]
        #order checkpoints by epoch
        available_ckpts = sorted(available_ckpts, key=lambda x: int(re.search(r'epoch=(\d+)', x).group(1)))
        last_ckpts = available_ckpts[-self.config['n_checkpoints']:]

        swa_weights = {}
        for ckpt in last_ckpts:
            weights = torchml.load(ckpt)['state_dict']
            for k,v in weights.items():
                if k not in swa_weights:
                    swa_weights[k] = v/len(last_ckpts)
                else:
                    swa_weights[k] += v/len(last_ckpts)
        
        return swa_weights