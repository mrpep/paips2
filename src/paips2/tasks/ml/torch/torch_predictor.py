import torch
from torch import nn
from torch.nn import functional as F
from torch.utils.data import DataLoader
from torch.utils.data import random_split
from paips2.utils import get_classes_in_module, get_modules
from paips2.core import Task
import pytorch_lightning as pl
from pytorch_lightning.loggers import WandbLogger
import copy
import torchinfo
from pathlib import Path

class TorchPredictor(Task):
    def get_valid_parameters(self):
        return ['data', 'model', 'model_weights'], ['device']
    
    def get_output_names(self):
        return ['predictions', 'targets']

    def process(self):
        data = self.config['data']
        model = self.config['model']
        model_weights = self.config['model_weights']
        model.load_state_dict(model_weights)
        preds = []
        targets = []
        with torch.no_grad():
            for x, target in data:
                if not isinstance(x,(list,tuple)):
                    x = [x]
                x = [x_i.to(model.device, dtype=model.dtype) for x_i in x if isinstance(x_i, torch.Tensor)]
                preds.append(model(*x))
                targets.append(target)
            preds = torch.cat(preds)
            targets = torch.cat(targets)
        
        return preds.detach().to('cpu').numpy(), targets.detach().to('cpu').numpy()