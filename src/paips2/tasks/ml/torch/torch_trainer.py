
import torch
from torch import nn
from torch.nn import functional as F
from torch.utils.data import DataLoader
from torch.utils.data import random_split
from paips2.utils import get_classes_in_module
from paips2.core import Task
import pytorch_lightning as pl
import copy

import torchinfo

class TorchTrainer(Task):
    def get_valid_parameters(self):
        return ['data', 'loss', 'optimizer', 'model', 'training_parameters'], ['validation_data','metrics','callbacks']
    
    def process(self):
        model = self.config['model']
        torchinfo.summary(model)
        model.set_optimizer(self.config['optimizer'])
        model.set_loss(self.config['loss'])
        model.set_metrics(self.config['metrics'])

        trainer = pl.Trainer(**self.config['training_parameters'])
        trainer.fit(model,self.config['data'],self.config['validation_data'])
        

    
