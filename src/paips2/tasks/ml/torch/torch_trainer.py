import torch
from torch import nn
from torch.nn import functional as F
from torch.utils.data import DataLoader
from torch.utils.data import random_split
from paips2.utils import get_classes_in_module
from paips2.core import Task
import pytorch_lightning as pl
from pytorch_lightning.loggers import WandbLogger
import copy
import torchinfo

class TorchTrainer(Task):
    def get_valid_parameters(self):
        return ['data', 'loss', 'optimizer', 'model', 'training_parameters'], ['validation_data','metrics','callbacks','wandb_run','wandb_project']
    
    def process(self):
        model = self.config['model']
        torchinfo.summary(model)
        model.set_optimizer(self.config['optimizer'])
        model.set_loss(self.config['loss'])
        model.set_metrics(self.config['metrics'])

        if self.config.get('wandb_run') and self.config.get('wandb_project'):
            logger = WandbLogger(name = self.config['wandb_run'], 
                        project=self.config['wandb_project'],
                        log_model=False)
            logger.watch(model, log="all")
        else:
            logger = True

        trainer = pl.Trainer(**self.config['training_parameters'],logger=logger)
        trainer.fit(model,self.config['data'],self.config['validation_data'])
    
