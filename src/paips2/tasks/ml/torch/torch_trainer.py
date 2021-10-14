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

class TorchTrainer(Task):
    def get_valid_parameters(self):
        return ['data', 'loss', 'optimizer', 'model', 'training_parameters'], ['validation_data','metrics','callbacks','wandb_run','wandb_project','callback_modules','loss_modules']
    
    def process(self):
        model = self.config['model']
        torchinfo.summary(model)
        model.set_optimizer(self.config['optimizer'])
        model.set_loss(self.config['loss'], modules=self.config.get('loss_modules',['torch.nn']))
        model.set_metrics(self.config['metrics'])

        if self.config.get('wandb_run') and self.config.get('wandb_project'):
            logger = WandbLogger(name = self.config['wandb_run'], 
                        project=self.config['wandb_project'],
                        log_model=False)
            logger.watch(model, log="all")
        else:
            logger = True

        callbacks = []
        callback_modules = get_modules(self.config.get('callback_modules',['pytorch_lightning.callbacks']))

        available_callbacks = {}
        for m in callback_modules:
            available_callbacks.update(get_classes_in_module(m))
        #available_callbacks = get_classes_in_module(pl.callbacks)
        for k,v in self.config.get('callbacks',{}).items():
            if k == 'ModelCheckpoint':
                if 'dirpath' not in v:
                    v['dirpath'] = str(Path(self.cache_path,self.get_hash(),'checkpoints').absolute())
                if 'filename' not in v:
                    if 'monitor' in v:
                        v['filename'] = '{epoch}-{' + v['monitor'] + ':.2f}'
                    else:
                        v['filename'] = '{epoch}'
            callbacks.append(available_callbacks[k](**v))

        trainer = pl.Trainer(**self.config['training_parameters'],logger=logger,callbacks=callbacks)
        trainer.fit(model,self.config['data'],self.config['validation_data'])
    
