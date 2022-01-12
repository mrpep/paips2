import torch
from torch import nn
from torch.nn import functional as F
from torch.utils.data import DataLoader
from torch.utils.data import random_split
from paips2.utils import get_classes_in_module, get_modules
from paips2.core import Task
import pytorch_lightning as pl
from pytorch_lightning.loggers import WandbLogger
from pytorch_lightning import seed_everything
import copy
import torchinfo
from pathlib import Path

class TorchTrainer(Task):
    def get_valid_parameters(self):
        return ['data', 'loss', 'optimizer', 'model', 'training_parameters'], ['validation_data','metrics','callbacks','wandb_run','wandb_project','wandb_group','wandb_config','callback_modules','loss_modules','metric_modules','scheduler']
    
    def get_output_names(self):
        return ['best_weights', 'last_model_weights', 'last_optimizer_state', 'checkpoint_path', 'logged_metrics']

    def process(self):
        model = self.config['model']
        torchinfo.summary(model)
        model.set_optimizer(self.config['optimizer'], self.config.get('scheduler'))
        model.set_loss(self.config['loss'], modules=self.config.get('loss_modules',['torch.nn']))
        model.set_metrics(self.config['metrics'], modules = self.config.get('metric_modules',['torchmetrics']))

        if self.config.get('wandb_run') and self.config.get('wandb_project'):
            logger = WandbLogger(name = self.config['wandb_run'], 
                        project=self.config['wandb_project'],
                        log_model=False,
                        group=self.config.get('wandb_group'),
                        reinit=True,
                        config=self.config.get('wandb_config'))
            logger.watch(model, log="all")
        else:
            logger = True

        callbacks = []
        callback_modules = get_modules(self.config.get('callback_modules',['pytorch_lightning.callbacks']))

        available_callbacks = {}
        for m in callback_modules:
            available_callbacks.update(get_classes_in_module(m))
        #available_callbacks = get_classes_in_module(pl.callbacks)
        callbacks_config = self.config.get('callbacks',{})
        if isinstance(callbacks_config,dict):
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
        trainer.logger.experiment.finish()
        trainer.logger.close()

        model_ckpt_cb = [c for c in trainer.callbacks if c.__class__.__name__ == 'ModelCheckpoint'][0]
        outs = []
        if model_ckpt_cb.best_model_path != '':
            outs.append(torch.load(model_ckpt_cb.best_model_path))
        outs.append(trainer.model.state_dict())
        outs.append(trainer.model.optimizers().state_dict())
        outs.append(model_ckpt_cb.dirpath)
        outs.append(model.log_history)

        return tuple(outs)    
