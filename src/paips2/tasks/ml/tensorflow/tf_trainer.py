import tensorflow as tf
import tensorflow.keras.optimizers as tfo
import tensorflow.keras.losses as tfl
import tensorflow.keras.metrics as tfm
import tensorflow.keras.callbacks as tfc

from paips2.core import Task
from paips2.utils import get_classes_in_module

class TFTrainer(Task):
    def get_valid_parameters(self):
        return ['data', 'loss', 'optimizer', 'model'], ['epochs', 'validation_data','metrics','callbacks']
    
    def process(self):
        model = self.config['model']
        model.summary()

        loss_config = self.config['loss']
        if isinstance(loss_config,str):
            loss = loss_config
        else:
            available_losses = get_classes_in_module(tfl)
            loss = available_losses[loss_config.pop('type')]
            loss = loss(**loss_config)

        optimizer_config = self.config['optimizer']
        if isinstance(optimizer_config,str):
            optimizer = optimizer_config
        else:
            available_opts = get_classes_in_module(tfo)  
            optimizer = available_opts[optimizer_config.pop('type')]
            optimizer = optimizer(**optimizer_config)
        
        metrics_config = self.config.get('metrics',[])
        metrics = []
        available_metrics = get_classes_in_module(tfm)
        for m in metrics_config:
            if isinstance(m,str):
                metrics.append(m)
            else:
                m_i = available_metrics[m.pop('type')]
                metrics.append(m_i(**m))

        callbacks_config = self.config.get('callbacks',[])
        callbacks = []
        for c in callbacks_config:
            if isinstance(c,str):
                callbacks.append(c)
            else:
                c_i = available_metrics[c.pop('type')]
                callbacks.append(c_i(**c))
        
        epochs = self.config.get('epochs',1)
        model.compile(loss=loss,
                      optimizer=optimizer,
                      metrics=metrics)
        model.fit(self.config['data'],
                  validation_data=self.config.get('validation_data'),
                  epochs=epochs,
                  callbacks=callbacks)
