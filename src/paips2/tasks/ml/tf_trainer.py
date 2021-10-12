import tensorflow as tf
from tensorflow.python.ops.clip_ops import clip_by_norm
from paips2.core import Task

class TFTrainer(Task):
    def get_valid_parameters(self):
        return ['data', 'loss', 'optimizer', 'model'], ['epochs', 'lr', 'validation_data']
    
    def process(self):
        model = self.config['model']
        model.summary()
        model.compile(loss=self.config['loss'],
                      optimizer=tf.keras.optimizers.Adam(learning_rate=0.0001,clipnorm=1.0),
                      metrics=[tf.keras.metrics.SparseCategoricalAccuracy()])
        model.fit(self.config['data'],validation_data=self.config.get('validation_data'),epochs=self.config.get('epochs',1))
