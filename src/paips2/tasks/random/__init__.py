from paips2.core import Task

class FixSeed(Task):
    def get_output_names(self):
        return ['seed']

    def get_valid_parameters(self):
        return ['seed'], []

    def process(self):
        import random
        random.seed(self.config['seed'])
        try:
            import numpy as np
            np.random.seed(self.config['seed'])
            self.logger.info('Seeded numpy with {}'.format(self.config['seed']))
        except:
            pass
        try:
            import torch
            torch.manual_seed(self.config['seed'])
            self.logger.info('Seeded torch with {}'.format(self.config['seed']))
        except:
            pass
        try:
            from pytorch_lightning import seed_everything
            seed_everything(self.config['seed'])
            self.logger.info('Seeded pytorch_lightning with {}'.format(self.config['seed']))
        except:
            pass
        return self.config['seed']