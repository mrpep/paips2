from paips2.core import Task
import numpy as np
import pandas as pd
from pathlib import Path
import joblib
import torch

def moving_average(a, n=3) :
    ret = np.cumsum(a, dtype=float)
    ret[n:] = ret[n:] - ret[:-n]
    return ret[n - 1:] / n

class CheckpointFilter(Task):
    def get_valid_parameters(self):
        return ['checkpoints_path', 'training_history', 'monitor_metrics'], ['scoring_criteria', 'swa_checkpoints']

    def get_output_names(self):
        return ['out']

    def process(self):
        training_history = pd.concat(self.config['training_history'])
        
        subsets = []
        for elem in self.config['monitor_metrics']:
            subset_training_history = training_history
            for k,v in elem.items():
                subset_training_history = subset_training_history.loc[subset_training_history[k] == v]
            subsets.append(subset_training_history)
        metric_subsets = pd.concat(subsets)
        score_criteria = self.config.get('scoring_criteria','max')
        aggregated_vals = metric_subsets.groupby('epoch').sum()['value']

        swa = self.config.get('swa_checkpoints',1)
        if swa>1:
            aggregated_vals = aggregated_vals.sort_index().values
            aggregated_vals = moving_average(aggregated_vals, swa)

        if score_criteria == 'max':
            if swa>1:
                best_epoch = np.argmax(aggregated_vals) + swa//2
            else:
                best_epoch = aggregated_vals.idxmax()
        elif score_criteria == 'min':
            if swa>1:
                best_epoch = np.argmin(aggregated_vals) + swa//2
            else:
                best_epoch = aggregated_vals.idxmin()
        
        checkpoints = list(Path(self.config['checkpoints_path']).rglob('*'))
        
        def get_epoch_from_filename(x):
            parts = x.stem.split('=')
            return int(parts[parts.index('epoch') + 1])

        ckpts = {get_epoch_from_filename(x):x for x in checkpoints}

        if swa>1:
            upper_lim = min(len(ckpts), best_epoch + swa//2)
            lower_lim = upper_lim - swa
            swa_ckpts = [torch.load(v) for k,v in ckpts.items() if k>=lower_lim and k<upper_lim]
            swa_weights = {}
            for ckpt in swa_ckpts:
                weights = ckpt['state_dict']
                for k,v in weights.items():
                    if k not in swa_weights:
                        swa_weights[k] = v/len(swa_ckpts)
                    else:
                        swa_weights[k] += v/len(swa_ckpts)
            swa_ckpts[0]['state_dict'] = swa_weights
            return swa_ckpts[0]
        else:
            return torch.load(ckpts[best_epoch])