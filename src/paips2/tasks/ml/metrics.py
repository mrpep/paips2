from paips2.core import Task
import numpy as np
import inspect

class CalculateMetrics(Task):
    def get_valid_parameters(self):
        return ['y_pred', 'y_target'], ['backend_library', 'metrics', 'labels']

    def process(self):
        preds = self.config['y_pred']
        targets = self.config['y_target']
        labels = self.config.get('labels')
        backend_library = self.config.get('backend_library','sklearn')
        metrics = self.config.get('metrics',['accuracy_score',
                                             'confusion_matrix',
                                             'f1_score',
                                             'precision_score',
                                             'recall_score',
                                             'roc_curve',
                                             'roc_auc_score',
                                             'average_precision_score',
                                             'precision_recall_curve'])
        results = {}
        if backend_library == 'sklearn':
            import sklearn.metrics as skm
            decisions = np.argmax(preds, axis=-1)
            for metric in metrics:
                m = getattr(skm, metric)
                m_signature = inspect.signature(m).parameters
                if labels is not None and 'labels' in m_signature:
                    kwargs = {'labels': labels}
                else:
                    kwargs = {}
                if 'y_pred' in m_signature:
                    if 'average' in m_signature:
                        for avg in ['micro','macro','weighted',None]:
                            if avg == None:
                                results[metric + '_perclass'] = m(targets, decisions, average=avg, **kwargs)
                            else:
                                results[metric+'_'+avg] = m(targets, decisions, average=avg, **kwargs)
                    else:
                        results[metric] = m(targets, decisions, **kwargs)
                else:
                    if metric in ['roc_curve','roc_auc_score','average_precision_score','precision_recall_curve']:
                        m_ = []
                        for c in range(preds.shape[1]):
                            targets_ = (targets == c).astype(np.int32)
                            m_.append(m(targets_, preds[:,c], **kwargs))
                        results[metric] = m_

        return results