from paips2.core import Task
import librosa
import numpy as np

class TimeFrequencyRepresentation(Task):
    def get_valid_parameters(self):
        return ['in'], ['representation', 'log', 'log_offset', 'parameters', 'delta', 'delta_delta']
    
    def get_output_names(self):
        if self.config.get('return_len') is not None:
            return ['out','len']
        else:
            return ['out']

    def process_one(self,x):
        tfr = self.config.get('representation','melspectrogram')
        apply_log = self.config.get('log',False)
        log_offset = self.config.get('log_offset',1e-16)
        tfr_params = self.config.get('parameters',{})
        if tfr == 'melspectrogram':
            y = librosa.feature.melspectrogram(x,**tfr_params)
        if apply_log:
            y = np.log(y + log_offset)
        delta = self.config.get('delta',False)
        delta_delta = self.config.get('delta_delta',False)
        if delta:
            y_delta = librosa.feature.delta(y)
        if delta_delta:
            y_delta_delta = librosa.feature.delta(y,order=2)
        if delta:
            y = np.concatenate((y,y_delta),axis=-2)
        if delta_delta:
            y = np.concatenate((y,y_delta_delta),axis=-2)

        y = y.T

        return y

    def process(self):
        x = self.config['in']
        if not isinstance(x,list):
            x = [x]
        y = [self.process_one(xi) for xi in x]

        return y

