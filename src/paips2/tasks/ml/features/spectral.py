from paips2.core import Task
import librosa
import numpy as np

class TimeFrequencyRepresentation(Task):
    def get_valid_parameters(self):
        return ['in'], ['representation', 'log', 'log_offset', 'parameters']
    
    def process(self):
        tfr = self.config.get('representation','melspectrogram')
        apply_log = self.config.get('log',False)
        log_offset = self.config.get('log_offset',1e-16)
        tfr_params = self.config.get('parameters',{})
        x = self.config.get('in',None)
        if tfr == 'melspectrogram':
            y = librosa.feature.melspectrogram(x,**tfr_params)
        if apply_log:
            y = np.log(y + log_offset)
        return y

