from numpy.lib.arraysetops import isin
from paips2.core import Task
import numpy as np

class LabelEncoder(Task):
    def get_valid_parameters(self):
        return ['in'], ['column_in', 'column_out', 'type', 'ignore']

    def get_output_names(self):
        return ['out', 'label_to_code_map', 'code_to_label_map']

    def process(self):
        data = self.config.get('in')
        column_in = self.config.get('column_in')
        column_out = self.config.get('column_out','classID')
        encoding_type = self.config.get('type','integer')
        ignore = self.config.get('ignore',[])
        if not isinstance(ignore,list):
            ignore = [ignore]

        if encoding_type == 'integer':
            labels = data[column_in].unique()
            labels = [l for l in labels if l not in ignore]
            label_to_code_map = {l: i for i,l in enumerate(labels)}
            code_to_label_map = {i: l for i,l in enumerate(labels)}
            for l in ignore:
                label_to_code_map[l] = -1
                if (-1 in code_to_label_map) and isinstance(code_to_label_map[-1],list):
                    code_to_label_map[-1].append(l)
                else:
                    code_to_label_map[-1] = [l]

            data[column_out] = data[column_in].apply(lambda x: label_to_code_map[x])

        return data, label_to_code_map, code_to_label_map