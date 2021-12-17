from paips2.core import Task
import numpy as np

class LabelEncoder(Task):
    def get_valid_parameters(self):
        return ['in'], ['column_in', 'column_out', 'type', 'ignore', 'ignore_float']

    def get_output_names(self):
        return ['out', 'label_to_code_map', 'code_to_label_map', 'n_classes']

    def process(self):
        data = self.config.get('in')
        column_in = self.config.get('column_in')
        column_out = self.config.get('column_out','classID')
        encoding_type = self.config.get('type','integer')
        ignore = self.config.get('ignore',[])
        ignore_float = self.config.get('ignore_float',True)
        def is_float(x):
            try:
                y = float(x)
                return True
            except:
                return False
        if not isinstance(ignore,list):
            ignore = [ignore]
        if encoding_type == 'integer':
            labels = data[column_in].loc[data[column_in].apply(lambda x: isinstance(x,str))].unique()
            labels = [l for l in labels if l not in ignore]
            label_to_code_map = {l: i for i,l in enumerate(labels)}
            code_to_label_map = {i: l for i,l in enumerate(labels)}
            for l in ignore:
                label_to_code_map[l] = -1
                if (-1 in code_to_label_map) and isinstance(code_to_label_map[-1],list):
                    code_to_label_map[-1].append(l)
                else:
                    code_to_label_map[-1] = [l]

            def encode(x):
                if ignore_float and is_float(x):
                    return float(x)
                elif isinstance(x,str):
                    return label_to_code_map[x]
                elif isinstance(x,dict):
                    return {label_to_code_map[k]:v for k,v in x.items()}
                else:
                    raise Exception('Unknown type: {}'.format(type(x)))

            data[column_out] = data[column_in].apply(encode)
        return data, label_to_code_map, code_to_label_map, len(label_to_code_map)

class OneHotVector(Task):
    """
    in: can be an integer (position where the hot vector will be 1), float (if continous_values is True) or dictionary (k integer, v float for soft labels)
    n_classes: number of classes, which will be the length of the one hot vector
    column_in: name of the column where the input is stored
    continuous_values(default=False): if True, an extra element is appended to the hot vector, which is a float number (this is for mixed classification/regression)
    """
    def get_valid_parameters(self):
        return ['in','n_classes'], ['continuous_values','column_in']

    def get_output_names(self):
        return ['out']

    def process(self):
        data = self.config['in']
        column_in = self.config.get('column_in')
        n_classes = self.config['n_classes']
        if self.config.get('continuous_values',False):
            y = np.zeros((n_classes+1,))
        else:
            y = np.zeros((n_classes,))
        if isinstance(data,dict):
            for k,v in data.items():
                y[int(k)] = v
        else:
            try:
                if isinstance(data,int):
                    y[data] = 1.0
                elif (isinstance(data,float)) and (self.config.get('continuous_values',False)):
                    y[-1] = data
            except:
                from IPython import embed; embed()
        return y