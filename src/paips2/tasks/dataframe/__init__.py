from paips2.core import Task, TaskIO
import numpy as np
import pandas as pd
from tqdm import tqdm
import copy
tqdm.pandas()

class DataframeApply(Task):
    def get_valid_parameters(self):
        return ['in', 'column_in','column_out','processing_task'], ['h5_file']
    
    def process(self):
        dataframe = self.config['in']
        column_in = self.config['column_in']
        column_out = self.config['column_out']
        processing_task = self.config['processing_task']
        processing_task.logger = None
        processing_task.make_dag()
        output_names = processing_task.get_output_names()
        h5file = self.config.get('h5_file')
        if h5file is not None:
            import h5py
            f = h5py.File(h5file,'w')
            def apply_graph_h5(row):
                outs = []
                for k,v in column_in.items():
                    processing_task.config['in'][k] = TaskIO(row[v],'0')
                for out_name, out in zip(output_names,processing_task.run_through_graph()):
                    h5_key = '{}/{}'.format(row.name,out_name)
                    f[h5_key] = out
                    outs.append(h5_key)
                return tuple(outs)
            dataframe[[column_out[col] for col in output_names]] = dataframe.progress_apply(apply_graph_h5,axis=1,result_type='expand')
        else:
            def apply_graph_df(row):
                outs = []
                processing_task.reset()
                for k,v in column_in.items():
                    processing_task.config['in'][k] = TaskIO(row[v],'0')
                return processing_task.run_through_graph()

            if self.config.get('log',True):
                dataframe[[column_out[col] for col in output_names]] = dataframe.progress_apply(apply_graph_df,axis=1,result_type='expand')
            else:
                dataframe[[column_out[col] for col in output_names]] = dataframe.apply(apply_graph_df,axis=1,result_type='expand')
        return dataframe

class DataframeFilterByColumn(Task):
    def get_valid_parameters(self):
        return ['in', 'column'], []
    
    def get_output_names(self):
        return list(self.config.get('in')[self.config.get('column')].unique())

    def process(self):
        df = self.config.get('in')
        col = self.config.get('column')
        dfs = [df.loc[df[col] == col_val] for col_val in self.get_output_names()]

        return tuple(dfs)

class DataframeMelt(Task):
    def get_valid_parameters(self):
        return ['in','var_name','value_name'], ['columns','exclude_columns']

    def process(self):
        df = self.config.get('in')
        if self.config.get('exclude_columns'):
            columns = [col for col in df.columns if col not in self.config.get('exclude_columns')]
        else:
            columns = self.config.get('columns')
        return df.melt(columns,var_name=self.config.get('var_name'),value_name=self.config.get('value_name'))

class ColumnToNumpy(Task):
    def get_valid_parameters(self):
        return ['in','column'], []
    def process(self):
        return np.stack(self.config['in'][self.config['column']])
