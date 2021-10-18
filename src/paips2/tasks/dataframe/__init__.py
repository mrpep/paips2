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

            if isinstance(dataframe,pd.core.series.Series):
                dataframe = pd.DataFrame(dataframe).T
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

class DataframeColumnSelect(Task):
    def get_valid_parameters(self):
        return ['in','column'],[]

    def process(self):
        return self.config['in'][self.config['column']]

class DataframeRandomColumn(Task):
    def get_valid_parameters(self):
        return ['in', 'column', 'proportions'], []

    def process(self):
        data = self.config['in']
        labels = list(self.config['proportions'].keys())
        sampled_idxs = []
        for l in labels[:-1]:
            idxs = data.sample(frac=self.config['proportions'][l]).index
            sampled_idxs.extend(idxs)
            data.loc[idxs,self.config['column']] = l
        data.loc[~data.index.isin(sampled_idxs), self.config['column']] = labels[-1]

        return data

class DataframeFramer(Task):
    def get_valid_parameters(self):
        return ['in','frame_size'], ['hop_size','duration_column']
    
    def process(self):
        data = self.config['in']
        if self.config['frame_size'] is None:
            return data
        else:
            if isinstance(self.config['frame_size'],list):
                frame_size = self.config['frame_size'][0]
            else:
                frame_size = self.config['frame_size']
            if self.config.get('hop_size') is None:
                hop_size = frame_size
            else:
                if isinstance(self.config['hop_size'],list):
                    hop_size = self.config['hop_size'][0]
                else:
                    hop_size = self.config['hop_size']
            duration_column = self.config.get('duration_column', 'frames')

            framed_rows = []
            for logid,row in tqdm(data.iterrows()):
                max_frames = row[duration_column]
                starts = np.arange(0,max_frames-frame_size,hop_size)
                ends = starts + frame_size
                row_dict = row.to_dict()
                row_dict['start'], row_dict['end'], row_dict['parent_id'] = starts, ends, logid
                df_i = pd.DataFrame(row_dict)
                framed_rows.append(df_i)
            framed_df = pd.concat(framed_rows)
            framed_df = framed_df.reset_index()
            framed_df = framed_df.rename(columns={'index': 'frame_index'})
        return framed_df