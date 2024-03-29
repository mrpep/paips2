from paips2.core import Task, TaskIO
import numpy as np
import pandas as pd
from tqdm import tqdm
import copy

from paips2.core import Graph
tqdm.pandas()
from paips2.core.settings import common_optional_params, common_required_params
from kahnfigh import Config

class DataframeApply(Task):
    def get_valid_parameters(self):
        return ['in', 'column_in','column_out','processing_task'], ['h5_file']
    
    def process(self):
        dataframe = self.config['in']
        column_in = self.config['column_in']
        column_out = self.config['column_out']
        processing_task = self.config['processing_task']
        if isinstance(processing_task,str):
            graph_config = Config(processing_task)
            graph_name = list(graph_config.keys())[0]
            graph_config = graph_config[graph_name]
            processing_task = Graph(graph_config,graph_name,self.logger,self.global_flags)
            processing_task.calculate_hashes = False
        processing_task.logger = None
        processing_task.make_dag()
        output_names = processing_task.get_output_names()
        h5file = self.config.get('h5_file')
        if h5file is not None:
            import h5py
            f = h5py.File(h5file,'w')
            def apply_graph_h5(row):
                outs = []
                processing_task.reset(copy.deepcopy(processing_task.config))
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
                processing_task.reset(copy.deepcopy(processing_task.config))
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

class DataframeFilter(Task):
    def get_valid_parameters(self):
        return ['in', 'column'], ['column_value','column_exclude_value']

    def process(self):
        df = self.config.get('in')
        column_in = self.config.get('column')
        column_value = self.config.get('column_value')
        column_exclude_value = self.config.get('column_exclude_value')
        if column_value is not None:
            if isinstance(column_value,str):
                column_value = [column_value]
            df = df.loc[df[column_in].isin(column_value)]
        
        if column_exclude_value is not None:
            if isinstance(column_exclude_value,str):
                column_exclude_value = [column_exclude_value]
            df = df.loc[~df[column_in].isin(column_exclude_value)]

        return df
    
class DataframeFilterByColumn(Task):
    #Returns dataframes with unique values in the column
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
    #Performs a pandas.melt on a dataframe
    def get_valid_parameters(self):
        return ['in','var_name','value_name'], ['columns','exclude_columns']

    def process(self):
        df = self.config.get('in')
        if self.config.get('exclude_columns'):
            columns = [col for col in df.columns if col not in self.config.get('exclude_columns')]
        else:
            columns = self.config.get('columns')
        return df.melt(columns,var_name=self.config.get('var_name'),value_name=self.config.get('value_name'))

class DataframeMerge(Task):
    def get_valid_parameters(self):
        return ['left','right'], ['how','on','left_on','right_on','left_index','right_index']

    def process(self):
        config = copy.deepcopy(self.config)
        left = config.pop('left')
        right = config.pop('right')
        for p in common_required_params + common_optional_params:
            if p in config:
                config.pop(p)
        return pd.merge(left,right,**config)

class DataframeSumCols(Task):
    def get_valid_parameters(self):
        return ['in','summands','output_col'], []

    def process(self):
        df = self.config.get('in')
        df[self.config['output_col']] = df[self.config['summands']].sum(axis=1)
        cols = [col for col in self.config['summands'] if col != self.config['output_col']]
        df = df.drop(cols,axis=1)
        return df

class ColumnToNumpy(Task):
    def get_valid_parameters(self):
        return ['in','column'], []
    def process(self):
        return np.stack(self.config['in'][self.config['column']])

class DataframeConcatenate(Task):
    def get_valid_parameters(self):
        return ['in'], ['axis']

    def process(self):
        if isinstance(self.config['in'],list) and len(self.config['in']) > 1:
            to_concat = []
            for l in self.config['in']:
                if isinstance(l,list):
                    for l_i in l:
                        to_concat.append(l_i)
                else:
                    to_concat.append(l)
            return pd.concat(to_concat, axis=self.config.get('axis',0))
        elif isinstance(self.config['in'],list) and len(self.config['in']) == 1:
            return self.config['in'][0]
        else:
            return self.config['in']

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
            frame_size = int(frame_size)
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
                max_frames = int(row[duration_column])
                starts = np.arange(0,max_frames-frame_size,hop_size)
                ends = starts + frame_size
                row_dict = row.to_dict()
                row_dict['start'], row_dict['end'], row_dict['parent_id'] = starts, ends, logid
                try:
                    df_i = pd.DataFrame(row_dict)
                except:
                    from IPython import embed; embed()
                framed_rows.append(df_i)
            framed_df = pd.concat(framed_rows)
            framed_df = framed_df.reset_index()
            framed_df = framed_df.rename(columns={'index': 'frame_index'})
        return framed_df

class DataframeRenamer(Task):
    def get_valid_parameters(self):
        return ['in', 'what', 'mapping'], []

    def process(self):
        what = self.config['what']
        mapping = self.config['mapping']
        data = self.config['in']
        if isinstance(mapping, list):
        #This is because integer keys are not allowed in dicts and interpreted as list positions
            mapping = {i:mapping[i] for i in range(len(mapping)) if mapping[i] is not None}
        if what == 'column':
            return data.rename(columns=mapping)
        elif what == 'index':
            return data.rename(index=mapping)
        else:
            data[what] = data[what].apply(lambda x: mapping[x] if x in mapping else x)
            return data

class DataframeSampleNFromUniqueValues(Task):
    def get_valid_parameters(self):
        return ['in', 'n', 'column'], []

    def process(self):
        data = self.config['in']
        n = self.config['n']
        column = self.config['column']
        unique_values = data[column].unique()
        sampled_dfs = []
        for v in unique_values:
            sampled_dfs.append(data[data[column] == v].sample(n=n))
        
        return pd.concat(sampled_dfs)

class DataframeUnique(Task):
    def get_valid_parameters(self):
        return ['in', 'column'], []

    def process(self):
        data = self.config['in']
        column = self.config['column']
        is_list = data[column].apply(lambda x: isinstance(x,list))
        is_na = data[column].isna()
        unique_vals = set(data.loc[(~is_list)&(~is_na)][column].unique())
        if is_list.sum()>0:
            unique_vals = unique_vals.union(set(data.loc[is_list][column].sum()))

        return list(unique_vals)
        
class NanImputer(Task):
    def get_valid_parameters(self):
        return ['in','column'], ['mode','distribution']

    def process(self):
        data = self.config['in']
        column = self.config['column']
        mode = self.config.get('mode','mean')
        distribution = self.config.get('distribution','uniform(0,1)')
        if mode == 'random':
            distribution_type = distribution.split('(')[0]
            distribution_params = [float(x) for x in distribution.split('(')[1].split(')')[0].split(',')]
            if distribution_type == 'uniformint':
                data[column] = data[column].apply(lambda x: int(np.random.uniform(*distribution_params)) if np.isnan(x) else x)
                data[column] = data[column].astype(int)
        return data