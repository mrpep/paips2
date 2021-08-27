from paips2.core import Task
import pandas as pd
import soundfile as sf
import glob
from pathlib import Path

class ClothoReader(Task):
    def get_valid_parameters(self):
        return ['dataset_path'], ['max_rows']

    def process(self):
        dataset_path = self.config.get('dataset_path')
        dfs = []
        for split in ['development','validation','evaluation']:
            df_metadata = pd.read_csv('{}/clotho_metadata_{}.csv'.format(Path(dataset_path).absolute(),split), encoding="ISO-8859-1").set_index('file_name')
            df_captions = pd.read_csv('{}/clotho_captions_{}.csv'.format(Path(dataset_path).absolute(),split)).set_index('file_name')
            df_audio_metadata = pd.DataFrame([sf.info(f).__dict__ for f in glob.glob('{}/{}/*.wav'.format(Path(dataset_path).absolute(),split))])
            df_audio_metadata['file_name']=df_audio_metadata['name'].apply(lambda x: x.split('/')[-1])
            df_audio_metadata = df_audio_metadata.set_index('file_name')
            df_split = pd.concat([df_metadata,df_captions,df_audio_metadata],axis=1)
            df_split['split'] = split
            dfs.append(df_split)

        df_clotho = pd.concat(dfs)
        if self.config.get('max_rows'):
            df_clotho = df_clotho.sample(self.config.get('max_rows'))
        
        return df_clotho