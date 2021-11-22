from re import split
from paips2.core import Task
import pandas as pd
import soundfile as sf
import glob
from pathlib import Path
from tqdm import tqdm
from paips2.utils.files import read_list
import pymediainfo

class AudioDatasetFromDirectory(Task):
    def get_valid_parameters(self):
        return ['dataset_path'], ['max_rows', 're', 'audio_extensions', 'split_lists','split_column_in','split_column_out','constant_column']

    def process(self):
        #Gather all audios in directory:
        dataset_path = Path(self.config.get('dataset_path')).expanduser()
        extension = self.config.get('audio_extensions','wav')
        if not isinstance(extension,list):
            extension = [extension]
        available_audios = []
        for ext in extension:
            available_audios.extend(list(dataset_path.rglob('*.{}'.format(ext))))
        metadatas = []
        for f in tqdm(available_audios):
            #Extract audio info:
            if f.suffix == '.wav':
                audio_metadata = sf.info(str(f.absolute())).__dict__
                audio_metadata['absolute_path'] = audio_metadata.pop('name')
                audio_metadata.pop('extra_info')
                audio_metadata.pop('verbose')
            else:
                info = pymediainfo.MediaInfo.parse(f)
                audio_metadata = info.to_data()['tracks']
                for t in audio_metadata:
                    if t['track_type'] == 'Audio':
                        audio_metadata = t
                        break
                audio_metadata['absolute_path'] = str(f.absolute())
                audio_metadata['frames'] = audio_metadata.pop('samples_count')
                for k,v in audio_metadata.items():
                    if isinstance(v,list) and len(v) == 1:
                        audio_metadata[k] = v[0]
                    elif isinstance(v,list) and len(v) > 1:
                        audio_metadata[k] = ','.join(v)
            audio_metadata['relative_path'] = str(f.relative_to(dataset_path))
            audio_metadata['name'] = str(f.stem)
            #Match regular expressions:
            filename_re = self.config.get('re', None)
            if filename_re:
                import re
                re_match = re.match(filename_re,audio_metadata['relative_path'])
                if re_match is not None:
                    fields = re_match.groupdict()
                    audio_metadata.update(fields)
            
            metadatas.append(audio_metadata)

        #Make dataframe and see if split lists were given:
        df_metadata = pd.DataFrame(metadatas)
        split_list = self.config.get('split_lists')
        split_col_in = self.config.get('split_column_in', 'relative_path')
        split_col_out = self.config.get('split_column_out', 'partition')

        if split_list is not None:
            #If a split is default, filenames not found in lists are given that split:
            k_default = None
            for k,v in split_list.items():
                if v == 'default':
                    k_default = k
            if k_default is not None:
                df_metadata[split_col_out] = k_default
                split_list.pop(k_default)
            #Then, we open each list file and assign the name to the split column
            split_list = {k: read_list(str(Path(dataset_path,v).absolute())) for k,v in split_list.items()}
            df_metadata = df_metadata.set_index(split_col_in)
            for k,v in split_list.items():
                df_metadata.loc[v, split_col_out] = k
            df_metadata[split_col_in] = df_metadata.index

        const_col = self.config.get('constant_column')
        if const_col:
            for k,v in const_col.items():
                df_metadata[k] = v

        if self.config.get('max_rows') is not None:
            df_metadata = df_metadata.sample(n=self.config.get('max_rows'))

        return df_metadata