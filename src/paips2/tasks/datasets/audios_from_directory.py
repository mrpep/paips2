from re import split
from paips2.core import Task
import pandas as pd
from paips2.utils.parallel import paralellize_fn
import soundfile as sf
import glob
from pathlib import Path
from tqdm import tqdm
from paips2.utils.files import read_list
import pymediainfo
import torchaudio
import soundfile as sf

class AudioDatasetFromDirectory(Task):
    def get_valid_parameters(self):
        return ['dataset_path'], ['max_rows', 're', 'audio_extensions', 'split_lists',
        'split_column_in','split_column_out','constant_column','glob_pattern','index','n_workers']

    def extract_audio_metadata(self, f, dataset_path):
        #Extract audio info:
        if f.suffix in ['.wav', '.flac']:
            #audio_metadata = sf.info(f).__dict__
            #audio_metadata['absolute_path'] = str(f.absolute())
            audio_metadata = {}
            audio_metadata_ = sf.info(str(f.absolute())).__dict__
            audio_metadata['absolute_path'] = audio_metadata_['name']
            audio_metadata['sample_rate'] = audio_metadata_['samplerate']
            audio_metadata['num_frames'] = audio_metadata_['frames']
            audio_metadata['num_channels'] = audio_metadata_['channels']
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
        for d in dataset_path:
            if str(Path(f).absolute()).startswith(str(Path(d).absolute())):
                audio_metadata['relative_path'] = str(Path(f).relative_to(d))
        audio_metadata['name'] = str(f.stem)
        #Match regular expressions:
        filename_re = self.config.get('re', None)
        if filename_re:
            import re
            re_match = re.match(filename_re,audio_metadata['relative_path'])
            if re_match is not None:
                fields = re_match.groupdict()
                audio_metadata.update(fields)

        return audio_metadata

    def process(self):
        #Gather all audios in directory:
        dataset_path = self.config.get('dataset_path')
        if not isinstance(dataset_path,list):
            dataset_path = [dataset_path]
        dataset_path = [Path(f).expanduser() for f in dataset_path]
        extension = self.config.get('audio_extensions','wav')
        if not isinstance(extension,list):
            extension = [extension]

        available_audios = []
        available_stems = set()
        for p in dataset_path:
            for ext in extension:
                if self.config.get('glob_pattern') is not None:
                    available_audios_i = list(p.glob(self.config.get('glob_pattern')))
                else:
                    available_audios_i = list(p.rglob('*.{}'.format(ext)))
                for p_i in available_audios_i:
                    path_stem = Path(p_i).relative_to(p)
                    if path_stem not in available_stems:
                        available_audios.append(p_i)
                        available_stems.add(path_stem)

        def extract_metadata(available_audios):
            metadatas = []
            for f in tqdm(available_audios):
                audio_metadata = self.extract_audio_metadata(f,dataset_path)
                metadatas.append(audio_metadata)
            return metadatas

        if self.config.get('n_workers',0)>0:
            metadatas = paralellize_fn(extract_metadata,available_audios,self.config['n_workers'])
        else:
            metadatas = extract_metadata(available_audios)
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
            split_list = {k: read_list(str(Path(dataset_path[0],v).absolute())) for k,v in split_list.items()}
            df_metadata = df_metadata.set_index(split_col_in)

            for k,v in split_list.items():
                df_metadata.loc[v, split_col_out] = k
            df_metadata[split_col_in] = df_metadata.index

        const_col = self.config.get('constant_column')
        if const_col:
            for k,v in const_col.items():
                df_metadata[k] = v

        index_re = self.config.get('index', None)
        if index_re is not None:
            df_metadata['index'] = df_metadata.apply(lambda x: index_re.format(**(x.to_dict())).replace('/','_'),axis=1)
            df_metadata = df_metadata.set_index('index')

        if self.config.get('max_rows') is not None:
            df_metadata = df_metadata.sample(n=self.config.get('max_rows'))

        return df_metadata
