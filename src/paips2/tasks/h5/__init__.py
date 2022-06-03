from paips2.core import Task
import pyarrow.parquet as pq
from pathlib import Path
import joblib

class ExportDataframe(Task):
    def get_valid_parameters(self):
        return ['in','out'], ['format', 'n_blocks','replace']

    def process(self):
        format = self.config.get('format','pickle')
        replace = self.config.get('replace',True)
        output_path = Path(self.config['out']).expanduser()
        if not output_path.parent.exists():
            output_path.parent.mkdir(parents=True)
        if format == 'parquet':
            n_blocks = self.config.get('n_blocks',1)
            if n_blocks > 1:
                output_path_ = []
                rows_per_block = len(self.config['in'])//n_blocks
                for b in range(n_blocks):
                    out_path_i = Path(output_path.parent,Path(output_path.stem + '_{}{}'.format(b,output_path.suffix)))
                    output_path_.append(out_path_i)
                    if replace or (not replace and not out_path_i.exists()):
                        self.config['in'].iloc[b*rows_per_block:(b+1)*rows_per_block].to_parquet(out_path_i)
            else:
                output_path_ = output_path
                if replace or (not replace and not output_path.exists()):
                    self.config['in'].to_parquet(output_path)
        elif format == 'pickle':
            n_blocks = self.config.get('n_blocks',1)
            if n_blocks > 1:
                rows_per_block = len(self.config['in'])//n_blocks
                output_path_ = []
                for b in range(n_blocks):
                    out_path_i = Path(output_path.parent,Path(output_path.stem + '_{}{}'.format(b,output_path.suffix)))
                    output_path_.append(out_path_i)
                    if replace or (not replace and not out_path_i.exists()):
                        joblib.dump(self.config['in'].iloc[b*rows_per_block:(b+1)*rows_per_block],out_path_i)
            else:
                output_path_ = output_path
                if replace or (not replace and not output_path.exists()):
                    joblib.dump(self.config['in'], output_path)
        return output_path_