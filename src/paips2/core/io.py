from pathlib import Path
from pyknife.aws import S3File
import joblib

class PaipFile:
    def __init__(self,*args):
        args = [arg if isinstance(arg,str) else str(arg) for arg in args]
        if str(args[0]).startswith('s3:/'):
            self.filesystem = 's3'
            self.file_obj = S3File(*args)
            self.local_filename = str(Path(self.file_obj.get_key()))
            self.parent = Path(self.file_obj.get_key()).parent
        else:
            self.filesystem = 'local'
            self.file_obj = Path(*args)
            self.parent = self.file_obj.parent
            self.local_filename = str(Path(self.file_obj))

    def load(self):
        if self.filesystem == 's3':
            if not Path(self.local_filename).exists():
                self.file_obj.download(Path(self.file_obj.get_key()))
            return joblib.load(Path(self.file_obj.get_key()))
        elif self.filesystem == 'local':
            if isinstance(self.file_obj,PaipFile):
                return self.load()
            else:
                return joblib.load(self.file_obj)

    def download(self):
        if self.filesystem == 's3':
            self.file_obj.download(Path(self.file_obj.get_key()))

    def __str__(self):
        return str(self.file_obj)

    def upload_from(self, source):
        if self.filesystem == 's3':
            self.file_obj.upload(source)

    def exists(self):
        return self.file_obj.exists()

    def is_symlink(self):
        if self.filesystem == 's3':
            return Path(self.file_obj.get_key()).is_symlink()
        else:
            return self.file_obj.is_symlink()

    def unlink(self):
        Path(self.local_filename).unlink()

    def absolute(self):
        if self.filesystem == 's3':
            return Path(self.file_obj.get_key()).absolute()
        else:
            return self.file_obj.absolute()

    def mkdir(self,*args, **kwargs):
        if self.filesystem == 's3':
            dirpath = Path(self.file_obj.get_key())
            if not dirpath.exists():
                dirpath.mkdir(*args,**kwargs)
        else:
            if not self.file_obj.exists():
                self.file_obj.mkdir(*args,**kwargs)

class TaskIO:
    def __init__(self,data,hash,storage_device='memory',name=None):
        self.data, self.hash, self.storage_device, self.name = data, hash, storage_device, name

    def load(self):
        if self.storage_device=='memory':
            return self.data
        elif self.storage_device=='disk':
            return PaipFile(self.data).load()

    def save(self, cache_path=None, export_path=None, compression_level=0, export=False, cache_db=None, overwrite_export=True):
        self.address = PaipFile(cache_path,self.hash,self.name)
        if not self.address.parent.exists():
            self.address.parent.mkdir(parents=True)

        #Save cache locally:
        if Path(self.address.local_filename).exists():
            Path(self.address.local_filename).unlink()
        joblib.dump(self.data,self.address.local_filename,compress=compression_level)

        #If S3, also upload it
        if (self.address.filesystem == 's3') and not export:
            self.address.upload_from(self.address.local_filename)

        return TaskIO(self.address.local_filename,self.hash,storage_device='disk',name=self.name)