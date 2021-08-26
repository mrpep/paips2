from paips2.core import Task
import importlib
from pathlib import Path
import shlex
import subprocess
import os

class PythonFunction(Task):
    def get_valid_parameters(self):
        return ['file_path','function_name'], ['function_args', 'function_kwargs']
    
    def get_output_names(self):
        return self.config.get('output_names','out')

    def process(self):
        file_path = self.config.get('file_path')
        func_name = self.config.get('function_name')
        func_args = self.config.get('function_args',[])
        func_kwargs = self.config.get('function_kwargs',{})

        spec = importlib.util.spec_from_file_location(Path(file_path).stem, file_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        func = getattr(module,func_name)

        return func(*func_args,**func_kwargs)

class ShScript(Task):
    def get_valid_parameters(self):
        return ['command'], ['check','text','shell','executable','cwd']

    def get_output_names(self):
        return ['stdout','stderr']
    
    def process(self):
        command = self.config.get('command')
        shell = self.config.get('shell',False)
        if not shell:
            command = shlex.split(command)

        text = self.config.get('text',True)
        check = self.config.get('check',True)
        executable = self.config.get('executable','/bin/sh')
        cwd = self.config.get('cwd',os.getcwd())

        stdout = subprocess.PIPE if shell else None
        stderr = subprocess.PIPE if shell else None
        
        normal = subprocess.run(command,
                    stdout=stdout, stderr=stderr,
                    check=check,
                    text=text,
                    shell=shell,
                    cwd=cwd,
                    executable=executable)

        return normal.stdout, normal.stderr



