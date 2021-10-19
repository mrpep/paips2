from kahnfigh import Config
from kahnfigh.utils import IgnorableTag, merge_configs
from paips2.core.settings import symbols
from ruamel.yaml import YAML

def apply_mods(conf,mods):
    yaml = YAML()
    if isinstance(mods,str):
        mods = mods.split('&')
    for mod in mods:
        mod_key = mod.split('=')[0]
        mod_value = mod.split('=')[1]
        if '!' in mod_value:
            conf[mod_key] = mod_value
        else:
            conf[mod_key] = yaml.load(mod_value)

def insert_yaml(x, special_tags=None, global_config={}):
    yaml_path = x.split('!yaml ')[-1]
    inserted_config = Config(yaml_path, special_tags=special_tags)
    global_config.update(inserted_config.get('vars', {}))
    if 'vars' in inserted_config:
        inserted_config.pop('vars')
    return process_tags(inserted_config, global_config)

def identity(x, special_tags=None, global_config={}):
    return x

def replace_var(x, special_tags=None, global_config={}):
    var_name = x.split('!var ')[-1]
    return global_config.get(var_name, x)

ignorable_tags = {'yaml': insert_yaml,
                  'no-cache': identity,
                  'var': replace_var}

def process_tags(conf, global_config):
    for tag_name, tag_processor in ignorable_tags.items():
        tag_paths = conf.find_path('!{}'.format(tag_name), mode='startswith')
        for p in tag_paths:
            conf[p] = tag_processor(conf[p], 
                                    special_tags=[IgnorableTag('!{}'.format(tag)) for tag in ignorable_tags],
                                    global_config = global_config)
    return conf, global_config

def include_config(conf,special_tags=None,global_config=None,mods=None):
    include_paths = conf.find_keys(symbols['include'])
    for p in include_paths:    
        p_parent = '/'.join(p.split('/')[:-1]) if '/' in p else None
        for c in conf[p]:
            imported_config = Config(c['config'],yaml_tags=special_tags)
            p_config, global_config = process_tags(imported_config,global_config)
            if p_parent is not None:
                p_config, global_config = process_tags(Config(conf[p_parent],yaml_tags=special_tags),global_config)
                new_config = merge_configs([p_config,imported_config])
                conf[p_parent] = new_config
            else:
                p_config, global_config = process_tags(Config(conf,yaml_tags=special_tags),global_config)
                conf = merge_configs([p_config,imported_config])
            #apply_mods(conf,mods)
        conf.pop(p)
    return conf, global_config
            
def process_config(conf,mods=None):
    if mods is not None:
        apply_mods(conf,mods)
    conf, global_conf = process_tags(conf,conf.get('vars',{}))
    conf, global_conf = include_config(conf,
                                       special_tags=[IgnorableTag('!{}'.format(tag)) for tag in ignorable_tags],
                                       global_config=global_conf,mods=mods)
    return conf