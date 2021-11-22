from kahnfigh import Config
from kahnfigh.utils import IgnorableTag, merge_configs
from paips2.core.settings import symbols
from ruamel.yaml import YAML

def apply_mods(conf,mods):
    #For each mod, apply it to the config. mods can be a list of strings or a string with each mode separated with a &
    #Each mod is PATH_TO_KEY=VALUE
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

def insert_yaml(x, special_tags=None, global_config={}, default_config={}):
    #Processing of !yaml tag, which replaces that value with the corresponding yaml config.
    yaml_path = x.split('!yaml ')[-1]
    inserted_config = Config(yaml_path, special_tags=special_tags)
    global_config.update(inserted_config.get('vars', {}))
    default_config.update(inserted_config.get('default_vars', {}))
    if 'vars' in inserted_config:
        inserted_config.pop('vars')
    if 'default_vars' in inserted_config:
        inserted_config.pop('default_vars')

    return process_tags(inserted_config, global_config,default_config)

def identity(x, special_tags=None, global_config={},default_config={}):
    return x

def replace_var(x, special_tags=None, global_config={},default_config={}):
    var_name = x.split('!var ')[-1]
    return global_config.get(var_name, x)

ignorable_tags = {'yaml': insert_yaml,
                  'no-cache': identity,
                  'var': replace_var}

def replace_var_dollars(conf, global_config, default_config):
    import re
    shallow_conf = conf.to_shallow()
    for k,v in shallow_conf.items():
        if isinstance(k,str):
            k_occurrence = re.findall('\$(.*?)\$',k)
        else:
            k_occurrence = []
        if isinstance(v,str):
            v_occurrence = re.findall('\$(.*?)\$',v)
        else:
            v_occurrence = []
        drop_k = False
        if len(k_occurrence) > 0:
            for k_occ in k_occurrence:
                new_k = k.replace('$'+k_occ+'$',global_config.get(k_occ,default_config.get(k_occ,'$'+k_occ+'$')))
            drop_k = True
        else:
            new_k = k
        if len(v_occurrence) > 0:
            for v_occ in v_occurrence:
                v = v.replace('$'+v_occ+'$',global_config.get(v_occ,default_config.get(v_occ,'$'+v_occ+'$')))
        conf[new_k] = v
        if drop_k:
            conf.pop(k)

def process_tags(conf, global_config, default_config):
    for tag_name, tag_processor in ignorable_tags.items():
        tag_paths = conf.find_path('!{}'.format(tag_name), mode='startswith')
        for p in tag_paths:
            conf[p] = tag_processor(conf[p], 
                                    special_tags=[IgnorableTag('!{}'.format(tag)) for tag in ignorable_tags],
                                    global_config = global_config,
                                    default_config = default_config)
    replace_var_dollars(conf,global_config,default_config)
    return conf, global_config, default_config

def include_config(conf,special_tags=None,global_config=None,default_config=None,mods=None):
    include_paths = conf.find_keys(symbols['include'])
    for p in include_paths:    
        p_parent = '/'.join(p.split('/')[:-1]) if '/' in p else None
        for c in conf[p]:
            imported_config = Config(c['config'],yaml_tags=special_tags)
            imported_config, global_config, default_config = process_tags(imported_config,global_config,default_config)
            
            global_config.update(imported_config.get('vars',{}))
            default_config.update(imported_config.get('default_vars',{}))
            if 'vars' in imported_config:
                imported_config.pop('vars')
            if 'default_vars' in imported_config:
                imported_config.pop('default_vars')
            tasks_to_keep = c.get('tasks',[])
            if tasks_to_keep is not None:
                if not isinstance(tasks_to_keep,list):
                    tasks_to_keep = [tasks_to_keep]
                tasks_config = {}
                for t_name, t_config in imported_config['tasks'].items():
                    if t_name in tasks_to_keep:
                        tasks_config[t_name] = t_config
                imported_config['tasks'] = tasks_config

            if p_parent is not None:
                p_config, global_config, default_config = process_tags(Config(conf[p_parent],yaml_tags=special_tags),global_config,default_config)
                new_config = merge_configs([p_config,imported_config])
                conf[p_parent] = new_config
            else:
                p_config, global_config, default_config = process_tags(Config(conf,yaml_tags=special_tags),global_config,default_config)
                conf = merge_configs([p_config,imported_config])

            #apply_mods(conf,mods)
        conf.pop(p)

    return conf, global_config,default_config
            
def process_config(conf,mods=None,logger=None):
    if mods is not None:
        apply_mods(conf,mods)
    conf, global_conf, default_conf = process_tags(conf,conf.get('vars',{}),conf.get('default_vars',{}))
    conf, global_conf, default_conf = include_config(conf,
                                       special_tags=[IgnorableTag('!{}'.format(tag)) for tag in ignorable_tags],
                                       global_config=global_conf,
                                       default_config=default_conf,
                                       mods=mods)
    #Replace unreplaced vars with default config:
    tag_paths = conf.find_path('!var', mode='startswith')
    for p in tag_paths:
        var_name = conf[p].split('!var ')[-1]
        if var_name in global_conf:
            conf[p] = global_conf[var_name]
        elif var_name in default_conf:
            conf[p] = default_conf[var_name]
        else:
            if logger is not None:
                logger.warning('Variable {} not found in vars or default vars. None value will be adopted'.format(var_name))
            conf[p] = None
    return conf

