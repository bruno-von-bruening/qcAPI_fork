from . import *

from .environment import file, get_python_from_conda_env
from .auxiliary import my_exception
from .environment import QCAPI_HOME

@val_call
def query_config(data:dict|BaseModel, query_tags:List[str]):
    try:
        cur_config=copy.deepcopy(data)
        for i,key in enumerate(query_tags): # Maybe key occurs double
            if issubclass(type(cur_config), BaseModel): cur_config=cur_config.dump_model()
            if not isinstance(cur_config, dict): 
                if i<1:
                    raise Exception(f"Cannot process {i}th key ({key}) since no dictionary was provided but {cur_config}")
                else:
                    raise Exception(f"Result of {i-1}th key ({query_tags[i-1]}) is {cur_config}, but expected a dictionary at this level")
            assert key in cur_config.keys(), f"Could not find {i+1}th key ({key}) of query: {query_tags}\n available_keys={list(cur_config.keys())}"
            cur_config=cur_config[key]
    except Exception as ex: my_exception(f"Problem in querying {data} with {query_tags}",ex)
    return cur_config

class qcAPI_storage_info(BaseModel):
    storage_root_directory: str # directory were database content will be attached
#class qcAPI_environment_info(BaseModel):
#    """ Information about conda environment and scripts to use"""
#    _data: dict ={}
#    def query(self, query_tags:List[str]):
#        return query_config(self._data, query_tags)
#    def __init__(self,*args, **kwargs):
#        super().__init__(*args)        
#        self._data=kwargs


class config_base(BaseModel):
    source: file
    def query(self, query_tags:List[str]):
        try:
            return query_config(self.model_dump(), query_tags)
        except Exception as ex: my_exception(f"Could not query information form file {self.source}", ex)

class qcAPI_server_config(config_base):
    source: file
    database_file: file|str # does not need to be a file 
    
    storage_info: qcAPI_storage_info
    #role: Literal['server','worker']

    @field_validator('database_file', mode='before')
    @classmethod
    def database_extension(cls, value: str) -> str:
        return value.replace('.db','')+'.db'

class qcAPI_worker_config(config_base):
    source: file
    environment: dict={} #qcAPI_environment_info=qcAPI_environment_info()
    imports:       List[file]|file|None = None


###### HANDLE CONFIG
@validate_call
def load_yaml(config_file:file):
    with open(config_file,'r') as rd:
        config=yaml.safe_load(rd)
        assert isinstance(config,dict), f"Content of file \'{os.path.realpath(config_file)} is not a dictionary"
    return config
@validate_call
def import_config(config: Union[dict,file]):
    if isinstance(config,str):
        config=load_yaml(config)
    
    try:
        import_key='imports'
        if import_key in config.keys():
            imports=config[import_key]
            @validate_call
            def add_loop(the_dict:dict, ref_dict:dict) -> dict:
                for k,v in the_dict.items():
                    if not k in ref_dict.keys():
                        ref_dict.update({k:v})
                    else:
                        if isinstance(ref_dict[k], dict):
                            ref_dict[k]=add_loop(v, ref_dict[k])
                        elif ref_dict[k] is None:
                            ref_dict.update({k:v})
                        elif ref_dict[k]==v:
                            pass
                        else:
                            raise Exception(f"Key {k} appears in provdied config but also in import {imports} with contradictory values")
                return ref_dict
            @validate_call
            def add(loc_config:dict, loc_add_config:dict) ->dict:

                try:
                    config=add_loop(loc_add_config, loc_config)
                except Exception as ex: raise Exception(loc_add_config, loc_config, ex)
                return config
            
            if isinstance(imports, str): imports=[ imports ]
            elif isinstance(imports, list): pass
            else: raise Exception(f"Found import with type {type(imports)} this is not expected.")
            #
            assert all([ isinstance(x, str) for x in imports]), f"Expected only strings, but got: {imports}"
            # Get the imports recursively
            for x in imports:
                if isinstance(config,str):
                    x=load_yaml(x)
                add_config=import_config(x)
                config=add(config, add_config)

        return config
    except Exception as ex: my_exception(f"Problem in importing:", ex)

@val_call
def load_config_generic(config_file:file, config_model):
    # Read the file
    try:
        config=load_yaml(config_file)
    except Exception as ex: my_exception(f"Could not process \'{config_file}\':", ex)

    # Check for import
    try:
        config=import_config(config)
    except Exception as ex: my_exception(f"Error while importing:", ex)
    
    # Check database keys
    try:
        qcapi_config=config_model(**config, source=os.path.realpath(config_file))
    except Exception as ex: my_exception(f"Could not construct a vaild qcAPI configuration from \'{config_file}\':" , ex)

    return qcapi_config

@validate_call
def load_server_config(config_file:file) -> qcAPI_server_config:
    """ """
    return load_config_generic(config_file, qcAPI_server_config)
@validate_call
def load_worker_config(config_file:file) -> qcAPI_worker_config:
    """ """
    return load_config_generic(config_file, qcAPI_worker_config)

@validate_call
def make_dummy_config_file() -> file:
    pass
    dic=dict(
        database_file='dummy_database'
    )

    import_file=os.path.join(QCAPI_HOME, 'install','environment_for_server_config.yaml')
    assert os.path.isfile(import_file), (
           f"Could not find file {import_file} which is supposed to define paths to scripts."
           +f" This file should be autogenerated when running setup.py in the install directory"
      )
    dic.update({'imports': import_file})

    config=qcAPI_Config(**dic)

    config_file='dummy_config.yaml'
    with open(config_file, 'w') as wr:
        yaml.safe_dump(config.model_dump(),wr)

    return config_file

# @validate_call
# def query_config(config_file:file, query: tuple=(), target=None):
#     """
#     
#     target: """
#     config=load_worker_config_from_file(config_file)
#     the_config=config.model_dump()
#     try:
#         cur_config=copy.deepcopy(the_config)
#         for i,key in enumerate(query): # Maybe key occurs double
#             if not isinstance(cur_config, dict): 
#                 if i<1:
#                     raise Exception(f"Cannot process {i}th key ({key}) since no dictionary was provided but {cur_config}")
#                 else:
#                     raise Exception(f"Result of {i-1}th key ({query[i-1]}) is {cur_config}, but expected a dictionary at this level")
#             
#             if issubclass(type(cur_config), BaseModel): cur_config=cur_config.dump_model()
#             assert key in cur_config.keys(), f"Could not find {i}th key ({key}) in {config_file} of query: {query}\n available_keys={list(cur_config.keys())}"
#             cur_config=cur_config[key]
#     except Exception as ex: my_exception(f"Problem in querying {config_file} with {query}",ex)
#     return cur_config 
# @validate_call
# def get_env(config_file:file, the_key:str):
#     """ """
#     query=('environment', the_key)
#     python_env=query_config(config_file, (*query,'python_env') )
#     script_exc=query_config(config_file, (*query, 'script' ))
#     python_exc=get_python_from_conda_env(python_env)
#     
#     return python_exc, script_exc

# Outdate use query config instead
def load_global_config(config_file):
    config=load_config_from_file(config_file).model_dump()
    global_key='environment'
    if global_key in config.keys():
        global_conf=config[global_key]
    else:
        print(f"Did not find key {global_key} in {config_file}: {config.keys()}")
        global_conf={}
    return global_conf