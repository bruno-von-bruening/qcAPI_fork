from . import *
from util.environment import get_python_from_conda_env

def get_camcasp_path():
    CAMCASP='CAMCASP'
    assert CAMCASP in os.environ.keys(), f"Variable \'{CAMCASP}\' is not defined!"
    camcasp_path=os.environ[CAMCASP]
    return camcasp_path

@val_call
def get_python_exc_and_script(config_file:file, tag)->Tuple[file,file]:
    worker_config=load_worker_config(config_file)
    python_env=worker_config.query( ('environment',tag, 'python_env'))
    script_exc=  worker_config.query( ('environment',tag, 'script')) 
    python_exc=os.path.join(get_python_from_conda_env(python_env))
    import copy

    old_script=copy.deepcopy(script_exc)


    try: # I could allow path variable but be careful, this may lead to loading them from current environment!
        try:
            script_exc=[
                ( x if not x.startswith('$') else os.environ[x.lstrip('$')] ) for x in script_exc 
            ]
        except Exception as ex: 
            raise Exception(f"Problem in getting system variable: {ex}")

        script_exc=os.path.join( *script_exc )

        # def from_path(s):
        #     script_exc_test=shutil.which( s )
        #     path_exc=(script_exc_test is not None)
            
        # If not a path, then try to get from system variable
        if not '/' in script_exc:


            # maybe its conda
            script_exc_test=get_python_from_conda_env(python_env).replace('python',script_exc)  # remove python from the end
            if not os.path.isfile(script_exc_test):
                raise Exception(f"Could not find executable {script_exc_test} for conda env {python_env}")
            script_exc=script_exc_test
        else:
            assert os.path.isfile(script_exc), f"Not a file {script_exc}"
    except Exception as ex: raise Exception(f"Provided script {old_script} (for {tag}) does not yield valid file. {ex}") from ex

    return python_exc, script_exc

from contextlib import contextmanager
@contextmanager
def cd(path):
    origin = os.getcwd()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(origin)

@val_call
def pack_run_directory(
    working_directory:directory, 
    to_store: List[pdtc_directory|pdtc_file]|pdtc_directory|pdtc_file, 
    the_model: sqlmodel_cl_meta, 
    id:str|int, 
    worker_id:str
) -> pdtc_file:
    """ 
    Copies one or more directories or files into a tar archives and compresses it.
    As an intermediate step, copy all files to a container directory which is next to working_directory
    """

    # Check the run direcotry 
    if isinstance(to_store, str):
        to_store=[ to_store ]
    # all to store should be located within run directory!
    for x in to_store:
        assert os.path.realpath(x).startswith(os.path.realpath(working_directory)), f"Not within working directory {x}"
    
    # check if the working directory is in to_store if yes then drop files specified at sublevel
    inside_other_directory=lambda x,full_list: any( os.path.realpath(x).startswith(os.path.realpath(os.path.join(working_directory,y))) for y in full_list if y!=x )
    for x in to_store:
        if inside_other_directory(x,to_store):
            warn(f"Directory {x} is within another directory specified in to_store. Dropping {x} from to_store since it will be included in the tar file by the parent directory.") 
            to_store.remove(x)
    to_store=list(set(to_store)) # remove duplicates if any

    store_dir_name=os.path.realpath(f"{the_model.__name__}_{id}_WID-{worker_id}_STORAGE")
    store_dir=os.path.join( os.path.dirname(working_directory), store_dir_name )

    upper_level=os.path.dirname(os.path.realpath(store_dir))
    strip_upper_level=lambda x: os.path.relpath(os.path.realpath(x), upper_level)
    store_dir=strip_upper_level(store_dir)
    to_store=[ strip_upper_level(x) for x in to_store ]

    with cd(upper_level):
        try:
            # Remove the directory if it exists!
            if os.path.exists(store_dir):
                warn(f"Storage directory {store_dir} already exists. Removing it to avoid problems.")
                run_shell_command(f"rm -r {store_dir}")

            if not ( len(to_store)==1 or not os.path.isdir(to_store[0]) ):
                os.mkdir(store_dir)
            
            cmd=f"cp -r {' '.join(to_store)} {store_dir}"
            try: 
                run_shell_command(cmd)
            except Exception as ex:
                raise Exception(f"Could not copy run directory: {ex}") from ex
        except Exception as ex:
            raise Exception(f"Location was {upper_level} paths are relative to it. {ex}") from ex
        
        # now the assert store_dir is there and pack it
        assert os.path.isdir(store_dir), f"Expected to see directory {os.path.realpath(store_dir)} but does not exist"
        the_tar=f"{os.path.basename(store_dir)}.tar"
        compressed_file=f"{the_tar}.xz"
        # Pack
        try:
            run_shell_command(f"tar --create --remove-files --file={the_tar} {store_dir}")
        except Exception as ex:
            raise Exception(f"Problem in creating tar file {the_tar} from directory {store_dir}: {ex}") from ex
        # compress
        try:
            run_shell_command(f"xz {the_tar}")
            assert os.path.isfile(compressed_file), f"Expected to see file {os.path.realpath(compressed_file)} but does not exist"
        except Exception as ex:
             raise Exception(f"Problem in compressing run directory {the_tar}: {ex}") from ex
        compress_file=os.path.realpath(compressed_file)

    return compress_file


    
