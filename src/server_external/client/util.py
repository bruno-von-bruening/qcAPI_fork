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

@val_call
def pack_run_directory(working_directory:directory, run_directory: List[pdtc_directory]|pdtc_directory, the_model: sqlmodel_cl_meta, id:str|int, worker_id:str) -> pdtc_file:
    
    # Check that the working direcotry is the 
    # This is not really elegant but a check is better than deleting something undesired
    if isinstance(run_directory, str):
        assert os.path.realpath(run_directory)==os.path.realpath(working_directory)
    else:
        for x in run_directory:
            assert any([ y(os.path.join(working_directory,x)) for y in [os.path.isdir,os.path.isfile]]) , f"Not a file {x}"
        run_directory=[ os.path.join(working_directory,x) for x in run_directory]

    the_tar=os.path.realpath(f"{the_model.__name__}_{id}_WID-{worker_id}")
    if os.path.realpath(the_tar).startswith(os.path.realpath(run_directory)):
        raise Exception(f"Refusing to pack run directory since the target {the_tar} is within the run directory {run_directory}")
    if os.path.realpath(the_tar)==os.path.dirname(os.path.realpath(run_directory)) or os.path.realpath(the_tar)==os.path.realpath(run_directory):
        raise Exception(f"Refusing to pack run directory since the target tar name {the_tar} is the same as the run directory {run_directory}")
    if os.path.isdir(the_tar): run_shell_command(f"rm -r {the_tar}")

    if isinstance(run_directory,str):
        try:
            run_shell_command(f"cp -r {run_directory} {the_tar}")
        except Exception as ex:
            raise Exception(f"Could not copy run directory: {ex}") from ex
        pack_files=run_directory
    else:
        if  len(run_directory)>0:
            os.mkdir(the_tar)
            run_shell_command( f"cp -r {' '.join(run_directory)} {the_tar}")
        else:
            raise Exception(f"Expected at least on run directory, got: {run_directory}")


    os.chdir( os.path.dirname(the_tar) )
    try:     
        local_tar=os.path.basename(the_tar)
        run_shell_command(f"tar --create --file={local_tar}.tar {local_tar} --remove-files")
        run_shell_command(f"xz {local_tar}.tar")
        compressed_file=f"{local_tar}.tar.xz"
        assert os.path.isfile(compressed_file), f"Expected to see file {os.path.realpath(compressed_file)} but does not exist"
    except Exception as ex:
        warn(f"Problem in compressing run directory {the_tar}: {ex}. Proceeding by returning uncompressed directory.")
    return os.path.realpath(compressed_file)