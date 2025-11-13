from . import *
from typing import Tuple

from qcp_objects.operations.calculate_esp_dmp import calculate_esp_dmp
from util.util import copy_file
from util.environment import directory, file
from functools import partial
import copy

@validate_call
def setup_environment(
    record_id:str|int ,worker_id:str, 
    moment_file:file, grid_file:file,
    job_tag:str='DMPESP',
):#-> Tuple[str,str]:
    """ Create job name and switch to directory"""

    
    jobname=make_jobname(record_id, worker_id=worker_id, job_tag=job_tag)
    work_dir=make_dir(jobname=jobname)
    os.chdir(work_dir)

    # Copy files
    the_cop=partial(copy_file, target=os.getcwd())
    copy_job=[
        (moment_file, [], dict(link=False) ),
        (grid_file, [],dict(link=False)),
    ]
    for file,  args, kwargs in copy_job:
        copy_file(file, **kwargs)


    return jobname, work_dir


@validate_call
def execute(python:file, script:file, moment_file:file, grid_file:file, ranks:str|None):

    @validate_call
    def make_shell_command(python:str, script:str, moment_file:file, grid_file:file, ranks:str) -> str:
        cmd=f"{python} {script} --mom {moment_file} --grid {grid_file} --ranks {ranks}"
        return cmd
    
    cmd=make_shell_command(python, script, moment_file, grid_file, ranks)
    stdout,stderr=run_shell_command(cmd)

@validate_call
def retrieve(tracker:Tracker, results_file:file='results.yaml'):
    try:
        with open(results_file,'r') as rd:
            data=yaml.safe_load(rd)
        map_file=data['files']['map']
    except Exception as ex:
        raise Exception(f"Could not load yaml data from {results_file}: {ex}")
    try:
        stats=data['results']['statistical_indicators']
        assert isinstance(stats, dict), f"Excpected stats as dictionary, got {type(stats)}:\n{stats}"
    except Exception as ex:
        stats=None
        tracker.add_warning(f"Could not retrieve statistical indicators from {results_file}: {str(ex)}")

    try:
        map=File_Model(
            path_to_container=os.path.relpath(os.path.realpath(os.path.dirname(map_file)), os.environ['HOME']),
            path_in_container='.',
            file_name=os.path.basename(map_file)
        )
    except Exception as ex:
        raise Exception(f"Failure in generating File_Model from map_file=\'{map_file}\':\n{ex}")

    if not isinstance(stats, type(None)):
        try:
            stats=Map_Stats_Model(**stats)
            stats=stats.model_dump()
        except Exception as ex:
            tracker.add_warning(f"Failure in generating {Map_Stats_Model.__name__} fomr map_file=\'{map_file}\':\n{ex}")
            stats=None

    
    run_data=dict(
        files={
            DMP_MAP_File.__name__: os.path.realpath(map_file)
        },
        sub_entries={
            DMP_ESP_MAP_Stats.__name__: stats
        }
    )
    return tracker,run_data
    
# receive arguments

# execute the python script(in environments)
def run_dmp_esp(
        python:str, script:str, moment_file:str, grid_file:str, #ranks:str,
        record:dict, worker_id:str, 
        num_threads: int = 1, target_dir:str=None,
        do_test:bool=False,
)-> dict:
    """"""

    tracker=Tracker()
    try:
        record_id=record['id']
        ranks=record['ranks']
        # Check input
        # make_dir and link all files to it
        # execut the script
        # retrieve results (ideally from dropeed yaml file)

        setup_environment(record_id=record['id'], worker_id=worker_id, moment_file=moment_file, grid_file=grid_file)

        execute(python, script, moment_file, grid_file, ranks)

        tracker, results=retrieve(tracker)
        run_data=copy.deepcopy(results)
        working_directory=os.getcwd()
        files_to_store=get_relevant_files(working_directory, run_data)
        run_data.update(dict(
            working_directory=working_directory,
            files_to_store=files_to_store
        ))

        converged=RecordStatus.converged
    except Exception as ex:
        converged=RecordStatus.failed
        run_data=None
        tracker.add_error(f"Terminating due to error:\n"+analyse_exception(ex))

    record.update( tracker.model_dump())
    record.update(dict( converged=converged, run_data=run_data ))
    return record 