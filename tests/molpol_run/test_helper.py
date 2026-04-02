#from qcp_global_utils.shell_processes.execution import run_shell_command
import subprocess as sp
import time
import os, shutil



def start_server(config_file):
    """ Return server handle"""
    assert config_file, f"Not a file: {config_file}"
    try:
        #  fastapi='/home/bruno/0_Software/miniconda3/envs/qcAPI/bin/fastapi'
        #  assert isfile(fastapi)
        #  server=sp.Popen([f"{fastapi}","run","server.py"], stderr=sp.PIPE, stdout=sp.PIPE)
        python='python' # '/home/bruno/0_Software/miniconda3/envs/qcAPI/bin/python'
        # assert isfile(python)
        script=shutil.which('qcp_server.py')
        server=sp.Popen(f"python {script} run --config {config_file}".split(), stderr=sp.PIPE, stdout=sp.PIPE)
        pid=server.pid

        time.sleep(3)
        if not isinstance(server.poll(), type(None)):
            #kill_process(server)
            stdout, stderr = server.communicate()
            error_with_indent='\n'.join([4*' '+x for x in stderr.decode().split('\n')])
            error_with_indent+='\n'.join(['STDOUT:']+[4*' '+x for x in stdout.decode().split('\n')])
            raise Exception(f"Exiting do to server shutdown! ERROR:\n{error_with_indent}")
        else:
            print(f"Server started with process id: {pid}")
        
    except Exception as ex:
        raise Exception(f"Cannot open server: \n{ex}")
    return server
