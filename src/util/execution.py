from . import *
import subprocess as sp

@validate_call
def run_command_in_shell(cmd:str, stderr:str|None='stderr.txt',stdout:str|None='stdout.txt'):

    if stdout is not None:
        cmd=f"{cmd} 1> {stdout}"
        read_stdout=True
    else:
        read_stdout=False
    if stderr is not None:
        cmd=f"{cmd} 2> {stderr}"
        read_stderr=True
    else:
        read_stderr=False

    process=sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE, preexec_fn=os.setsid)
    p_stdout, p_stderr = process.communicate()
    
    if process.returncode!=0: 
        message=f"The commannd {cmd} did terminate with error: {p_stderr.decode('utf-8')}"
        if read_stderr:
            with open(stderr,'r') as rd:
                message+=f"\nError printed to {stderr}=\n {rd.read()}"
        raise Exception(message)
    
    return stdout, stderr
    