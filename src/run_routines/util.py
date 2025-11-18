from . import *
from util.import_helper import *

def my_sep(message, jobname, sep, info_line=None, sep_times=1, print_hostname=True):
    """ Create headline to indicate status in output file (for orientation/debugging purposes)"""
    sep*=60
    if print_hostname:
        hostname=os.popen("hostname").read().strip()
        host_str=f" ( hostname=\'{hostname}\' )"
    else:
        host_str=''
    lines= [sep]*sep_times + [message + f" ({datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}){host_str}:"] + [' '*4+f"jobname=\'{jobname}\'"]
    if info_line!=None:
        lines+=[' '*4+info_line]
    lines+=[sep]*sep_times 
    print('\n'.join(lines))


def recover_psi4_storage_file(jobname):
    # Storage file:
    storage_file_search=f"STORAGE_*{jobname}.yaml"
    storage_file=glob.glob(storage_file_search)
    assert len(storage_file)==1, f"Did not find exactely one storage file at {os.getcwd()} for {storage_file_search}: {storage_file}"
    storage_file=storage_file[0]
    assert len(storage_file)
    assert os.path.isfile(storage_file), f"Not a file {storage_file}"
    
    return storage_file
