from ..basic_imports import *

def get_relevant_files(working_directory: pdtc_directory, run_data: dict) -> list: 
    """
    Get all files in the current directory that are not fchk files and not already listed in run_data['files']."""
    files_there={} if not 'files' in run_data else run_data['files']
    return [
            item for item in glob.glob('*') 
            if not bool(re.search(r'.fchk', item.lower())) and not item in files_there
    ]