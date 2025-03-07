#/usr/bin/env python


import os, yaml

def get_environment_var(key, type='file'):
    """ I could have a options in every script that provides an unique code and versions that can be performed"""
    available=os.environ.keys()
    if not key in available:
        quit(f"SHELL Environment variable \'{key}\' not defined, please do that!")

    var=os.environ[key] 
    print(var)
    if type=='file':
        assert os.path.isfile(var), f"{var}"
    elif type=='directory':
        assert os.path.isdir(var), f"{var}"

    return var
def get(input, type=None):
    def helper(string, type=None):
        assert isinstance(string, str)
        if string.startswith('$'):
            string=get_environment_var(string[1:], type=type)
        return string

    if isinstance(input, tuple):
        path_members=[]
        for x in input:
            path_members.append(helper(x, type='directory'))
        path=os.path.join(*path_members)
        assert os.path.isfile(path)
    elif isinstance(input, str):
        path=helper(input, type=type)
    return path
    
def check_conda_env(env_string):
    """ """

    # Get the conda executable
    conda_exe_var='CONDA_EXE'
    if not conda_exe_var in os.environ.keys():
        raise Exception(f"NO environment variable \'{conda_exe_var}\'")
    conda_exe_path=os.environ[conda_exe_var]

    # Truncate the end to get base path
    assert conda_exe_path.endswith('bin/conda'), conda_exe_path
    conda_exe_path=conda_exe_path.replace('bin/conda','')

    assert os.path.isdir(conda_exe_path), f"Conda base environment assumed to be \'{conda_exe_path}\' but is not a directory!"

    assert os.path.isdir(os.path.join(conda_exe_path,'envs',env_string)), f"Conda environment cannot be found in current shell environment: {env_string} is not below {conda_exe_path}"
    return conda_exe_path

def setup_env():


    envs={}
    psi4_script=get_environment_var('PSI4_SCRIPT')
    horton_script=get_environment_var('HORTON_SCRIPT')
    isod_script=get(('$DENSITY_OPERATIONS_HOME', 'bin/calc_IsoDensSurf.py'))
    rho_esp_script=get( ('$DENSITY_OPERATIONS_HOME', 'bin/calc_esp.py'))
    qc_dens_lib=get('$DENSITY_OPERATIONS_LIB', type='directory')
    dmp_esp_script=get(
        ('$PROPERTY_OBJECTS_HOME', 'bin/compute_dmp_esp.py')
    )
    cmp_esp_script=get(
        ('$PROPERTY_OBJECTS_HOME', 'bin/compare_esp.py')
    )

    python_envs={
        'psi4'                  : ('psi4',psi4_script),
        'horton'                : ('horton_part',horton_script),
        'iso_density_surface'   : ('qcDENS', isod_script),
        'density_esp'           : ('qcDENS', rho_esp_script),
        'multipolar_esp'        : ('qcPROP', dmp_esp_script),
        'esp_comparison'        : ('qcPROP', cmp_esp_script)
    }

    for k, v in python_envs.items():
        check_conda_env(v[0])
        envs.update({k: {'python_env': v[0], 'script':v[1]}})
    
    envs['density_esp'].update({'shell_env':
        {'QC_DENSITY_OPERATIONS_LIB': qc_dens_lib}
    })



    return envs

def setup():
    whole_dic={}
    dic=setup_env()
    whole_dic.update({'global':dic})
    
    env_file='environment_for_server_config.yaml'
    with open(env_file, 'w') as wr:
        yaml.safe_dump(whole_dic, wr)
        string=yaml.safe_dump(whole_dic)



if __name__=='__main__':
    setup()
