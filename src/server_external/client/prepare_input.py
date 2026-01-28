from . import *

from .util import get_python_exc_and_script

from run_routines.wave_function.run_wfn import compute_wave_function
from run_routines.pol.run_pol import compute_polarizability_psi4

from qcp_objects.objects.properties import geometry

# @val_call
# def prepare_script(config_file: pdtc_file, serv_adr, record, UNIQUE_NAME:str, max_iter:int) -> Callable:
#     # Recover data for production
#     prod_key='production_data' # Key under which production data is dumped
#     assert prod_key in record.keys(), f"Could not find key \'{prod_key}\' in record keys ({list(record.keys())})"
#     prod_data=record.pop(prod_key)
# 
#     if UNIQUE_NAME==NAME_WFN:
#         script = prepare_wfn_script(config_file, record, serv_adr, max_iter=max_iter)
#     elif NAME_PART==UNIQUE_NAME:
#         script = prepare_part_script(config_file, record, serv_adr, max_iter=max_iter)
#     elif UNIQUE_NAME==NAME_IDSURF:
#         fchk_file=prod_data['fchk_file']
#         script = prepare_idsurf_script(config_file, fchk_file=fchk_file)
#     elif NAME_ESPRHO==UNIQUE_NAME:
#         fchk_file=prod_data['fchk_file']
#         surface_file=prod_data['surface_file']
#         script = prepare_espmap_script(config_file, fchk_file=fchk_file, surface_file=surface_file)
#     elif NAME_ESPDMP==UNIQUE_NAME:
#         moment_file=prod_data['moment_file']
#         surface_file=prod_data['surface_file']
#         script = prepare_espdmp_script(config_file, moment_file=moment_file, surface_file=surface_file)
#     elif NAME_ESPCMP == UNIQUE_NAME:
#         rho_map_file=prod_data['rho_map_file']
#         dmp_map_file=prod_data['dmp_map_file']
#         script = prepare_espcmp_script(config_file, dmp_map_file=dmp_map_file, rho_map_file=rho_map_file)
#     elif NAME_DISPOL == UNIQUE_NAME:
#         script=prepare_dispol_script(config_file, prod_data, serv_adr)
#     else:
#         raise Exception(f"No routine defined for {UNIQUE_NAME}")
#     return script
#     # After extraction clean the record

@val_call
def get_geometry(compound:Compound,conformation:Conformation)-> geometry:

    # request_code=f"{serv_adr}/get/Conformation?ids={id}&links=Compound"
    # response=requests.get(request_code)
    # status_code=response.status_code
    # if status_code!=HTTPStatus.OK:
    #     raise Exception(f"Failed to get geometry (request={request_code} status_code={status_code}, error={response.text})")

    # # Get the results out
    # entries=response.json()['json']['entries']
    # assert len(entries)==1, f"Expected one return for id={id} got {len(entries)}"
    # links=response.json()['json']['entries'][0]['Compound']
    # assert len(links)==1, f"Found not one but {len(links)} Compounds for Conformation: {links}"
    # comp=links[0]
    # conf = entries[0]['Conformation']
    # conf = Conformation(**conf).to_dict()
    

    try:

        geom=conformation.to_geometry(charge=compound.charge, multiplicity=compound.multiplicity)
        #geometry(
        #    coordinates     =conformation.coordinates_from_string,
        #    multiplicity    =compound.multiplicity,
        #    atom_types      =conformation.elements_from_string,
        #    charge          =compound.charge,
        #    length_units='BOHR',
        #)
        return geom
    except Exception as ex: raise Exception(f"Could not get geometry into shape! {ex}")


def prepare_wfn_script(tracker:Tracker, record:sqlmodel_meta, max_iter=None):
    serv_adr=tracker.server_address
    config_file=tracker.config_file
    
    from qcp_objects.objects.properties import geometry

    # Get geometry
    id=record.conformation_id
    # conf_id_key='conformation_id'
    # assert conf_id_key in record.keys()
    # id=record[conf_id_key]
    geom=get_geometry(id)

    tag='run_psi4'
    python_exc, psi4_script=get_python_exc_and_script(config_file,tag)

    script=partial(compute_wave_function, python_exc, psi4_script, geom=geom, max_iter=max_iter)
    return script

@val_call
def prepare_molpol_script(tracker:Tracker, record:SQLModel, code:Code, conformation, compound, wave_function):
    geom=get_geometry(compound, conformation) # Pass either id or if conf and comp provided get that

    if code.code=='run_psi4':
        python_exc, psi4_script=get_python_exc_and_script(tracker.config_file,code.code)
        script=compute_polarizability_psi4
        return partial(script, python_exc, psi4_script, wave_function=wave_function, geom=geom, code=code)
    else:
        raise Exception(f"Requested unkown code: {record.code}")

def prepare_part_script(config_file, record, serv_adr, max_iter=None):

    def get_wave_function_file(id):
        request_code=f"{serv_adr}/get/FCHK_File?ids={id}"
        response=requests.get(request_code)
        status_code=response.status_code
        if status_code!=HTTPStatus.OK:
            raise Exception(f"Failed to get fchk (request={request_code} status_code={status_code}, error={response.text})")
        fchk_info=response.json()['json']['entries']
        assert len(fchk_info)==1, f"Did not found exately one FCHK_file for {id}: {fchk_info}"



# def must_be_wave_function(v):
#     if not isinstance(v, Wave_Function):
#         raise TypeError(f"Expected Wave_Function instance, got {type(v).__name__}")
#     return v
# def must_be_sqlmodel_instance(v):
#     if not issubclass(type(v), SQLModel):
#         raise TypeError(f"Expected instance of subclass of {SQLModel}, got instance of {type(v)}")
#     return v
# 
# WF = Annotated[Wave_Function, BeforeValidator(must_be_wave_function)]
# sqlmodel_inst = Annotated[, BeforeValidator(must_be_wave_function)]

@val_call
def prepare_script(tracker:Tracker, results:return_data, max_iter:int|None=None) -> Callable:

    config_file=tracker.config_file
    serv_adr=tracker.server_address

    UNIQUE_NAME=type(results.record).__name__
    # Decide which function to use and define arguments
    # prod_key='production_data' # Key under which production data is dumped
    # assert prod_key in record.keys(), f"Could not find key \'{prod_key}\' in record keys ({list(record.keys())})"
    # prod_data=record[pod_key]
    # del record[prod_key]

    if UNIQUE_NAME==Wave_Function.__name__:
        script = prepare_wfn_script(tracker, results.record, max_iter=max_iter)
    elif NAME_PART==UNIQUE_NAME:
        script = prepare_part_script(config_file, record, serv_adr, max_iter=max_iter)
    elif UNIQUE_NAME==NAME_IDSURF:
        fchk_file=prod_data['fchk_file']
        script = prepare_idsurf_script(config_file, fchk_file=fchk_file)
    elif NAME_ESPRHO==UNIQUE_NAME:
        fchk_file=prod_data['fchk_file']
        surface_file=prod_data['surface_file']
        script = prepare_espmap_script(config_file, fchk_file=fchk_file, surface_file=surface_file)
    elif NAME_ESPDMP==UNIQUE_NAME:
        moment_file=prod_data['moment_file']
        surface_file=prod_data['surface_file']
        script = prepare_espdmp_script(config_file, moment_file=moment_file, surface_file=surface_file)
    elif NAME_ESPCMP == UNIQUE_NAME:
        rho_map_file=prod_data['rho_map_file']
        dmp_map_file=prod_data['dmp_map_file']
        script = prepare_espcmp_script(config_file, dmp_map_file=dmp_map_file, rho_map_file=rho_map_file)
    elif NAME_DISPOL == UNIQUE_NAME:
        script=prepare_dispol_script(config_file, prod_data, serv_adr)
    elif NAME_MOLPOL == UNIQUE_NAME:
        kwargs= {}
        for k in [Compound,Conformation, Wave_Function, Code]:
            try:
                kwargs[k.__name__.lower()]=results.sub_entries[k.__name__]
            except Exception as ex: raise Exception(f"Expected \'{k}\' in provided sub_entries, got: {list(results.sub_entries.keys())}")
        script = prepare_molpol_script(tracker,results.record, **kwargs)
    else:
        raise Exception(f"No routine defined for {UNIQUE_NAME}")

    
    script=partial(script, tracker, results.record)
    return script

def prepare_idsurf_script(config_file, fchk_file):

    # Get the config for the current file
    idsurf_key='iso_density_surface'
    python_exc, script_exc=get_python_exc_and_script(config_file,idsurf_key)

    script=partial(run_isodens_surf, python_exc, script_exc, fchk_file)
    return script
def prepare_espmap_script(config_file, fchk_file, surface_file):
    """ """
    # Get the config for the current file
    the_key='density_esp'
    the_script=run_esp_surf

    python_exc, script_exc=get_python_exc_and_script(config_file,the_key)

    mode='fortran'
    if mode=='fortran':
        worker_config=load_worker_config(config_file)
        shell_env_key='shell_env'
        shell_env=worker_config.query( ('environment',the_key, shell_env_key))
    else:
        shell_env=None

    script=partial(the_script, python_exc, script_exc, fchk_file, surface_file, shell_env=shell_env, mode=mode)
    return script
@validate_call
def prepare_espdmp_script(
    config_file:str, moment_file:str, surface_file:str#, ranks:str
):
    """ """
    the_script=run_dmp_esp
    the_key='multipolar_esp'
    python_exc, script_exc=get_python_exc_and_script(config_file,the_key)

    script=partial(the_script, python_exc, script_exc, moment_file, surface_file)
    return script

def prepare_espcmp_script(config_file:file, dmp_map_file:file, rho_map_file:file):
    the_script=run_espcmp
    
    the_key='esp_comparison'
    query=('environment', the_key)
    python_exc, script_exc=get_python_exc_and_script(config_file,the_key)

    script=partial(the_script, python_exc, script_exc, dmp_map_file, rho_map_file)
    return script

#@val_call
def prepare_dispol_script(
    config_file, run_data, address
):
    try:
        fchk_file_id=run_data['fchk_file_id']
        partitioning_entry=run_data['part']
        part_weights=run_data['part_weights'] 
        wfn_entry=run_data['wfn_entry']
    except Exception as ex: raise Exception(ex)

    worker_config=load_worker_config(config_file)

    the_key='camcasp'
    python_exc, script_exc=get_python_exc_and_script(config_file,the_key)


    the_script=run_dispol
    script=partial(the_script, python_exc, script_exc, address, wfn_entry, fchk_file_id, partitioning_entry, part_weights)
    return script