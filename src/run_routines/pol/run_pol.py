from . import *

from ..psi4.run_psi4_base import run_psi4_base, job_opts
# from ..psi4.run_psi4_help import open_storage_file
from qcp_objects.objects.properties import polarizability_tensor, MolecularMultipoleMoments
from ..psi4.run_psi4_help import open_storage_file, get_fchk_file, get_from_storage, get_code_entry
from orm_import.database_declaration import (
    Molecular_Multipoles,
    Molecular_Polarizability,
)

def get_center_from_storage(storage_file:pdtc_file):
    center=get_from_storage(storage_file, ['results','properties','expansion_center'])
    return center

def get_moments_base(storage_file:pdtc_file, path:Tuple[str], critical=False) -> MolecularMultipoleMoments|None:
    try:
        mom=get_from_storage(storage_file, path)
    except Exception as ex:
        if not critical:
            warn(f"Could not get {path} from {storage_file}: {str(ex)}")
            return None
        else: raise Exception(f"Could not find moments for path {path} in storage file {storage_file}: {str(ex)}") from ex
    
    center=get_center_from_storage(storage_file)

    if mom:
        try:
            mom=MolecularMultipoleMoments(mom, expansion_center=center, 
                                            type_of_center=MolecularMultipoleMoments.__allowed_centers__.CONC)
        except Exception as ex:
            raise Exception(f"Could not generate molecular multipole moment: {ex}") from ex
    return mom

def get_moments_from_density(storage_file:pdtc_file):
    route=('results','properties','MolMom_from_dens')
    return get_moments_base(storage_file, route)
def get_moments_from_energy(storage_file:pdtc_file):
    route=('results','properties','MolMom_FinFie_through_eng')
    return get_moments_base(storage_file, route)
def get_pol(storage_file,key) -> polarizability_tensor|None:
    try:
        pol_data=get_from_storage(storage_file, ('results','properties',key))
    except Exception as ex:
        warn(f"Could not get {key} from {storage_file}: {str(ex)}")
        return None

    if pol_data is None: return pol_data # if none

    if len(pol_data)==0:
        raise Exception(f"Data of length zero found for polarizability key \'{key}\' in storage file {storage_file}!")

    try:
        pol_tensor=polarizability_tensor(pol_data)
        return pol_tensor
    except Exception as ex: 
        raise Exception(f"Could not generate polarizability tensor: {ex}") from ex
    

from typing import Generator, Any

def get_resource_data_from_storage(tracker,storage_file: pdtc_file, critical=False) -> dict:
    try:
        data = get_from_storage(storage_file, ('about_the_run', 'resource_usage'))
    except Exception as ex:
        raise Exception(f"Could not get resource usage data from storage file {storage_file}: {str(ex)}") from ex

    def my_get(key: str | Tuple[str]):
        # Make a recursive get that can handle nested keys
        def rec(dat, keys):
            if not isinstance(dat, dict):
                return None
            val = dat.get(keys[0], None)
            if len(keys) > 1:
                if not isinstance(val, dict):
                    return None
                val = rec(val, keys[1:])
            return val
        keys = [key] if isinstance(key, str) else list(key)
        return rec(data, keys) if data else None

    return dict(
        # Time for this calculation
        production_time   = my_get('real_time'),
        clocked_time      = my_get('clocked_time'),

        # Time ratios
        frac_idle         = my_get(('time_ratios', 'idle')),
        frac_serial       = my_get(('time_ratios', 'serial')),
        frac_parallel     = my_get(('time_ratios', 'parallel')),

        # Max CPU loads
        load_user_max     = my_get(('max_loads', 'cpu_user')),
        load_system_max   = my_get(('max_loads', 'cpu_system')),
        load_total_max    = my_get(('max_loads', 'cpu_total')),

        # Average CPU loads
        load_user_avr     = my_get(('avr_loads', 'serial', 'user')),   # no direct avr_loads total user → use serial+parallel sum if needed
        load_system_avr   = my_get(('avr_loads', 'serial', 'system')),
        load_total_avr    = my_get(('avr_loads', 'serial', 'total')),

        # Memory
        mem_use_max       = my_get('peak_memory'),
        mem_use_avr       = my_get('avr_memory'),

        # Disk
        disk_use_max      = my_get(('disk_usage', 'disk_max_in_GB')),
        disk_use_avr      = my_get(('disk_usage', 'disk_avr_in_GB')),
    )


@val_call
def compute_polarizability_psi4(
    python, psi4_script, tracker:Tracker, 
    record: Molecular_Polarizability, wave_function:Wave_Function, geom:geometry,
    code:Code,
) -> job_results:

    try: # Run the psi4 calculation
        if record.approach == record.allowed_approaches.finite_field.value:
            job_tag=job_opts.MOLPOL_FINITE_FIELD
        elif record.approach == record.allowed_approaches.linear_response.value:
            job_tag=job_opts.MOLPOL_LINEAR_RESPONSE
        else:
            raise Exception(f"Approach {record.approach} not implemented for psi4 polarizability calculations")
        tracker, wfn_record, run_data = run_psi4_base(python, psi4_script, tracker, wave_function, record, geom, job_tag, code,
                                                      )
        sub_entries={
            Molecular_Polarizability.__name__: [],
            Molecular_Multipoles.__name__: [],
        }
        files_for_entries={ }
    except Exception as ex: raise Exception(f"Error in generic psi4 loop:\n{ex}") from ex

    # Now the results are there and we recover the data for the polarizability object 

    converged= ( wfn_record.status==RecordStatus.succeeded )
    record.status=wfn_record.status
    this_job=f"{Molecular_Polarizability.__name__}%{record.id}"

    def make_new_id(old_wfn, new_method):
        new_wfn=old_wfn.model_dump(exclude=['protocol_hash'])
        new_wfn['method']=new_method
        new_wfn['id']=None # will be auto assigned
        return Wave_Function(**new_wfn).make_uid()


    try: # Inherit data from wave function to polarizability record (without this object there will be no data to recover!)
        storage_file=run_data.files['storage_file']
    except Exception as ex:
        if converged:
            tracker.add_error(f"Fatal Could not find storage file in run data files {run_data.files}: {str(ex)}")
            converged=False
        storage_file=None
    
    # Get info about resource usage
    if not storage_file is None:
        try:
            resource_info=get_resource_data_from_storage(tracker, storage_file)
            run_data.resource_usage=resource_info
        except Exception as ex:
            tracker.add_warning(f"Could not recover resource usage data from storage file {storage_file}: {str(ex)}")

    # Extract the actual data of the object
    if converged:
        try: # Inherit data from wave function to polarizability record
            center=get_center_from_storage(storage_file)
            tracker, di=get_fchk_file(tracker,storage_file, id=wfn_record.id)
            files_for_entries.update( **di )
            
            if record.approach == record.allowed_approaches.linear_response.value:
                key="MolPol_LinRsp"
                tensor_main=get_pol(storage_file,key)

                try:
                    mom_dens=get_moments_from_density(storage_file)
                    moms={'MAIN': {'dens': mom_dens}}
                except Exception as ex:
                    warn(f"Could not recover molecular moments (may be impossible for CCSD(T) without DF)")
                    moms={}
            else:
                extra_vals={
                    'ccsd(t)': ['ccsd','mp2','hf'],
                    'ccsd': ['mp2','hf'],
                    'mp2': ['hf'],
                }
                extra=extra_vals.get(wave_function.method.lower(),[])
                methods=['MAIN']+extra

                # Gather moments and polarizabiliteis
                pols,moms=dict(),dict()
                try:
                    zero_moms_done=get_from_storage(storage_file, ('settings','properties','did_zero_moms'))
                except Exception as ex:
                    warn(f"Could not recover zero moments status from storage (assume the have been computed): {ex}")
                    zero_moms_done=True
                try:
                    fd_moms_done=get_from_storage(storage_file, ('settings','properties','did_fd_moms'))
                except Exception as ex:
                    warn(f"Could not recover finite difference moments status from storage (assume the have been computed): {ex}")
                    fd_moms_done=True
                
                for k in methods:
                    for tag in ['dens','eng']:
                        if not (tag=='dens' and not fd_moms_done):
                            key=('' if k.lower()=='main' else f"{k.upper()}_")+ f"MolPol_FinFie_through_{tag}"

                            pol_tens=get_pol(storage_file,key)

                            if pol_tens:
                                if not k in pols.keys(): pols[k]={}
                                pols[k].update({ tag : pol_tens})

                        mom=None
                        if tag=='eng':
                            if not ( tag=='dens' and not zero_moms_done):
                                key= ('results','properties',
                                    ('' if k.lower()=='main' else f"{k.upper()}_")+f"MolMom_FinFie_through_{tag}")
                                mom=get_moments_base(storage_file, key) # at least the main energy moments should be there  

                        elif tag=='dens' and zero_moms_done:
                            if k.lower()=='main':
                                key= ('results','properties','MolMom_from_dens')
                                mom=get_moments_base(storage_file, key) # at least the main density moments should be there
                        if mom:
                            if not k in moms.keys(): moms[k]={}
                            moms[k].update({ tag : mom})
            


                if len([ v is not None for k,v in pols.items()])==0:
                    raise Exception(f"Could find neither energy not density moments polarisabilities!")
                if len([ v is not None for k,v in moms.items()])==0:
                    warn(f"Could find neither energy not density moments for polarizability calculation!")

                # if dens in pols that's the main object, otherwise create a new one
                spec_model=Molecular_Polarizability.specs_model_ff
                old_specs=spec_model(**json.loads(record.specs))
                if old_specs.eval_through==spec_model.allowed_eval_from.energy:
                    main_key='eng'
                    side_key='dens'
                elif old_specs.eval_through==spec_model.allowed_eval_from.multipoles:
                    main_key='dens'
                    side_key='eng'
                else: raise Exception(f"Unknown eval_through specification in polarizability record: {old_specs.eval_through}")

                tensor_main=None
                for method,v  in pols.items():
                    if v is None: continue

                    for k, tensor_loop in v.items(): # 'eng' or 'dens'
                        if tensor_loop is None: continue
                        if k==main_key and method.lower()=='main':
                            old_specs.eval_through=(spec_model.allowed_eval_from.energy if main_key=='eng' else spec_model.allowed_eval_from.density)
                            record.specs=json.dumps( old_specs.model_dump() )
                            tensor_main=tensor_loop
                        else:
                            new_specs=old_specs.model_copy()
                            new_specs.eval_through=(spec_model.allowed_eval_from.energy if k=='eng' else spec_model.allowed_eval_from.multipoles)
                            tensor_side=tensor_loop
                            new_rec=record.model_dump()
                            new_rec.update(
                                id=None,  # will be auto assigned
                                side_result_from=f"{Molecular_Polarizability.__name__}%{record.id}",
                                specs=new_specs.model_dump(),
                                expansion_center=' '.join([str(x) for x in center]),
                                tensor_elements=str(tensor_side.tensor_elements),
                                induced_ranks=' '.join([str(x) for x in tensor_side.induced_ranks]),
                                field_ranks=' '.join([str(x) for x in tensor_side.field_ranks]),
                                status=record.status,
                                specs_hash=type(record)._default,
                                # Do not provide spes hash it should be newly generated!
                            )
                            if not method.lower()=='main':
                                new_rec.update(
                                    wfn_id=make_new_id(wfn_record, method)
                                )
                            # if not Molecular_Polarizability.__name__ in sub_entries.keys(): sub_entries[Molecular_Polarizability.__name__]=[]
                            sub_entries[Molecular_Polarizability.__name__]+=[
                                Molecular_Polarizability(**new_rec)
                            ]
                        #else: raise Exception(f"Could no polarizability record in storage file {storage_file}!")
                if tensor_main is None:
                    raise Exception(f"Could not find main polarizability record (evaluated_through: {main_key}) in storage file {storage_file}!")

            for method,v in moms.items():
                if v is None: continue

                if method.lower()=='main': id=wfn_record.id
                else: id=make_new_id(wfn_record, method)

                for k, mom_loop in v.items(): # 'eng' or 'dens'
                    if mom_loop is None: continue
                    ev_kind=(Molecular_Multipoles.eval_kind.density if k=='dens' else Molecular_Multipoles.eval_kind.energy)
                    specs=record.specs_model_ff(**json.loads(record.specs)).model_dump(exclude=['eval_through'])

                    try: # If the Polarizabilities are not symmetric within a threshold this will raise an error! (catch that!)
                        the_mom=Molecular_Multipoles.from_object(mom_loop, wfn_id=id,
                            evaluated_through=ev_kind, side_result_from=this_job, specs=specs
                        )
                        sub_entries[Molecular_Multipoles.__name__]+=[ the_mom ]
                    except Exception as ex:
                        tracker.add_error(f"Could not generate Molecular_Multipoles object for method {method} and tag {k}: {ex}")
                        record.status=RecordStatus.failed


        except Exception as ex:
            raise Exception(f"Error in recovering polarizability data from storage file {analyse_exception(ex)}") from ex

    else:
        tensor_main=None
        center=None


    try:
        
        wfn_record.side_result_from=this_job
        sub_entries.update({
            Wave_Function.__name__:wfn_record
        })
        if tensor_main is not None: # otherwise keep the defaults
            record.tensor_elements=str(tensor_main.tensor_elements)
            record.induced_ranks=' '.join( [ str(x) for x in tensor_main.induced_ranks])
            record.field_ranks=' '.join( [ str(x) for x in tensor_main.field_ranks])
            if not center is None: record.expansion_center=' '.join([str(x) for x in center])


    except Exception as ex:
        raise Exception(f"Error in updating polarizability record:\n {ex}") from ex

    try:
        # keys=['messages','errors','warnings']
        # for key in keys:
        #     try:
        #         setattr(record, key, getattr(tracker, key))
        #     except:
        #         warn(f"Could not copy attribute {key} from {type(tracker)} to {type(record)}")

        run_info={'status':tracker.status, 'status_code':tracker.status_code}
        results=job_results(
            run_data=run_data,
            record=record,
            run_info=run_info,
            sub_entries=sub_entries,
            files_for_entries=files_for_entries,
        )
        return tracker,results
    except Exception as ex:
        raise Exception(f"Error in formatting results:\n {ex}") from ex



