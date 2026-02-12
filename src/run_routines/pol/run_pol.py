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
    route=('results','properties','MolMom')
    return get_moments_base(storage_file, route)
def get_moments_from_energy(storage_file:pdtc_file):
    route=('results','properties','MolMom_FinFie_through_eng')
    return get_moments_base(storage_file, route)
    


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
        sub_entries={}
        files_for_entries={ }
    except Exception as ex: raise Exception(f"Error in generic psi4 loop:\n{ex}") from ex

    # Now the results are there and we recover the data for the polarizability object 

    converged= ( wfn_record.status==RecordStatus.succeeded )
    record.status=wfn_record.status

    if converged:
        try: # Inherit data from wave function to polarizability record
        
            storage_file=run_data.files['storage_file']
            center=get_center_from_storage(storage_file)

            tracker, di=get_fchk_file(tracker,storage_file, id=wfn_record.id)
            files_for_entries.update( **di )
            
            pols=dict()

            def get_pol(key) -> polarizability_tensor|None:
                try:
                    pol_data=get_from_storage(storage_file, ('results','properties',key))
                except Exception as ex:
                    warn(f"Could not get {key} from {storage_file}: {str(ex)}")
                    return None

                if len(pol_data)==0:
                    raise Exception(f"Data of length zero found for polarizability key \'{key}\' in storage file {storage_file}!")

                try:
                    pol_tensor=polarizability_tensor(pol_data)
                    return pol_tensor
                except Exception as ex: 
                    raise Exception(f"Could not generate \'{tag}\' polarizability tensor: {ex}") from ex
            
            if record.approach == record.allowed_approaches.linear_response.value:
                key="MolPol_LinRsp"
                tensor=get_pol(key)
            else:
                # pol_tens=dict()
                for tag in ['dens','eng']:
                    key=f"MolPol_FinFie_through_{tag}"

                    pol_tens=get_pol(key)
                    if pol_tens:
                        pols.update({ tag : pol_tens})

                if len(pols)==0:
                    raise Exception(f"Could find neither energy not density moments polarisabilities!")

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

                if main_key in pols.keys():
                    tensor=pols[main_key]
                    center=get_center_from_storage(storage_file)
                    if side_key in pols.keys():
                        old_specs.eval_through=(spec_model.allowed_eval_from.energy if side_key=='eng' else spec_model.allowed_eval_from.multipoles)
                        tensor_side=pols[side_key]
                        new_rec=record.model_dump()
                        new_rec.update(
                            id=None,  # will be auto assigned
                            side_result_from=f"{Molecular_Polarizability.__name__}%{record.id}",
                            specs=old_specs.model_dump(),
                            expansion_center=' '.join([str(x) for x in center]),
                            tensor_elements=str(tensor_side.tensor_elements),
                            induced_ranks=' '.join([str(x) for x in tensor_side.induced_ranks]),
                            field_ranks=' '.join([str(x) for x in tensor_side.field_ranks]),
                            status=record.status,
                            specs_hash=None,
                        )
                        sub_entries.update({
                            Molecular_Polarizability.__name__ : Molecular_Polarizability(**new_rec)
                        })
                elif side_key in pols.keys():
                    tensor=pols[side_key]
                    spec_model.eval_through=(spec_model.allowed_eval_from.energy if main_key=='eng' else spec_model.allowed_eval_from.density)
                    record.specs=json.dumps( spec_model.model_dump() )
                else: raise Exception(f"Could no polarizability record in storage file {storage_file}!")


            # They are allways around
            mom_eng=get_moments_from_energy(storage_file)
            # They do not allways get computed
            try:
                mom_dens=get_moments_from_density(storage_file)
            except Exception as ex:
                warn(f"Could not recover molecular moments (may be impossible for CCSD(T) without DF)")
                mom_dens=None

        except Exception as ex:
            raise Exception(f"Error in recovering polarizability data from storage file {analyse_exception(ex)}") from ex

    else:
        tensor=None
        mom_eng=None
        mom_dens=None
        center=None

    try:
        this_job=f"{Molecular_Polarizability.__name__}%{record.id}"
        
        wfn_record.side_result_from=this_job
        sub_entries.update({
            Wave_Function.__name__:wfn_record
        })
        if tensor is not None: # otherwise keep the defaults
            record.tensor_elements=str(tensor.tensor_elements)
            record.induced_ranks=' '.join( [ str(x) for x in tensor.induced_ranks])
            record.field_ranks=' '.join( [ str(x) for x in tensor.field_ranks])
            if not center is None: record.expansion_center=' '.join([str(x) for x in center])


        sub_entries.update({
            Molecular_Multipoles.__name__:[]
        })
        if mom_dens:
            the_mom=Molecular_Multipoles.from_object(mom_dens, wfn_id=wfn_record.id,
                evaluated_through=Molecular_Multipoles.__eval_kind__.density, side_result_from=this_job
            )
            sub_entries[Molecular_Multipoles.__name__].append(the_mom)
        if mom_eng:
            the_mom=Molecular_Multipoles.from_object(mom_eng, wfn_id=wfn_record.id,
                evaluated_through=Molecular_Multipoles.__eval_kind__.energy, side_result_from=this_job
            )
            sub_entries[Molecular_Multipoles.__name__].append(the_mom)
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



