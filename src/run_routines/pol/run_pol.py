from . import *

from ..psi4.run_psi4_base import run_psi4_base
# from ..psi4.run_psi4_help import open_storage_file
from qcp_objects.objects.properties import polarizability_tensor, MolecularMultipoleMoments
from ..psi4.run_psi4_help import open_storage_file, get_fchk_file, get_from_storage
from orm_import.database_declaration import (
    Molecular_Multipoles,
    Molecular_Polarizability,
)

@val_call
def compute_polarizability_psi4(
    python, psi4_script, tracker:Tracker, 
    record: Molecular_Polarizability, wave_function:Wave_Function, geom:geometry
) -> job_results:

    extra_cmdln_opts=dict(
            no_freeze_core=True, no_df=True, no_mom_ff=True, unrestricted=True
    )
    try: # Run the psi4 calculation
        job_tag=f"polarizability_finite_field"
        tracker, wfn_record, run_data = run_psi4_base(python, psi4_script, tracker, wave_function, geom, job_tag,
                                                      extra_cmdln_opts=extra_cmdln_opts)
        sub_entries={}
        files_for_entries={ }
    except Exception as ex: raise Exception(f"Error in generic psi4 loop:\n{ex}") from ex

    # Now the results are there and we recover the data for the polarizability object 

    converged= ( wfn_record.converged==RecordStatus.converged )

    if converged:
        try: # Inherit data from wave function to polarizability record
        
            storage_file=run_data.files['storage_file']

            tracker, di=get_fchk_file(tracker,storage_file, id=wfn_record.id)
            files_for_entries.update( **di )
            
            pols=dict()
            center=get_from_storage(storage_file, ['results','properties','expansion_center'])
            for tag in ['dens','eng']:
                key=f"MolPol_FinFie_through_{tag}"
                try:
                    pol_data=get_from_storage(storage_file, ['results','properties',key])
                except Exception as ex:
                    warn(f"Could not get {key} from {storage_file}: {str(ex)}")
                    continue
                pol_tensor=None
                if pol_data is not None:
                    if len(pol_data)>0:
                        try:
                            pol_tensor=polarizability_tensor(pol_data)
                        except Exception as ex: 
                            raise Exception(f"Could not generate \'{tag}\' polarizability tensor: {ex}") from ex
                        pols.update({ tag : pol_tensor})
            if len(pols)==0:
                raise Exception(f"Could find neither energy not density moments polarisabilities!")

            # if dens in pols that's the main object, otherwise create a new one
            if 'dens' in pols.keys():
                tensor=pols['dens']
                specs='density_evaluation'
                if 'eng' in pols.keys(): # If energy also there make a side entry
                    new_rec=record.model_dump()
                    tens2=pols['eng']
                    specs='energy'
                    new_rec.update(
                            id=None, # will be auto assigned
                            specs=json.dumps(f"obtained_from: {specs}"),
                            expansion_center=' '.join([ str(x) for x in center]),
                            tensor_elements=str(tens2.tensor_elements),
                            induced_ranks=' '.join( [ str(x) for x in tens2.induced_ranks]),
                            field_ranks=' '.join( [ str(x) for x in tens2.field_ranks]),
                    )
                    sub_entries.update({
                        Molecular_Polarizability.__name__ : MolecularPolarizability(**new_rec)
                    })

            else:
                tensor=pols['eng']
                specs='energy'



            try:
                mom=get_from_storage(storage_file, ['results','properties','MolMom'])
            except:
                warn(f"Could not recover molecular moments (may be impossible for CCSD(T) without DF)")
                mom=None
            if not mom is None:
                try:
                    mom=MolecularMultipoleMoments(mom, expansion_center=center, 
                                                 type_of_center=MolecularMultipoleMoments.__allowed_centers__.CONC)
                except Exception as ex:
                    raise Exception(f"Could not generate molecular multipole moment: {ex}") from ex

            

        except Exception as ex:
            raise Exception(f"Error in recovering polarizability data from storage file {analyse_exception(ex)}") from ex
    else:
        tensor=None
        mom=None
        specs=None
        center=None

    try:
        wfn_record.side_result_from=f"{Molecular_Polarizability.__name__}%{record.id})"
        sub_entries.update({
            Wave_Function.__name__:wfn_record
        })
        record.converged=wfn_record.converged
        if tensor is not None: # otherwise keep the defaults
            record.tensor_elements=str(tensor.tensor_elements)
            record.induced_ranks=' '.join( [ str(x) for x in tensor.induced_ranks])
            record.field_ranks=' '.join( [ str(x) for x in tensor.field_ranks])
            if not specs is None: record.specs=json.dumps(f"obtained_from: {specs}")
            if not center is None: record.expansion_center=' '.join([str(x) for x in center])

        if mom is not None:
            the_mom=Molecular_Multipoles.from_object(mom, wfn_id=wfn_record.id)
            sub_entries.update({
                Molecular_Multipoles.__name__:the_mom
            })
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


    
