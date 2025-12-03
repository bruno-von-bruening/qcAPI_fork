from . import *

from ..psi4.run_psi4_base import run_psi4_base
# from ..psi4.run_psi4_help import open_storage_file
from qcp_objects.objects.properties import polarizability_tensor
from ..psi4.run_psi4_help import open_storage_file, get_fchk_file


@val_call
def compute_polarizability_psi4(
    python, psi4_script, tracker:Tracker, 
    record: Molecular_Polarizability, wave_function:Wave_Function, geom:geometry
) -> job_results:


    try: # Run the psi4 calculation
        job_tag=f"polarizability_finite_field"
        tracker, wfn_record, run_data = run_psi4_base(python, psi4_script, tracker, wave_function, geom, job_tag)
        sub_entries={}
    except Exception as ex: raise Exception(f"Error in generic psi4 loop:\n{ex}") from ex

    # Now the results are there and we recover the data for the polarizability object 

    converged= ( wfn_record.converged==RecordStatus.converged )

    if converged:
        try: # Inherit data from wave function to polarizability record
        
            storage_file=run_data.files['storage_file']
            storage_data=open_storage_file(storage_file)

            sub_entries.update( **get_fchk_file(storage_file, id=wfn_record.id) )
            
            tag='pol_ff'
            assert tag in storage_data.keys(), f"Expected key '{tag}' in storage data from file {storage_file}"
            data=storage_data[tag]


            try:
                tensor=polarizability_tensor(data)
            except Exception as ex: 
                raise Exception(f"Could not generate polarizability tensor: {ex}") from ex
            

        except Exception as ex:
            raise Exception(f"Error in recovering polarizability data from storage file {analyse_exception(ex)}") from ex
    else:
        tensor=None

    try:
        wfn_record.messages=json.dumps(
            [f"Filled through {Molecular_Polarizability.__name__} (id={record.id})"]+json.loads(wfn_record.messages)
        )
        sub_entries.update({
            Wave_Function.__name__:wfn_record
        })
        record.converged=wfn_record.converged
        if tensor is not None: # otherwise keep the defaults
            record.tensor_elements=str(tensor.tensor_elements)
            record.induced_ranks=' '.join( [ str(x) for x in tensor.induced_ranks])
            record.field_ranks=' '.join( [ str(x) for x in tensor.field_ranks])
    except Exception as ex:
        raise Exception(f"Error in updating polarizability record:\n {ex}") from ex

    try:
        keys=['messages','errors','warnings']
        for key in keys:
            try:
                setattr(record, key, getattr(tracker, key))
            except:
                warn(f"Could not copy attribute {key} from {type(tracker)} to {type(record)}")

        run_info={'status':tracker.status, 'status_code':tracker.status_code}
        results=job_results(
            run_data=run_data,
            record=record,
            run_info=run_info,
            sub_entries=sub_entries,
        )
        return results
    except Exception as ex:
        raise Exception(f"Error in formatting results:\n {ex}") from ex


    
