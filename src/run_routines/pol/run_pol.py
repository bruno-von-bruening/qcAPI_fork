from . import *

from ..psi4.run_psi4_base import run_psi4_base

@val_call
def compute_polarizability_psi4(
    python, psi4_script, tracker:Tracker, 
    record: Molecular_Polarizability, wave_function:Wave_Function, geom:geometry
) -> Callable:
    job_tag=f"polarizability_finite_field"

    try:
        wfn_record, run_data, run_info, sub_entries = run_psi4_base(python, psi4_script, tracker, wave_function, geom, job_tag)
    except: raise Exception(f"Error in generic psi4 loop:\n{ex}")


    try:
        sub_entries.update({
            Wave_Function.__name__:wfn_record
        })

        record.converged=wfn_record.converged

        assert 'storage_file' in run_data.files.keys(), f"Expected"
        storage_file=run_data.files['storage_file']

        storage_data=load_json_or_yaml(storage_file)
        tag='pol_ff'
        assert 'pol_ff' in storage_data, f"Key \'pol_ff\' not in storage file {storage_file}" 
        from qcp_objects.objects.properties import polarizability_tensor
        try:
            pol=polarizability_tensor(storage_data[tag])
        except Exception as ex: raise Exception(f"{ex}")

        tensor=str(pol.tensor_elements)
        record.tensor=tensor
        results=job_results(
            run_data=run_data,
            record=record,
            run_info=run_info,
            sub_entries=sub_entries,
        )
        return results
    except Exception as ex:
        raise Exception(f"Error in recovering results {ex}")


    
