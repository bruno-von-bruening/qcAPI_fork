#!/usr/bin/env python

import os, sys, re, time
import psi4

BOHR_TO_ANGSTROM=.52917721067

def set_hardware(memory='4GB', num_threads=4):
    content=[]
    content+=[f"# Hardware settings "]
    content+=[f"set_memory( \"{memory}\" )"]
    content+=[f"set_num_threads( {num_threads} )"]
    content=''.join([ f"{line}\n" for line in content])
    return content

def molecule_input(atom_types, coordinates, units=None, charge=0, multiplicity=1, name='monomer'):

    if isinstance(units, type(None)):
        units={'LENGTH':'BOHR'}
    content=[]

    # Comment (makes it easier to copy this block into xyz
    content+=[f"# Number of nuclei: {len(coordinates)}"]

    # Charge and multiplicity
    content+=[f"{charge} {multiplicity} # Charge and Multiplicity"]
    
    # Make geometry
    geometry=[]
    for at_ty, coord in zip(atom_types, coordinates):
        assert len(at_ty)<3 and at_ty!=''
        coord=[ f"{c:.8f}" for c in coord]
        coord=[ f"{c:>15}" for c in coord]
        geometry+=[ f"{at_ty.capitalize():<2} {' '.join(coord)}"]
    content+=geometry


    content+=[
        'Symmetry C1',
        'nocom',
        'noreorient',
        f"units {''.join(list(units.values()))}"
    ]

    box=f"molecule {name} {{\n__REP__}}\n"
    spacer=4* ' '
    content=''.join([ f"{spacer}{line}\n" for line in content])
    string=box.replace('__REP__', content)
    return string

def calc_string(dft_functional, basis, molecule, reference='rks', en_var='E', wfn_var='wfn', marker='NO_MARKER'):
    freeze_core=True
    content=[]
    content+=[
        f"start=time()",
    ]
    content+=[
        f"set reference {reference}",
        f"set basis {basis}",
        f"set freeze_core {freeze_core}",
    ]
    content+=[ f"{en_var}, {wfn_var} = energy(\"{dft_functional}\", molecule={molecule}, return_wfn=True)" ]
    identifier=f"TIMING {marker:<15}"
    content+=[ 
        f'duration=time()-start' ,
        f"""psi4.print_out(f\"\"\"
{identifier} {'[in seconds]':<15} : {{duration}}
{identifier} {'[in H:M:S]':<15} : {{int(duration//3600)}}-{{int(duration%3600//60):02d}}-{{duration%60}}
        \"\"\")"""
    ]


    string=''.join([f"{x}\n"  for x in content])
    return string


def grac_shift(E_ion, E_neut, wfn_neut, verbose=True):
    ac_var=f"AC_shift"
    homo_var=f"E_homo"
    
    string=[     f"{homo_var} = {wfn_neut}.epsilon_a_subset('AO', 'ALL').np[{wfn_neut}.nalpha()-1]"
        ,f"IP = {E_ion} - {E_neut}"
        ,f"{ac_var} = IP + {homo_var}"
        ,f"set dft_grac_shift {ac_var}"
    ]
    string='\n'.join(string)

    if verbose:
        information_string=[f"Information about GRAC calculation"]
        information_string+=[
            f"IP {{IP}}",
            f"E_homo {{{homo_var}}}",
            f"GRAC_shift {{{ac_var}}}",
        ]
        information_string='\n'.join(information_string)
        string+=f"\npsi4.print_out(f\"\"\"{information_string}\"\"\")\n"
    return string

def psi4_settings(output_file):
    local_content=['# Non-hardware related psi4 settings']
    local_content.append(f"set_output_file(\"{output_file}\")")
    return '\n'.join(local_content)

def psi4_exit_ana(output_file):
    with open(output_file, 'r') as rd:
        lines=rd.readlines()

    normal_exit_line_fl=bool(re.match(r'\*\*\* Psi4 exiting successfully[.]*^', lines[-1]))

    if normal_exit_line_fl:
        exit_status='normal'
    else:
        exit_status='failed'
    return {
        'exit_status': exit_status
    }

                    
def complete_calc(
    atom_types, coordinates, charge=0, multiplicity=1, # structure
    dft_functional='PBE0', basis_set='aug-cc-pVTZ', do_grac=True, # calculation
    jobname='test', units=None,
    hardware_settings={'memory':'4GB', 'num_threads':4}
):
    content=[]

    # Comment
    the_time=f"{time.strftime('%Y-%M-%D_%H:%M:%S', time.localtime())} (Zone: {time.tzname})"
    comment_string=f"# Psi4 calculation for job {jobname} input file\n# generated at {the_time}"
    content+=[comment_string, '']

    content+=[f"from time import time",'']

    #Set the hardware
    hardware_string=set_hardware(**hardware_settings)
    content+=[hardware_string,'']

    # Set psi4 settings
    output_file=f"{jobname}.psi4out"
    psi4_settings_string=psi4_settings(output_file=output_file)
    content+=[psi4_settings_string,'']

    ## Set the molecule data
    dat_monomer={
        'name':'monomer',
        'charge':charge,
        'multiplicity':multiplicity,
    }
    string=molecule_input(atom_types, coordinates, **dat_monomer, units=units)
    content+=[string]
    if do_grac:
        # Shift charge and multiplicity
        charge_ion=charge+1
        if multiplicity!=1:
            raise Exception(f"Can only handle multiplicty of 1 for GRAC at the moment!")
        else:
            multiplicity_ion=2

        dat_monomer_ion={
            'name':         'monomer_ion',
            'charge':       charge_ion,
            'multiplicity': multiplicity_ion,
        }
        string_ion=molecule_input(atom_types, coordinates, **dat_monomer_ion, units=units)
        content+=[string_ion]

    #### Calculations
    calc_neut={
        'molecule':'monomer',
        'reference':'rks',
        'en_var':'E_neut',
        'wfn_var':'wfn_neut'
    }
    string_calc_neut = calc_string(dft_functional, basis_set, **calc_neut, marker='SCF_NEUTRAL')
    content+=[f"#Calculate neutral molecule", string_calc_neut]
    if do_grac:
        calc_ion={
            'molecule':'monomer_ion',
            'reference':'uks',
            'en_var':'E_ion',
            'wfn_var':'wfn_ion'
        }
        string_calc_ion = calc_string(dft_functional, basis_set, **calc_ion, marker='SCF_ION')
        content+=[f"#Calculate ion", string_calc_ion]
        calc_grac={
            'molecule':'monomer',
            'reference':'rks',
            'en_var':'E_grac',
            'wfn_var':'wfn_grac'
        }
        # Set acs
        ac_string = grac_shift( E_ion=calc_ion['en_var'], E_neut=calc_neut['en_var'], wfn_neut=calc_neut['wfn_var'])
        string_calc_grac = calc_string(dft_functional, basis_set, **calc_grac, marker='SCF_GRAC')
        content+=[f"# GRAC SHIFT CALCULATION\n"+ac_string+f"# GRAC CORRECTED SCF\n"+string_calc_grac]

    ### Save wave function
    wfn_file=f"{jobname}.wfn.npy"
    fchk_file=f"{jobname}.fchk"
    if do_grac:
        wfn_var=calc_grac['wfn_var']
    else:
        wfn_var=calc_neut['wfn_var']
    content+=[f"# Save wave function to file"]
    content+=[f"{wfn_var}.to_file(\"{wfn_file}\")"]
    content+=[f"fchk({wfn_var},\"{fchk_file}\")"]

    ### Print file
    psi4_path=f"{jobname}.psi4"
    content='\n'.join(content)
    with open(psi4_path,'w') as wr:
        wr.write(content)

    return {
        'psi4_input_file': os.path.realpath(psi4_path),
        'wfn_file': os.path.realpath(wfn_file),
        'output_file': os.path.realpath(output_file)
    }


def property_calc(wfn_file, method, basis, grac_shift=None):
    start_time=time.time()
    assert os.path.isfile(wfn_file)

    wfn=psi4.core.Wavefunction.from_file(wfn_file)
    mol=wfn.molecule()

    # We need to calculate that with GRAC shift
    if not isinstance(grac_shift, type(None)):
        psi4.set_options({ 'dft_grac_shift': grac_shift })
    if do_gradient:
        de, wfn = psi4.gradient(f"{method}/{basis}", molecule=mol, return_wfn=True)
        forces = -de.np/BOHR_TO_ANGSTROM
        e = wfn.energy()
    else:
        e, wfn = psi4.energy(f"{method}/{basis}", molecule=mol, return_wfn=True)
    
    props = [
        "dipole",
        "wiberg_lowdin_indices",
        "mayer_indices",
        "mbis_charges",
        # "MBIS_VOLUME_RATIOS",
    ]
    psi4.oeprop(wfn, *props, title="test")

    dipole = wfn.array_variable("SCF DIPOLE").np.flatten() * BOHR_TO_ANGSTROM
    mbis_charges = wfn.array_variable("MBIS CHARGES").np.flatten()
    mbis_dipoles = wfn.array_variable("MBIS DIPOLES").np * BOHR_TO_ANGSTROM
    mbis_quadrupoles = wfn.array_variable("MBIS QUADRUPOLES").np * BOHR_TO_ANGSTROM**2
    mbis_octupoles = wfn.array_variable("MBIS OCTUPOLES").np * BOHR_TO_ANGSTROM**3
    mbis_volumes = (
        wfn.array_variable("MBIS RADIAL MOMENTS <R^3>").np.flatten() * BOHR_TO_ANGSTROM**3
    )
    # mbis_volume_ratios = wfn.array_variable("MBIS VOLUME RATIOS").np.flatten()
    mbis_valence_widths = (
        wfn.array_variable("MBIS VALENCE WIDTHS").np.flatten() * BOHR_TO_ANGSTROM
    )
    wiberg_lowdin_indices = wfn.array_variable("WIBERG LOWDIN INDICES").np
    mayer_indices = wfn.array_variable("MAYER INDICES").np

    elapsed_time = time.time() - start_time
    psi4.core.clean()

    return dict(
        #energy=e,
        #forces=forces.tolist(),
        dipole=dipole.tolist(),
        mbis_charges=mbis_charges.tolist(),
        mbis_dipoles=mbis_dipoles.tolist(),
        mbis_quadrupoles=mbis_quadrupoles.tolist(),
        mbis_octupoles=mbis_octupoles.tolist(),
        mbis_volumes=mbis_volumes.tolist(),
        # mbis_volume_ratios=mbis_volume_ratios.tolist(),
        mbis_valence_widths=mbis_valence_widths.tolist(),
        wiberg_lowdin_indices=wiberg_lowdin_indices.tolist(),
        mayer_indices=mayer_indices.tolist(),
    )

    

if __name__=='__main__':
    pass
