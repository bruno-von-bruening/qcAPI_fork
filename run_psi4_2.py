#!/usr/bin/env python

import os, sys, re, time
import psi4, numpy as np

BOHR_TO_ANGSTROM=.52917721067
PERIODIC_TABLE_STR = """
H                                                                                                                           He
Li  Be                                                                                                  B   C   N   O   F   Ne
Na  Mg                                                                                                  Al  Si  P   S   Cl  Ar
K   Ca  Sc                                                          Ti  V   Cr  Mn  Fe  Co  Ni  Cu  Zn  Ga  Ge  As  Se  Br  Kr
Rb  Sr  Y                                                           Zr  Nb  Mo  Tc  Ru  Rh  Pd  Ag  Cd  In  Sn  Sb  Te  I   Xe
Cs  Ba  La  Ce  Pr  Nd  Pm  Sm  Eu  Gd  Tb  Dy  Ho  Er  Tm  Yb  Lu  Hf  Ta  W   Re  Os  Ir  Pt  Au  Hg  Tl  Pb  Bi  Po  At  Rn
Fr  Ra  Ac  Th  Pa  U   Np  Pu  Am  Cm  Bk  Cf  Es  Fm  Md  No  Lr  Rf  Db  Sg  Bh  Hs  Mt  Ds  Rg  Cn  Nh  Fl  Mc  Lv  Ts  Og
"""
atom_types=PERIODIC_TABLE_STR.split()
Z_to_atty=dict([ (i+1, at_ty) for i, at_ty in enumerate(atom_types)])
def atomic_charge_to_atom_type(Z):
    """ Return atom type when given an atomic Number """
    assert Z in Z_to_atty.keys()
    return Z_to_atty[Z]
def atom_type_to_Z(at_ty):
    """ Return integer atomic number for given atom type"""
    answer=[ Z for Z,at in Z_to_atty.items() if at.upper()==at_ty.upper()]
    assert len(answer)==1, answer
    answer=answer[0]
    return answer


def set_hardware(memory='4GB', num_threads=4, storage_var=None):
    content=[]
    content+=[f"# Hardware settings "]
    content+=[f"set_memory( \"{memory}\" )"]
    content+=[f"set_num_threads( {num_threads} )"]
    if not isinstance(storage_var, type(None)):
        content+=[ f"{storage_var}['about_the_run'].update({{'num_threads': {num_threads}, 'memory': \'{memory}\'}})"]
    content='\n'.join([ f"{line}" for line in content])
    return content

def molecule_input(atom_types, coordinates, units=None, total_charge=0, multiplicity=1, name='monomer'):

    if isinstance(units, type(None)):
        units={'LENGTH':'BOHR'}
    content=[]

    # Comment (makes it easier to copy this block into xyz
    content+=[f"# Number of nuclei: {len(coordinates)}"]

    # Charge and multiplicity
    content+=[f"{total_charge} {multiplicity} # Charge and Multiplicity"]
    
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
def molecule_string(atom_types, coordinates, units=None, total_charge=0, multiplicity=1):

    if isinstance(units, type(None)):
        units={'LENGTH':'BOHR'}
    content=[]

    # Comment (makes it easier to copy this block into xyz
    content+=[f"# Number of nuclei: {len(coordinates)}"]

    # Charge and multiplicity
    content+=[f"{total_charge} {multiplicity} # Charge and Multiplicity"]
    
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

    return '\n'.join(content)

def calc_string(dft_functional, basis, molecule, reference='rks', en_var='E', wfn_var='wfn', marker='NO_MARKER', do_timing=True,
                #save_wfn=False, jobname=None, tag=None
    ):
    
    #if save_wfn:
    #    assert not isinstance(jobname,type(None)), f"Save wave function requested but jobname not provided!"

    freeze_core=True
    content=[]
    if do_timing:
        content+=[
            f"start=time()",
        ]
    content+=[
        f"set reference {reference}",
        f"set basis {basis}",
        f"set freeze_core {freeze_core}",
    ]
    content+=[ f"{en_var}, {wfn_var} = energy(\"{dft_functional}\", molecule={molecule}, return_wfn=True)" ]

    if do_timing:
        identifier=f"TIMING {marker:<15}"
        content+=[
            f'# TIMING,',
            f'duration=time()-start' ,
            f"psi4.print_out(  \'\\n\'.join([",
                f"f\"{identifier} {'[in seconds]':<15} : {{duration}}\",",
                f"f\"{identifier} {'[in H:M:S]':<15} : {{int(duration//3600)}}-{{int(duration%3600//60):02d}}-{{duration%60}}\",",
            '])  )'
        ]
    #if save_wfn:
    #    content+=save_wfn_string(wfn_var=wfn_var, jobname=jobname, tag=tag)


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

    if verbose:
        ind=4*' '
        information_string=[f"Information about GRAC calculation",
            f"IP {{IP}}",
            f"E_homo {{{homo_var}}}",
            f"GRAC_shift {{{ac_var}}}",
        ]
        information_string='\n'.join(
            [f"\npsi4.print_out(  \'\\n\'.join([",]+
            [ind+'\"'+x+'\",' for x in information_string]+
            ["])  )",]
        )
        string+=[ information_string ]
    string='\n'.join(string)
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

def save_wfn_string(wfn_var, jobname, tag, save_fchk=True, save_npy=False):
    """ Return strings that saves wave function corresponding to provided variable name """
    local_content=[f"# Save wave function to file"]
    ### Save wave function
    if isinstance(tag, type(None)):
       tag=''
    else:
        tag=f"_{tag}" 

    if save_npy:
        wfn_file=f"{jobname}{tag}.wfn.npy"
        local_content+=[f"{wfn_var}.to_file(\"{wfn_file}\")"]
    else:
        wfn_file=None

    if save_fchk:
        fchk_file=f"{jobname}{tag}.fchk"
        local_content+=[f"fchk({wfn_var},\"{fchk_file}\")"]
    else:
        fchk_file=None

    return local_content, wfn_file, fchk_file
                    
def complete_calc(
    atom_types, coordinates, total_charge=0, multiplicity=1, # structure
    dft_functional='PBE0', basis_set='aug-cc-pVTZ', do_GRAC=True, # calculation
    jobname='test', units=None,
    hardware_settings={'memory':'4GB', 'num_threads':4}
):
    params=dict(
        save_standard_wfn=True,
    )
    save_npy=False
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
        'total_charge':total_charge,
        'multiplicity':multiplicity,
    }
    string=molecule_input(atom_types, coordinates, **dat_monomer, units=units)
    content+=[string]
    

    if do_GRAC:
        # Shift charge and multiplicity
        charge_ion=total_charge+1
        if multiplicity!=1:
            raise Exception(f"Can only handle multiplicty of 1 for GRAC at the moment!")
        else:
            multiplicity_ion=2

        dat_monomer_ion={
            'name':         'monomer_ion',
            'total_charge':       charge_ion,
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
    save_calc_neut, wfn_neut_file, fchk_neut_file= save_wfn_string(calc_neut['wfn_var'], jobname=jobname, tag='neut', save_npy=save_npy) 
    content+=[f"#Calculate neutral molecule", string_calc_neut, *save_calc_neut]


    if do_GRAC:
        calc_ion={
            'molecule':'monomer_ion',
            'reference':'uks',
            'en_var':'E_ion',
            'wfn_var':'wfn_ion'
        }
        string_calc_ion = calc_string(dft_functional, basis_set, **calc_ion, marker='SCF_ION')
        content+=[f"#Calculate ion", string_calc_ion]

        # Set acs
        ac_string = grac_shift( E_ion=calc_ion['en_var'], E_neut=calc_neut['en_var'], wfn_neut=calc_neut['wfn_var'])

        calc_grac={
            'molecule':'monomer',
            'reference':'rks',
            'en_var':'E_grac',
            'wfn_var':'wfn_grac'
        }
        string_calc_grac = calc_string(dft_functional, basis_set, **calc_grac, marker='SCF_GRAC')
        save_calc_grac, wfn_grac_file, fchk_grac_file= save_wfn_string(wfn_var=calc_grac['wfn_var'], jobname=jobname, tag='grac', save_npy=save_npy) 
        content+=[f"# GRAC SHIFT CALCULATION\n"+ac_string+f"\n# GRAC CORRECTED SCF\n"+string_calc_grac, *save_calc_grac]
    


    # Remove temporary files
    content+=['clean()']

    ### Print file
    psi4inp_path=f"{jobname}.psi4inp"
    content='\n'.join(content)
    with open(psi4inp_path,'w') as wr:
        wr.write(content)

    psi4_files= {
        'psi4inp_file': os.path.realpath(psi4inp_path),
        'psi4out_file': os.path.realpath(output_file),
        #'wfn_neut_file': os.path.realpath(wfn_neut_file),
        'fchk_neut_file': os.path.realpath(fchk_neut_file),
    }
    if do_GRAC:
        psi4_files.update({
            #'wfn_grac_file': os.path.realpath(wfn_grac_file),
            'fchk_grac_file': os.path.realpath(fchk_grac_file),
        })
    psi4_files.update({
        'do_GRAC':do_GRAC,
    })
    return psi4_files


def property_calc(wfn_file, method, basis, grac_shift=None):
    start_time=time.time()
    assert os.path.isfile(wfn_file)

    wfn=psi4.core.Wavefunction.from_file(wfn_file)
    mol=wfn.molecule()

    # We need to calculate that with GRAC shift
    if not isinstance(grac_shift, type(None)):
        psi4.set_options({ 'dft_grac_shift': grac_shift })
    do_gradient=True
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

def get_polarizability(method, geom=None, storage_var=None):
    """ Given a geometry calculate the Polarisabilities available in Psi4
    [ ] Provide starting guess (idk if that will be that much faster though)
    """
    
    func=method['functional']
    basis_set=method['basis_set']
    molecule_name=geom['molecule_name']

    
    

    def calculate_center_of_mass(atom_types, coordinates):
        """ 
        Calculate center of mass as
        x_com = sum ( w_i * x_i )    with x_i beeing coordinates and w_i weights: w_i = Z_i/( sum Z_j )
        """
        atom_numbers=[]
        for at_ty in atom_types:
            if isinstance(at_ty, int):
                atom_numbers.append(at_ty)
            elif isinstance(at_ty, str):
                assert len(at_ty)<3 and len(at_ty)>0
                Z=atom_type_to_Z(at_ty)
                atom_numbers.append(Z)
        atom_numbers=np.array(atom_numbers)
        com=np.sum(atom_numbers[:,None]*coordinates, axis=0)/np.sum(atom_numbers)
        return com


    atom_types, coordinates = xyz_file_read(xyz_file)
    COM, units_of_origin=[ [ float(x) for x in calculate_center_of_mass(atom_types, coordinates)], 'ANGSTROM' ] # recast that for easy printout
        

    property_calculation=[
    "# perform polarizability calculation",
    "props = ['DIPOLE_POLARIZABILITIES','QUADRUPOLE_POLARIZABILITIES']",
    #"set_options({",
    #"   \'basis\' : basis_set,",
    #"   \'reference\' : 'rks',",
    #"})",
    "E, wfn = properties(functional, molecule=monomer, properties= props, return_wfn=True)",
    ]

    # Get the polarisabilities
    def recover_polarisabilities(origin, type_of_origin=None, units_of_origin=None):
        recover_polarisabilities=f"""
        #### Recover the polarisabilities saved in the global variables
        # Manipulate the 'variables' variable where polarisabilities are stored
        the_variables=variables()
        key_per_rank={{
            'dipole':'DIPOLE POLARIZABILITY',
            'quadrupole':'QUADRUPOLE POLARIZABILITY'
        }}
        # Construct all expected subindices
        def make_product(the_list):
        \tnew_list=[]
        \tfor x in the_list:
            \t\tfor y in the_list:
            \t\t\tnew_list.append(x+y)
        \treturn new_list
        expected_subindices={{
            'dipole':make_product(['x','y','z']),
            'quadrupole':make_product(['xx','yy','zz','xz','xy','yz']),
        }}

        polarisability_tensors={{}}
        zero_thres=1.e-12
        for rank, rank_key in key_per_rank.items():
        \ttensor={{}}
        \tavailable_keys=[x for x in the_variables.keys() if x.startswith(rank_key)]
        \tcovered_keys=dict([(x,False) for x in available_keys ])
        \tfor index in expected_subindices[rank]:
            \t\tfull_index=' '.join([rank_key,index.upper()])
            \t\tassert full_index in available_keys, [index, available_keys]
            \t\t# Mark the index as covered
            \t\tassert not covered_keys[full_index], f"Double read in of {{full_index}}?"
            \t\tcovered_keys[full_index]=True
            \t\t# Safe the index and value into the tensor
            \t\tvalue=the_variables[full_index]
            \t\tvalue=0 if abs(value)<zero_thres else value
            \t\ttensor.update({{index:value}})
        \tpolarisability_tensors.update({{rank:tensor}})
        \tassert all(covered_keys.values())

        # Drop to file
        polarisability_tensors.update({{ 'origin': {{'coordinate':{COM}, 'type_of_origin':\'{type_of_origin}\', 'units_of_origin':\'{units_of_origin}\' }}}})
        import yaml
        polarizability_file='polarisability_tensors.yaml'
        with open(polarizability_file,'w') as wr:
          \tyaml.dump(polarisability_tensors, wr)
        {storage_var}['files'].update({{'polarizability_tensors':polarizability_file}})
        """
        ind=4*' '
        recover_polarisabilities=[x.strip(' ') for x in recover_polarisabilities.split('\n')]
        return recover_polarisabilities
        # Sorting according to stones convention (See Cartesian–Spherical Conversion Tables@Stone's Theory of Intermolecular Interactions book)

    define_polarisability_origin=[x.strip() for x in f"""
    # Define center of mass (in angstrom!) and assert that it has been read in correctly!
    core.set_global_option("PROPERTIES_ORIGIN", {COM})
    assert {COM}==get_option('None','PROPERTIES_ORIGIN'), 'Check Center of mass'
    """.split('\n')]
    # Pierce file together
    lines=(        []
        + define_polarisability_origin
        + property_calculation
        + recover_polarisabilities(COM, type_of_origin='COM',units_of_origin=units_of_origin)
    )
    return lines


def xyz_file_read(file_name):
    with open(file_name, 'r') as rd:
        lines=rd.readlines()
    assert len(lines)>2
    coordinates=[ x.split()[1:4] for x in lines[2:] if x.strip()!='' ]
    atom_types=[ x.split()[0] for x in lines[2:] if x.strip()!='' ]
    num_atoms=int(lines[0])
    assert len(coordinates)==num_atoms, f"{len(coordinates)} {num_atoms}"
    coordinates=np.array([ list(map(float, x)) for x in coordinates])

    return atom_types, coordinates
def xyz_to_string(file_name):
    """
    """
    atom_types, coordinates = xyz_file_read(file_name)
    molecule_str=molecule_string(atom_types=atom_types, coordinates=coordinates, units={'LENGTH':'ANGSTROM'})

    return molecule_str


def get_gs_energy(func, basis, molecule_name='monomer', uks=False):
    E_var='E_neut'
    wfn_var='wfn_neut'
    if uks:
        ref='uks'
    else:
        ref='rks'
    base_string= [ x.strip() for x in f"""
    set reference {ref}
    set_options({{
        'basis': {basis},
        'reference': 'rks',
    }})
    {E_var},{wfn_var} = energy({func}, molecule={molecule_name}, return_wfn=True)
    """.split('\n') ]
    return base_string, E_var, wfn_var

def get_ionization_energy(func_var, basis_set_var, molecule_name='monomer', uks=True):
    E_var='E_ion'
    wfn_var='wfn_ion'
    if uks:
        ref='uks'
    else:
        ref='rks'
    molecule_ion_name=molecule_name+"_ion"
    base_string= [ x.strip() for x in f"""
    {molecule_ion_name}=Molecule.from_string({molecule_name}_string)
    {molecule_ion_name}.set_molecular_charge({molecule_name}.molecular_charge()+1)
    {molecule_ion_name}.set_multiplicity({molecule_name}.multiplicity()+1)
    set_options({{
        'basis': {basis_set_var},
        'reference': 'uks',
    }})
    {E_var},{wfn_var} = energy({func_var}, molecule={molecule_ion_name}, return_wfn=True)
    """.split('\n') ]
    return base_string, E_var, wfn_var

def get_grac_shift(func_var, basis_set_var, molecule_name='monomer', charge=0, spin=1):
    neut_string, E_neut, wfn_neut = get_gs_energy(func_var, basis_set_var, molecule_name=molecule_name)
    ion_string, E_ion, wfn_ion    = get_ionization_energy(func_var, basis_set_var, molecule_name=molecule_name)

    # Compute the difference -> grac_shift
    ac_var=f"AC_shift"
    string=[ x.strip() for x in f"""
    E_homo = {wfn_neut}.epsilon_a_subset('AO', 'ALL').np[{wfn_neut}.nalpha()-1]
    IP = {E_ion} - {E_neut}
    {ac_var} = IP + E_homo
    """.split('\n') ]

    grac_string=neut_string+ion_string+string
    return grac_string, ac_var
def set_grac_shift(ac_var):
    return [x.strip() for x in f"""
        set_options({{'dft_grac_shift': {ac_var}}})
        the_grac_shift=get_option('SCF','dft_grac_shift')
        print_out(f\"GRAC SHIFT SET TO: {{the_grac_shift}}\")
        assert the_grac_shift>0.0, f\"The grac shift is not larger zero!\"
        """.split('\n')
    ]
    # if verbose:
    #     ind=4*' '
    #     information_string=[f"Information about GRAC calculation",
    #         f"IP {{IP}}",
    #         f"E_homo {{{homo_var}}}",
    #         f"GRAC_shift {{{ac_var}}}",
    #     ]
    #     information_string='\n'.join(
    #         [f"\npsi4.print_out(  \'\\n\'.join([",]+
    #         [ind+'\"'+x+'\",' for x in information_string]+
    #         ["])  )",]
    #     )
    #     string+=[ information_string ]
    # string='\n'.join(string)
    # return string

def get_ip_tuned_omega(func, basis, molecule_name='monomer', charge=0, spin=1, storage_var=None):
    """IP tuning requires calcultion of ionization energy [do this once in this case]
    then we alter omega so that we obtain a homo value that equals the negative ionization energy
    """
    assert not isinstance(storage_var, type(None)), f"Provide storage var!"
    neut_string, E_neut, wfn_neut = get_gs_energy(func, basis, molecule_name=molecule_name)
    ion_string, E_ion, wfn_ion    = get_ionization_energy(func, basis, molecule_name=molecule_name)

    # Compute the difference -> grac_shift
    ac_var=f"AC_shift"
    get_IP_string=[ x.strip() for x in f"""
    IP = {E_ion} - {E_neut}
    print_out(f"The calculate IP is: {{IP}}")
    {storage_var}['results'].update({{'IP':IP}})
    """.split('\n') ]


    optimize_omega_func=f"""
    def get_ip_diff(molecule, func, basis, omega, IP):
        set_options({{
            'reference' : 'rks',
            'DFT_omega' : omega,
            'basis'     : basis,
        }})
        E_omega, wfn_omega = energy(func, molecule=molecule, return_wfn=True)
        E_homo = wfn_omega.epsilon_a_subset('AO', 'ALL').np[wfn_omega.nalpha()-1]
        diff=E_homo+IP
        return diff
    """
    optimization_algorithm=f"""
    def bisection(x_vals, y_vals):
        x_lw, x_md, x_up =x_vals
        new_x_lw, new_x_md, new_x_up =x_vals
        y_lw, y_md, y_up = y_vals

            
        
        if y_lw*y_md<0:
            new_x_up=x_md
            new_x_lw=x_lw
        else:
            new_x_up=x_up
            new_x_lw=x_md
        new_x_md=(new_x_up+new_x_lw)/2.
        return  [new_x_lw, new_x_md, new_x_up]
    def return_diff_values(omega_range, old_omega_range, old_omega_vals):
        if isinstance(old_omega_range, type(None)):
            new_vals = [ get_ip_diff({molecule_name}, functional, basis_set, om, IP) for om in omega_range]
        else:
            mapper=dict( [ (k,v) for k,v in zip(old_omega_range, old_omega_vals)])
            new_vals=[]
            assert sum([ x in mapper.keys() for x in omega_range ]) == 2, f"{{mapper}} {{omega_range}}"
            for om in omega_range:
                if om in mapper.keys():
                    new_vals.append(mapper[om])
                else:
                    new_vals.append(get_ip_diff({molecule_name}, functional, basis_set, om, IP) )
        return new_vals

    omegas_ini=[ 0.1 ,  .5 , .9 ]
    omegas=omegas_ini
    old_omegas=None
    old_diff_vals=None
    chosen_omega=None
    for i in range(10):
        thres=0.005
        if np.abs(omegas[0]-omegas[2])<thres:
            chosen_omega=omegas[1]
            break
        else:
            diff_vals=return_diff_values(omegas, old_omegas, old_diff_vals)
            if diff_vals[0]*diff_vals[1]>=0. :
                test_vals=[0.05*i for i in range(1,20)]
                # raise Exception( test_vals, [ float(get_ip_diff({molecule_name}, functional, basis_set, om, IP)) for om in test_vals ])
            old_omegas=omegas
            old_diff_vals=diff_vals
            print(omegas, diff_vals)
            omegas=bisection(omegas, diff_vals)
            print('new_omegas',omegas)
    assert not isinstance(chosen_omega, type(None))
    """

    def splitter(string):
        lines=string.split('\n')
        first_line=None
        for i in range(len(lines)):
            if lines[i].strip()=='':
                continue
            else:
                first_line=i
                break
        assert not isinstance(first_line, type(None))
        indent=( len(lines[first_line])-len(lines[first_line].lstrip(' ')) )*' '
        return [l.replace(indent,'',1) for l in lines[first_line:]]
    optimize_omega_func=splitter(optimize_omega_func)
    optimization_algorithm=[f"start_omega_optimization=time.time()",]+splitter(optimization_algorithm) \
        + [f"{storage_var}['about_the_run']['timings'].update({{'omega_optimization':time.time()-start_omega_optimization}})"]

    # {ac_var} = IP + E_homo

    get_omega_str=[f"start_get_IP=time.time()"]+neut_string+ion_string+get_IP_string+[f"{storage_var}['about_the_run']['timings'].update({{'get_IP':time.time()-start_get_IP}})"]+optimize_omega_func+optimization_algorithm
    omega_var=f"chosen_omega"

    return get_omega_str, omega_var

def set_omega(omega_var):
    return [f"set DFT_omega {omega_var}",
        f"print_out(f\"IP-TUNED OMEGA SET TO: {{{omega_var}}}\")"]


def process_method(func, basis_set):
    # Check for grac
    func_comp=func.upper().split('-')
    grac_comp=[x for x in  func_comp if x.upper() in ['GRAC'] ]
    do_grac=False
    if len(grac_comp)>0:
        func='-'.join([x for x in func_comp if not x in grac_comp ])
        do_grac=True
    
    # Check for IP-tuned
    ip_tags=['IPTUNED','IPTUNING']
    func_comp=func.upper().split('-')
    ip_comp=[x for x in func_comp if x.upper() in ip_tags]
    do_ip_tune=False
    if len(ip_comp)>0:
        func='-'.join([x for x in func_comp if not x in ip_comp])
        do_ip_tune=True


    def splitter(string):
        split=[ x.strip() for x in string.split('\n') ]
        return '\n'.join(split)

    return func, basis_set, do_ip_tune, do_grac

def run_psi4_single(job,func, basis_set, geom, execute, input_file_name):

    storage_var=f"storage"
    storage_lines=[
        f"# Store variables here (eg IP)",
        f"{storage_var}={{'files':{{}},  'results':{{}}, 'about_the_run':{{ 'timings':{{}} }} }}",
        f"import time",
        f"run_started_at=time.time()",
    ]
    storage_file=f"psi4_calc_variable_storage.yaml"
    storage_drop_lines=[
        f"# Drop the storage as yaml file",
        f"# Time the complete run",
        f"{storage_var}['about_the_run']['timings'].update({{ 'total_run_time': time.time()-run_started_at,"+
            f" 'unaccounted_time': time.time()-run_started_at - sum(list({storage_var}['about_the_run']['timings'].values()))}})",
        f"import yaml",
        f"storage_file=\'{storage_file}\'",
        f"with open(storage_file, 'w') as wr:",
        f"    yaml.dump({storage_var},wr)",
        f"print(f\"Dropped stored variables under {{storage_file}}\")",
    ]

    molecule_lines=geom['molecule_lines']

    func, basis_set, do_ip_tuning, do_grac = process_method(func, basis_set)
    method=dict(
        functional=func,
        basis_set=basis_set,
        do_ip_tuning=do_ip_tuning,
        do_grac=do_grac,
    )
    definitions=[ x.strip() for x in f"""\
    # Description of the method (indicate correction in comments)
    functional='{func}'
    basis_set='{basis_set}'
    # IP_TUNING: {do_ip_tuning}
    # GRAC correction: {do_grac}
    """.split('\n') ]


    func_var='functional'
    basis_set_var='basis_set'
    # Set up the method
    if do_grac:
        grac_string, grac_var=get_grac_shift(func_var,basis_set_var)
        set_grac_str=set_grac_shift(grac_var)
    else:
        grac_string=[]
        set_grac_str=[]
    
    if do_ip_tuning:
        ip_tune_string, omega_var=get_ip_tuned_omega(func_var, basis_set_var, storage_var=storage_var)
        set_omega_str=set_omega(omega_var)
    else:
        ip_tune_string=[]
        set_omega_str=[]

    def make_method_definition_str(method):
        do_ip_tuning=method['do_ip_tuning']
        do_grac=method['do_grac']
        method_lines=[
            f"    'basis':{basis_set_var},",
            f"    'reference':'rks',"
        ]
        info_lines=[f"# Information about the settings printed to output file", f"print_out(f\"INFORMATION about SET METHOD\\n\")"]
        if do_ip_tuning:
            method_lines+=[
                f"    'DFT_omega': {omega_var},"
            ]
            info_lines.append(f"print_out(f\"    DFT_omega = {{{omega_var}}}\\n\")")
        if do_grac:
            method_lines+=[
                f"    'DFT_GRAC_SHIFT': {grac_var},"
            ]
            info_lines.append(f"print_out(f\"    DFT_GRAC_SHIFT = {{{grac_var}}}\\n\")")
        method_setup_lines=[ "set_options({"] + method_lines + ["})"]
        return method_setup_lines+info_lines
    method_lines=make_method_definition_str(method)



    job_indication_lines=[
        10*"#",
        3*'#'+f" {job.upper()}",
        10*"#",
    ]
    hardware_lines=set_hardware(memory='8GB',num_threads=8, storage_var=storage_var).split('\n')
    content=( storage_lines+[""] +hardware_lines+[""]+molecule_lines+[""]
        + definitions
        + ip_tune_string + grac_string
        +job_indication_lines+method_lines
    )
    # Get the job
    if job in ['polarizabilities']:
        get_polarizability_lines=get_polarizability(method, geom=geom, storage_var=storage_var)
        content+=['started_polarizabilities=time.time()'] + get_polarizability_lines + \
        [f"{storage_var}['about_the_run']['timings'].update({{'polarizabilities':time.time()-started_polarizabilities}})"]
        content+=[""]+storage_drop_lines
        with open(input_file_name,'w') as wr:
            wr.write('\n'.join(content))
        if execute:
            import subprocess
            psi4_executable='/home/bruno/0_Software/miniconda3/envs/qcAPI/bin/psi4'
            assert os.path.isfile(psi4_executable)
            cmd=f"{psi4_executable} {input_file_name}"
            print(f"Excuting \'{cmd}\' in shell")
            start=time.time()
            psi4_run=subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = psi4_run.communicate()
            return_code=psi4_run.returncode
            if return_code != 0:
                quit(f"Psi4 finished with return code {return_code}:\n   stdout={stdout.decode()}  error={stderr.decode()}")
            else:
                print(f"Psi4 finished successfully (after {time.time()-start:.3f} seconds)")
        else:
            print(f"Wrote the polarisability instructions to {input_file_name}")

def run_psi4(
    job,
    func=None,
    basis_set=None,
    xyz_file=None,
    execute=False,
    input_file_name=None,
    do_test=False,
):
    
    if not do_test:
        # Care about defaults
        if isinstance(func, type(None)):
            func=['PBE0']
            print(f"No functional provided ressort to default functional {func}")
        elif isinstance(func, str):
            func=[func]
        if isinstance(basis_set, type(None)):
            basis_set=['aug-cc-pVTZ']
            print(f"No basis_set provided ressort to default functional {basis_set}")
        elif isinstance(basis_set, str):
            basis_set=[basis_set]
    else:
        func=['PBE0','PBE0-GRAC','WB97X-iptuning']
        basis_set=['sto-3g']

    if not do_test:
        if isinstance(xyz_file, list):
            pass
        elif isinstance(xyz_file, str):
            xyz_file=[xyz_file]
        else:
            raise Exception()
    else:
        # Generate dummy xyz
        xyz_file='H2_dummy.xyz'
        dummy_xyz='\n'.join(['2','Dummy file for tests','H 0 0 0','H .74 0 0'])
        with open(xyz_file,'w') as wr:
            wr.write(dummy_xyz)
        xyz_file=[xyz_file]

    for xyz_fi in xyz_file:
        # Get geometry from xyz file
        try:
            atom_types, coordinates = xyz_file_read(xyz_fi)
            charge=0
            multiplicity=1
            molecule_str='\n'.join([4*' '+x for x in molecule_string(atom_types=atom_types, coordinates=coordinates, units={'LENGTH':'ANGSTROM'}).split('\n') ])
            molecule_name="monomer"
            molecule_lines=[ f"#Define the molecule (definition in molecule string has the advantage of being able to create new molecules reliable)",
                f"monomer_string=\"\"\"\n{molecule_str}\n\"\"\"", f"{molecule_name}=Molecule.from_string(monomer_string)"]
            geom={
                'molecule_name':    molecule_name,
                'charge':           charge,
                'multiplicity':     multiplicity,
                'atom_types':       atom_types,
                'coordindates':     coordinates,
                'molecule_lines':   molecule_lines,
            }
        except Exception as ex:
            raise Exception(f"Could not process {xyz_fi} as xyz_file: {ex}")
        for f in func:
            for b in basis_set:
                job_mapper={'polarizabilities':'POLARIZ'}
                input_file_name=f"{job_mapper[job].upper()}_{f}_{b}_{os.path.basename(xyz_fi).split('.')[0]}.psi4inp"
                run_psi4_single(job, f, b, geom, execute, input_file_name)



    


if __name__=='__main__':
    description=None
    epilog=None
    job_choices=['polarizabilities']
    import argparse as ap; par=ap.ArgumentParser(description=description, epilog=epilog)
    add=par.add_argument
    add('job', help=f"What to do", choices=job_choices)
    add('--xyz', help=f"xyz_file to process")
    add('--func', help=f"Functional for psi4 calculations")
    add('--basis', help=f"Main Basis set for Psi4 Calculation")
    add('--exc', action='store_true', help=f"Exceute psi4 (requires conda environment that runs psi4)")
    add('--test', action='store_true', help=f"Run a test (also provide exc statement to run!)")
    args=par.parse_args()
    xyz_file    = args.xyz
    job         = args.job
    func        = args.func
    basis_set   = args.basis
    execute     = args.exc
    do_test     = args.test

    run_psi4(
        job,
        func=func,
        basis_set=basis_set,
        xyz_file=xyz_file,
        execute=execute,
        do_test=do_test,
    )
                

