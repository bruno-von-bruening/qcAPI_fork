import sys, os, re
from util.util import element_symbol_to_nuclear_charge, nuclear_charge_to_element_symbol
from util.util import BOHR, BOHR_TO_ANGSTROM, ANGSTROM_TO_BOHR, analyse_exception

from util.config import load_global_config
from util.execution import run_command_in_shell

from pydantic import ValidationError, validate_call
import subprocess as sp
import json, shutil, glob, yaml
import time

from util.util import make_jobname, make_dir
import numpy as np
from data_base.qcAPI_database import RecordStatus
from pydantic import BaseModel, BeforeValidator, PlainSerializer
from typing import List, Annotated, Union, Tuple

from property_database.data_models.utilities import File_Model, Map_Stats_Model

from data_base.database_declaration import DMP_ESP_MAP_Stats

from util.run_utils import Tracker
from util.environment import file, directory, link_file
from util.environment import run_shell_command, temporary_file

from qc_objects.objects.property import geometry, multipoles
from qc_objects.objects.basis import molecular_radial_basis

class multipoles(multipoles):
    def for_qcAPI(self):
        """Create dictionary for qcAPI database"""
        moms= np.array(self.get_moments).tolist()
        moms_json=json.dumps(moms)
        ranks= self.ranks_list
        return dict(
            length_units='ANGSTROM',
            representation='CARTESIAN',
            convention='Stone',
            ranks=ranks,
            traceless=True,
            multipoles=moms_json,
        )
class molecular_radial_basis(molecular_radial_basis):
    def for_qcAPI(self):
        self.check_integrity()
        sol_dic={}

        centers=self.centers
        atom_types=self.atom_types
        sol_dic.update({
            'coordinates_of_sites':centers,
            'types_of_sites': atom_types,
        })

        required_keys=['exponent_scales','exponent_orders','coefficients','normalizations','length_of_contracted']
        optional_keys=['initial_coefficients']
        the_map=dict([
            ('decay_factors','exponent_scales'),
            ('decay_orders','exponent_orders'),
            ('normalizations','normalizations'),
            ('coefficients','coefficients'),
            ('initial_coefficiens','initial_coefficients'),
            ('functions_per_site','length_of_contracted'),
        ])


        shape_functions=[ x.model_dump(include=required_keys+optional_keys) for x in self.basis_expansions ]

        sol_dic.update(dict( [
                (k, [ x[the_map[k]] for x in shape_functions ]) for k in the_map.keys()
        ]))

        return sol_dic

