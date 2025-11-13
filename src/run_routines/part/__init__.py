
from util.import_helper import *
from ..basic_imports import *

from util.util import element_symbol_to_nuclear_charge, nuclear_charge_to_element_symbol, make_jobname, make_dir
from util.util import BOHR, BOHR_TO_ANGSTROM, ANGSTROM_TO_BOHR, analyse_exception, my_exception

from util.config import load_global_config

from util.run_utils import Tracker
from util.environment import file, directory, link_file


from data_base.qcAPI_database import RecordStatus
from data_base.database_declaration import DMP_ESP_MAP_Stats

from qcp_database.data_models.utilities import File_Model, Map_Stats_Model

from qcp_objects.objects.properties import geometry, multipoles
from qcp_objects.objects.basis import molecular_radial_basis

from receiver.get_request import get_file

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

        required_keys=['exponent_scales','exponent_orders','coefficients','normalizations']#,'length_of_contracted']
        optional_keys=['initial_coefficients']
        the_map=dict([
            ('decay_factors','exponent_scales'),
            ('decay_orders','exponent_orders'),
            ('normalizations','normalizations'),
            ('coefficients','coefficients'),
            ('initial_coefficiens','initial_coefficients'),
            ('functions_per_site','length_of_contracted'),
        ])

        def my_reshape(x):
            y=x.model_dump(include=required_keys+optional_keys) 
            y.update({'length_of_contracted':len(y['exponent_scales'])})
            return y

        shape_functions=[ 
            my_reshape(x) for x in self.basis_expansions 
            ]

        sol_dic.update(dict( [
                (k, [ x[the_map[k]] for x in shape_functions ]) for k in the_map.keys()
        ]))

        return sol_dic

def load_fchk_file(address,fchk_file_entry):
    try:
        fchk_file=get_file(address, 'FCHK_File' , fchk_file_entry['id'])
        return fchk_file
    except Exception as ex: my_exception(f"Could not load fchk file:", ex)