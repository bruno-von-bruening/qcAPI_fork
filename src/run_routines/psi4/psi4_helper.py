
from enum import Enum
class job_opts(str,Enum):
    SINGLE_POINT='single_point'
    MOLPOL_FINITE_FIELD='polarizability_finite_field'
    MOLPOL_LINEAR_RESPONSE='polarizability_linear_response'