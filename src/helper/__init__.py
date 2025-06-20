from pydantic import BaseModel,validate_call; val_call=validate_call(config=dict(arbitrary_types_allowed=True))
from typing import List, Union, Tuple
import time

import importlib.metadata
__version__ = importlib.metadata.version("qcpAPI")
