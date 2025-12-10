import sys, os
import argparse
from argparse import Namespace, ArgumentParser

from util.import_helper import *
from util.http_util import pdtc_address

def wrap(main, argv=None):
    argv = argv if argv is not None else sys.argv[1:]
    main(argv)
from functools import partial
from orm_import.utils import names as prop_names, get_unique_tag