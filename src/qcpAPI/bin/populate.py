#!/usr/bin/env python

from . import *

from server_external.populate.main import main as main_internal

from . import *
from .wrapper import add_client_args, add_property_arg, process_argv, process_client_args, get_property_args
available_properties=list(prop_names.keys())



@val_call
@process_argv
def main(argv:List[str]|Namespace):
    description='Populate a given qcpAPI table with entries'
    par=ArgumentParser(description=description)

    par=add_client_args(par)
    par=add_property_arg(par)
    add=par.add_argument

    add('--files', type=str, nargs='+', help='Filenames of the pickled configurations', default=[])
    add('--method','-m', type=str, default=None ,help='Method to use')
    add('--basis','-b', type=str, default=None,help='Basis to use')
    add('--test', action='store_true',help='test (less than 50 entries)')

    args=par.parse_args(argv)
    address, config_file=process_client_args(args, require_config=True)
    property=get_property_args(args)
    filenames=args.files
    method=args.method
    basis=args.basis
    do_test=args.test

    main_internal(filenames, address, config_file, property, method, basis, do_test=do_test)

if __name__ == "__main__":
    main()


