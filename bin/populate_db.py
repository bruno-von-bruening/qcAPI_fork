#!/usr/bin/env python

from server_executions.populate_server import main
from util.util import names as prop_names, get_unique_tag
available_properties=list(prop_names.keys())

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Populate a qcAPI database with jobs')
    parser.add_argument('--files', type=str, nargs='+', help='Filenames of the pickled configurations', default=[])
    parser.add_argument('--property', '-p', type=str, help=f"Which property to be computed")#, choices=available_properties)
    parser.add_argument('--address','-a', type=str, default="127.0.0.1:8000", help='URL:PORT of the qcAPI server')
    parser.add_argument('--method','-m', type=str, default=None ,help='Method to use')
    parser.add_argument('--basis','-b', type=str, default=None,help='Basis to use')
    #"def2-tzvppd",
    # Method and LISA
    parser.add_argument('--test', action='store_true',help='test (less than 50 entries)')
    args = parser.parse_args()

    filenames=args.files
    url = args.address.split(":")[0]
    port = args.address.split(":")[1]
    address=f"http://{url}:{port}"
    #
    property=args.property
    property=get_unique_tag(property, print_options=True)

    method=args.method
    basis=args.basis
    #
    do_test=args.test

    main(filenames, address, property, method, basis, do_test=do_test)
    
    
