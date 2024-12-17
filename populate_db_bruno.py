#!/usr/bin/env python

import requests
import pickle
from fastapi.encoders import jsonable_encoder
import numpy as np
import os

def main(filenames,address, property, method, basis, do_test=False):
    """ Switch dependant on which property to compute"""
    if property in ['wfn']:
        assert all([ os.path.isfile(x) for x in filenames ])
        make_wfn(filenames, address, method, basis, do_test=do_test)
    elif property in ['part']:
        make_part(address, method, do_test=do_test)
    else:
        raise Exception(f"No case implemented for handling property {property}")

def make_wfn(filenames,address, method, basis, do_test=False):
    """" """

    # Load conformation from files
    conformations = []
    for filename in filenames:
        with open(filename, 'rb') as f:
            conformations += pickle.load(f)
    # If a test is requested we pick only the first 3 conformations (sorted by size)
    if do_test:
        conformations=sorted( conformations, key=lambda x:len(x['species']))[:3]
    # Jsonify the conformation
    conformations = jsonable_encoder(conformations,custom_encoder={np.ndarray: lambda x: x.tolist()})
    print(f"Number of supplied conformation: {len(conformations)}")

    # Post and print what has been copied
    response = requests.post(f"{address}/populate/wfn/{method}/{basis}", json=conformations)
    status_code=response.status_code
    if status_code!=200:
        raise Exception(f"Failed request: status_code={status_code}, details=\'{response.text}\'")
    else:

        first_id = response.json()["ids"][0]
        record = requests.get(f"http://{url}:{port}/get_record/{first_id}").json()
        print(f"Following record has been set on server database (first as exmaple): {record}")
    
    return None # Only execution nothing to return

def make_part(address, method, do_test=False):
    """ """
    
    # Check if method in available methods
    avail_methods=['MBIS','LISA']
    if not method.upper() in avail_methods: raise Exception(f"Method is not implemented yet: {method.upper()}\nAvailable Methods are {avail_methods}")
    
    response = requests.post(f"{address}/populate/part/{method.upper()}")
    status_code=response.status_code
    if status_code!=200:
        raise Exception(f"Could not populate: server returned: status_code={response.status_code} detail=\'{response.text}\'")
    else:
        json_obj=response.json()
        print(f"Successful population with return: {json_obj}")

    return None # Only execution nothing to return


if __name__ == "__main__":
    avail_properties = ['wfn','part']

    import argparse
    parser = argparse.ArgumentParser(description='Populate a qcAPI database with jobs')
    parser.add_argument('--filenames', type=str, nargs='+', help='Filenames of the pickled configurations')
    parser.add_argument('--property', type=str, default='wfn', help=f"Which property to be computed", choices=avail_properties)
    parser.add_argument('--address','-a', type=str, default="127.0.0.1:8000", help='URL:PORT of the qcAPI server')
    parser.add_argument('--method','-m', type=str, default="wb97m-d3bj" ,help='Method to use')
    parser.add_argument('--basis','-b', type=str, default="def2-tzvppd",help='Basis to use')
    # Method and LISA
    parser.add_argument('--test', action='store_true',help='test (less than 50 entries)')
    args = parser.parse_args()

    filenames=args.filenames
    url = args.address.split(":")[0]
    port = args.address.split(":")[1]
    address=f"http://{url}:{port}"
    #
    property=args.property
    method=args.method
    basis=args.basis
    #
    do_test=args.test

    main(filenames, address, property, method, basis, do_test=do_test)
    
