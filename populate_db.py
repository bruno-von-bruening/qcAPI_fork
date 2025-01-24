#!/usr/bin/env python

import requests
import pickle
from fastapi.encoders import jsonable_encoder
import numpy as np
import os, json
from http import HTTPStatus

def main(filenames,address, property, method, basis, isodensity_values, do_test=False):
    """ Switch dependant on which property to compute"""
    if property in ['wfn']:
        assert all([ os.path.isfile(x) for x in filenames ])
        request_code, json_obj = make_wfn(filenames, address, method, basis, do_test=do_test)
    elif property in ['part']:
        request_code, json_obj = make_part(address, method, do_test=do_test)
    elif property in ['isodens_surf']:
        request_code, json_obj = make_isodens_surf(address, isodensity_values, do_test=do_test)
    elif property in ['esp_surf']:
        request_code, json_obj = make_esp_surf(address, do_test=do_test)
    else:
        raise Exception(f"No case implemented for handling property {property}")
    
    #### POST Request
    #################
    # Request response
    response = requests.post(f"{address}/{request_code}", json=json_obj)
    # Preprocess output of response
    status_code, text =(response.status_code, response.text)
    try:
        text=json.loads(text)
        if 'detail' in text.keys():
            text=text['detail']
        else:
            text='No details provided'
    except Exception as ex:
        pass
    # Interprete output of response
    if status_code==HTTPStatus.OK:
        print(f"Succesfully executed population with {request_code} (status_code={status_code})")
    elif status_code==HTTPStatus.INTERNAL_SERVER_ERROR:
        raise Exception(f"Internal Server error (status_code={status_code}) encountered for {request_code} (developer debugging necessary):\n   {text}")
    else:
        raise Exception(f"Status code {status_code} encountered:\n   {text}")

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
    request_code=f"populate/wfn/{method}/{basis}"
    json_obj=conformations
    return request_code, json_obj

def make_part(address, method, do_test=False):
    """ """
    # Check if method in available methods
    avail_methods=['MBIS','LISA']
    if not method.upper() in avail_methods: raise Exception(f"Method is not implemented yet: {method.upper()}\nAvailable Methods are {avail_methods}")
    
    request_code=f"populate/part?method={method.upper()}"
    return request_code, None

def make_isodens_surf(address, isodensity_values, do_test=False):
    """ """
    # Checks
    assert isinstance(isodensity_values, list)
    try:
        isodensity_values=[ float(x) for x in isodensity_values ]
    except:
        raise Exception(f"Not all floats in passed arguments {isodensity_values}")

    separator='_'
    isodenisty_string=separator.join([f"{x}" for x in isodensity_values ])
    request_code=f"populate/isodens_surf?isodens_vals={isodenisty_string}"
    return request_code, None

def make_esp_surf(address, do_test=False):
    """Make request code """
    request_code=f"/populate/esp_surf"
    return request_code, None


if __name__ == "__main__":
    avail_properties = ['wfn','part','isodens_surf', 'esp_surf']

    import argparse
    par = argparse.ArgumentParser(description='Populate a qcAPI database with jobs')
    par.add_argument('--filenames', type=str, nargs='+', help='Filenames of the pickled configurations')
    par.add_argument('--property', type=str, default='wfn', help=f"Which property to be computed", choices=avail_properties)
    par.add_argument('--address','-a', type=str, default="127.0.0.1:8000", help='URL:PORT of the qcAPI server')
    par.add_argument('--method','-m', type=str, default="wb97m-d3bj" ,help='Method to use')
    par.add_argument('--basis','-b', type=str, default="def2-tzvppd",help='Basis to use')
    par.add_argument('--isodens_vals', nargs='+', type=float, default=[],
        help=f"Isodensity values for isodensity surface")
    # Method and LISA
    par.add_argument('--test', action='store_true',help='test (less than 50 entries)')
    args = par.parse_args()

    filenames=args.filenames
    url = args.address.split(":")[0]
    port = args.address.split(":")[1]
    address=f"http://{url}:{port}"
    #
    property=args.property
    method=args.method
    basis=args.basis
    isodensity_values=args.isodens_vals
    #
    do_test=args.test

    main(filenames, address, property, method, basis, isodensity_values, do_test=do_test)
    
