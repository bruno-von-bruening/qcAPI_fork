#!/usr/bin/env python
from importlib.metadata import version
from typing import Tuple, List, Union
from pydantic import validate_call; val_call=validate_call(config=dict(arbitrary_types_allowed=True))
import re
from packaging.version import Version
import importlib

def version_string(string)-> Version:
    return Version(string)
def get_version(string)->str:
    package=importlib.import_module(string)
    if not hasattr(package, '__version__'):
        try:
            version = importlib.metadata.version(string)
        except Exception as ex: raise Exception(f"Could not get version of package {string}: {ex}")
    else:
        version = package.__version__
    return version

#def version_string(string)-> Tuple[float, float, float]:
#    try:
#        version_code=string.split('.')
#        for num in version_code:
#            assert re.match(r'^\d+$', num), f"Version {the_version} contains non-numeric part: {num}"
#        the_version=tuple(int(num) for num in version_code)
#        assert len(the_version)==3, f"Version {the_version} is not a valid version tuple (expected 3 digits): {the_version}
#    except Exception as ex: raise Exception(f"Could not interpret verion {the_version} of {package}: {ex}")
#    return the_version

def get_version_of_custom_package(package_name:str) -> Tuple[float, float, float]:

    #package=importlib.import_module(package_name) 
    try:
        the_version=version(package_name)
    except Exception as ex: raise Exception(f"Could not get version of package {package_name}: {ex}")
    assert isinstance(the_version, str), f"Version of {package_name} is not a string: {the_version}"

    try:
        the_version=version_string(the_version)
    except Exception as ex: raise Exception(f"Could not interpret version {the_version} of {package_name}: {ex}")
    return the_version
req_sep=','
def requirment_intrepreatiion(string)->List[Tuple[str, Tuple[float, float, float]]]:
    requirements=string.split(req_sep)
    reqs=[]
    for r in requirements:
        assert len(r)>0

        if bool(re.match(r'^>',r)):
            cond='>'
        if bool(re.match(r'^>=',r)):
            cond='>='
        elif bool(re.match(r'^<',r)):
            cond='<'
        elif bool(re.match(r'^<=',r)):
            cond='<='
        elif bool(re.match(r'^[\d]',r)) or bool(re.match(r'^=',r)):
            cond='=='

        the_version=r.strip('<>=')
        version=version_string(the_version)
        reqs.append((cond, version))
    return reqs

def check_version_compatiblity(installed_version:Version, requirements:List[Tuple[str, Version]]) -> Tuple[bool, List[str]]:
    def comparions(tag1:int, tag2:int):
        if tag1>tag2: return 1
        elif tag1<tag2: return -1
        else: return 0
    errors=[]
    for condition, version in requirements:
        requirement=f"installed_version {condition} version"
        try:
            condition_met=eval(requirement)
        except Exception as ex: raise Exception(f"Could not evaluate \"{requirement}\": {ex}")
        if not condition_met:
            errors.append(f"{installed_version.base_version} {condition} {version.base_version}")
    return errors



        


def main(the_packages:List[Tuple[str,str]]) -> None:

    # Check if all packages are installed
    assert isinstance(the_packages, list), f"the_packages should be a list of tuples, got {type(the_packages)}"
    missing_packages=[]
    for package_name, version_requirement in the_packages:
        try:
            importlib.import_module(package_name)
        except Exception as ex: 
            missing_packages.append((package_name, version_requirement))
    if len(missing_packages)>0:
        package_line=[ f"   {package_name:<10} {version_requirement}" for package_name, version_requirement in missing_packages ]
        missing_packages='\n'.join(package_line)
        raise Exception(f"Missing packages:\n {missing_packages}")

    # Parse the infomration provided
    to_match=[]
    for package_name, version_requirement in the_packages:
        installed_version=get_version_of_custom_package(package_name)
        requirements=requirment_intrepreatiion(version_requirement)
        to_match+=[ (package_name,installed_version,requirements)]

    # Check if the installed versions match the requirements
    for package_name, installed_version, requirements in to_match:
        errors=check_version_compatiblity(installed_version, requirements)
        if len(errors)>0:
            error_lines=[f"   {error}" for error in errors]
            requirement_lines=[f"    {cond} {version.base_version}" for cond, version in requirements]
            raise Exception(f"Installed version of {package_name} is {installed_version}:\nREQUIREMENTS:\n"
                            +'\n'.join(requirement_lines)
                            +"\nERRORS:\n"
                             + '\n'.join(error_lines))
    print(f"Succesfull check!")


the_packages=[
    ('qcp_global_utils','>0.1.0'),
    ('qcp_objects','>0.1.0'),
    ('qcp_database','>0.1.0'),
]

if __name__=='__main__':
    import os, yaml
    try:
        requirement_file='version_requirements.yaml'
        assert os.path.isfile(requirement_file), f"Not a file: {requirement_file}"
        with open(requirement_file, 'r') as rd:
            requirements=yaml.safe_load(rd)
        requirements=[ (k,v) for k,v in requirements.items()] 
        print(f"Loaded requirements from {requirement_file}")
    except Exception as ex:
        raise Exception(f"Could not read requirements from {requirement_file}: {ex}")
    main(requirements)