# Molecular Quantum Chemical Properties (qcpAPI)
Manage quantum chemical calculations with focus on molecular propertiesm, in particular, distributed properties like
atomic multipoles and polarizabilities.
Runs calculations through a client/server model (REST API) centered around a SQLite database.

**Warning**: This is a work in progress and no security measures for the API have been implemented yet.

## Installation

Install dependencies via the provided conda environment:
```bash
  conda env create -f install/qcAPI_env.yaml
  python -m build && pip install . # 
```
This will generate a conda environment name 'qcpAPI' (You might change that by providing an custom name via '-n')

If calculations ought to be run you might setup further environments. You might want to install the 
run_psi4 project. Since this is shipped separately a separate install is necessary. The specific version to run with
this version of qcpAPI is defined in the bash script `install/clone_run_psi4.sh`.
```
cd install
bash install/clone_run_psi4.sh
cd run_psi4_* # hash included in the environment
conda env create -f install/run_psi4_env.yaml
python -m build && pip install
```

Since there are some global definition defined in this script (`install/env_setup` defines names of environment to run)
the script should know the path of the top-level. Add the following to the shell you want to run with.
```
export QCPAPI_HOME=<path-to-top-level-directory>
```

## Usage example
All server operation will be instigated through the ```qcp_server.py``` executable which gets available when activating the conda environment (as path variable deactivate and activate the environment if it does not appear immediately after install).
The different operations are provided as positional arguments following the script ```qcp_server.py```. Consult the ````--help``` option to learn more about passing arguments.

### Starting the server
First server will have to be launched with options controlled through a ```config.yaml``` file that is provided by the user.

With the configs setup the server can be started as:
```bash
  qcp_server.py run -e # This will generate a example of the config the server need in the following and opens editor
  mv auto_config.yaml config.yaml # move this example 
  qcp_server.py run -c config.yaml
```

### Populating the server
The population script is called through
```bash
  qcp_server.py populate --property <property> <args>
```
where the properties to add to the database is defined by the ```property``` keyword. 
The script will then create entries for the tables associated to the property and either fill based on provided files or implicitly by inheriting information from existing tables (e.g. a wave functions can inherit molecular or distributed properties).


### Spawning workers
```bash
  qcp_server.py spawn_workers --config <config_file> --target_dir <working_directory> --property <property> --num_processes <num_processes>
```
Since the worker is spawned



Start a client to process the pending calculations:
```bash
  python client.py --address 127.0.0.1:8000 --num_threads 4
```
This will start a psi4 client with 4 threads that will process the pending calculations in the database.
You can run this command multiple times to start multiple clients, and on different machines to distribute the calculations (provided the server is accessible from the client).

In another terminal, you can check the progress with:
```bash
  python probe_server.py --address 127.0.0.1:8000 
```

## Accessing server from a different machine
In case the server runs on a machine that requires ssh access one can bind the remote server under a port at the local host:
```bash
ssh -L <port_local_host>:<remote_ip>:<port_remote_host> <username>@<remote_ip>
```
Assuming port 8080 has been bound at local machine we can reach the server via "http://localhost:8080"




# CI tests
One tests that checks if after pushing the molecular polarizabilities could be produced when installing through conda
environment file. Currently run by hand (could implement it on commit, which requires a bit more work to make sure the
new environment (not the old is checked)).
