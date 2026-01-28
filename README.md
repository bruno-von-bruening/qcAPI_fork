# qcAPI

Distribute quantum chemistry calculations over different machines following a simple client/server model (REST API).

**Warning**: This is a work in progress and no security measures for the API have been implemented yet.

## Installation


Install dependencies via the provided conda environment:
```bash
  conda env create -f qcAPI_env.yml
  conda activate qcAPI
```
We will need many subpackages to run this script (density_operations, ):
```bash
  git clone "<path_to_script>"
  conda env create -f "<tag>_env.yaml"
  conda activate "<env>"

  # Manage paths and compile f90
  python install/setup.py 

  # Install pip
  python -m pip install --upgrade build  
  python -m build; python -m pip install .
```

Confirm that the libraries have the right version:
```
    # in install/
    conda activate qcpAPI
    python version_check.py
```

## Usage example
The server will operate through the ```qcp_server.py``` executable which gets available when activating the conda environment.
The different operations are provided as positional arguments following the script. Consult the ````--help``` option to learn more about passing arguments.

### Starting the server
First we need to define a config file in yaml format. -> TALK more about that.

With the configs setup the server can be started as:
```bash
  qcp_server.py run --config <config_file>
```
The server will create a SQLITE database which name is given in the `config.yaml` file and distribute calculations to clients that connect to it.

### Populating the server
The population script is called through
```bash
  qcp_server.py populate --property <property> <args>
```
where the properties to add to the database is defined by the ```property``` keyword. 
The script will then create entries for the tables associated to the property and either fill based on provided files or implicitly by inheriting information from existing tables (e.g. a wave functions can inherit molecular or distributed properties).

The initial inchikeys and conformations needs to be provided within files.
**Talk About how these files need to look like***

After population there will be new records in the database which are listed as *pending* if they need to be processed by a worker.

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




