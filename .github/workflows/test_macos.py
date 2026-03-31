name: macOS Python + Conda CI

on:
  workflow_dispatch:  # manual runs
  push:
    branches: [ main , CI_explore ]
  pull_request:

jobs:
test-macos:
runs-on: macos-14

```
steps:
  - name: Checkout repo
    uses: actions/checkout@v4

  - name: Install Miniconda
    uses: conda-incubator/setup-miniconda@v3
    with:
      auto-update-conda: true
      python-version: 3.11
      activate-environment: myenv  # optional: auto-activate after creation

  - name: Create conda environment from YAML
    run: conda env create -f install/qcpAPI_env.yaml -n test_env

  - name: Activate environment
    shell: bash
    run: |
      conda activate test_env
      python --version
      conda list

  - name: Install your package into environment
    shell: bash
    run: |
      conda activate test_env
      pip install .

  - name: Run test script
    shell: bash
    run: |
      conda activate test_env
      cd tests/molpol_run
      python test.py
