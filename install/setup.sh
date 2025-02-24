
conda_env='qcAPI'
conda_file='install/qcAPI_env.yaml'

if [ ! -f ${conda_file} ]; then
   echo "The file \'${conda_file}\' does not exist"
   exit
fi
dependencies=("PROPERTY_OBJECTS_HOME" "DENSITY_OPERATIONS_HOME" "PROPERTY_DATABASE_HOME")
paths=()

for var in "${dependencies[@]}"; do
   if [ -z ${var} ]; then
      echo required environment variable ${var} is not defined
      exit
   fi
   path=$(printf "%s"  "${!var}" )
   if [ -z ${path} ]; then
      echo the variable \'${var}\' appears to be empty and should be set
      exit
   fi
   if [ ! -d "${path}" ]; then
      echo the path ${path} associated with variable \'${var}\' is not a directory: \'${path}\'
      exit
   fi
   paths+=("${path}")
done




source activate base
conda init bash 

conda remove -n ${conda_env} --all
conda env create -f ${conda_file}

conda activate ${conda_env}
if [[ $? != 0 ]]; then
   echo "Cannot activate conda environment ${conda_env}"
   exit
fi

install_pip_project() {
   dir=$1
   if [ ! -d ${dir} ]; then
      print "Provided argument ${dir} is not a directory"
      exit
   fi
   old_dir=$(pwd)
   cd ${dir}

   conda run -n ${conda_env} python -m pip install --upgrade build
   conda run -n ${conda_env} python -m build
   conda run -n ${conda_env} python -m pip install .

   cd ${old_dir}
}


install_pip_project '.'
for var  in "${paths[@]}"; do
   install_pip_project "${var}"
done

