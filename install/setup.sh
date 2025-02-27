
conda_env='qcAPI'
conda_file='install/qcAPI_env.yaml'
dependencies=("PROPERTY_OBJECTS_HOME" "DENSITY_OPERATIONS_HOME" "PROPERTY_DATABASE_HOME" "QC_GLOBAL_UTILITIES_HOME")

if [ ! -f ${conda_file} ]; then
   echo "The file \'${conda_file}\' does not exist"
   exit
fi

source install/setup_ext.sh

paths=$( get_paths_for_dependencies ${dependencies[@]} )
if [[ $? != 0 ]]; then
   echo "Problem in setting up paths: ${paths}"
   exit
fi
conda_update_env ${conda_env} ${conda_file}
if [[ $? != 0 ]]; then
   echo "Problem in updating conda environment"
   exit
fi

conda activate ${conda_env}
if [[ $? != 0 ]]; then
   echo "Cannot activate conda environment ${conda_env}"
   exit
fi

install_pip_projects '.' ${paths[@]}
