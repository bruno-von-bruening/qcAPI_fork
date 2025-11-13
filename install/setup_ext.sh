
get_paths_for_dependencies() {
   dependencies=($@)
   for var in "${dependencies[@]}"; do
      if [ -z ${var} ]; then
         echo required environment variable ${var} is not defined
         exit 1
      fi
      path=$(printf "%s"  "${!var}" )
      if [ -z ${path} ]; then
         echo the variable \'${var}\' appears to be empty and should be set
         exit 1
      fi
      if [ ! -d "${path}" ]; then
         echo the path ${path} associated with variable \'${var}\' is not a directory: \'${path}\'
         exit 1
      fi
      paths+=("${path}")
   done
   echo ${paths[@]}
}


conda_update_env() {
   conda_env=$1
   conda_file=$2
   if [ ! -f ${conda_file} ]; then
      echo f"Provided invalid file: \"${conda_file}\" "
      exit 1
   fi


   source $CONDA_PREFIX/etc/profile.d/conda.sh
   $CONDA_EXE init bash 
   
   conda activate base
   conda env update -f "${conda_file}"  --prune
   conda deactivate
}

install_pip_project() {
   dir=$1
   if [ ! -d ${dir} ]; then
      print "Provided argument ${dir} is not a directory"
      exit 1
   fi
   old_dir=$(pwd)
   cd ${dir}

   conda run -n ${conda_env} python -m pip install --upgrade setuptools
   conda run -n ${conda_env} python -m pip install --upgrade build
   conda run -n ${conda_env} python -m build
   conda run -n ${conda_env} python -m pip install .
   cd ${old_dir}
}
install_pip_projects() {
   paths=($@)
   for var  in "${paths[@]}"; do
      install_pip_project "${var}"
      if [[ $? != 0 ]]; then
         exit 1
      fi
   done
}
