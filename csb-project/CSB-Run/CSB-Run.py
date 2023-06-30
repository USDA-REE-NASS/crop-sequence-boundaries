import os
import sys
import datetime as dt
import configparser
import subprocess
import multiprocessing
import importlib        # CSB-Run util functions
utils = importlib.import_module('CSB-Run.utils')
    

# get command line args, 3 minimum required, missing args None
# input args: workflow, startYear and endYear
args = utils.GetArgs(sys.argv)

# get CSB-Run config file, default csb_default.ini
print(f'Config file: {os.getcwd()}/config/csb_default.ini')
cfg = utils.GetConfig('default')

# set parameters for CSB-Run
# cpu_count = utils.GetBatch( args[0], args[3] ) 
                         # #workflow   #batchSize
python_path,\
script,\
creation_dir,\
partial = utils.SetRunParams(cfg, args)


# build the folders for csb creation/prep run
run_dir = utils.BuildFolders(creation_dir, args[0])
                                           #workflow

# run the CSB workflow script         
p = subprocess.run([python_path, script, args[1], args[2], run_dir, str(partial)]) 
                                       #startYear #endYear
