import shutil
import os
import sys
import configparser
import multiprocessing
import datetime as dt


# get arguments from sys.arg in CSB-Run.py
def GetArgs(sys_argv):
    # need minimum number of arguments
    if len(sys_argv) < 4:
        print(f'Missing arguments. Please provide <workflow> <startYear> <endYear>')
        print('Or for partial run <workflow>_partial <directory> <area>')
        #logging
    else:
        # get command line arguments
        workflow = sys_argv[1]
        
        batchSize = None
        configFile = None
        
        # get that partial run fun
        if workflow == 'create_partial':
            directory = sys_argv[2]
            area = sys_argv[3]
            
            return workflow, directory, area, batchSize, configFile
            
        else:
            startYear = sys_argv[2]
            endYear = sys_argv[3]
            
            return workflow, startYear, endYear, batchSize, configFile



# get CSB-Run configuration file, will choose default if none provided
def GetConfig(config_arg):
    config_dir = f'{os.getcwd()}/config'

    if config_arg == 'default':
        config_file = f'{config_dir}/csb_default.ini'
    else:
        config_file = f'{config_dir}/{config_arg}'   
 
    config = configparser.ConfigParser()
    config.read(config_file)
    
    return config 


def SetRunParams(config, args):
    # ArcGIS python version
    arcgis_env = config['global']['python_env']
    print(f'Python env: {arcgis_env}')

    # CSB-Data directory 
    data_dir = config['folders']['data']

    # create run name e.g create_1421 
    runname_params = f'{args[0]}_{args[1][-2:]}{args[2][-2:]}'

    # get version 
    version = config['global']['version']
    
    # make path to CSB workflow data folder
    scripts = {'create': 'CSB-create.py',
                'prep': 'CSB-prep.py',
                'distribute': 'CSB-distribute.py',
                'create_partial': 'CSB-create_partial.py'}
    workflow = args[0]
    
    script = f'{os.getcwd()}/CSB-Run/CSB-Run/{scripts[workflow]}'
    
    run_date = dt.datetime.today().strftime("%Y%m%d")
    runname = f'{runname_params}_{version}_{run_date}_'
    
    
    try:
        creation_dir = config[workflow][f'{workflow}_folder']
        creation_dir = creation_dir.replace('<runname>', runname)
    except:
        if workflow == 'create_partial':
            creation_dir = config['create']['create_folder']
        else:
            creation_dir = config['prep'][f'prep_folder']
            creation_dir = creation_dir.replace('<runname>', runname)

    creation_dir = creation_dir.replace('<data>', data_dir)

    # check if there is a partial run!
    if workflow == 'create_partial':
        creation_dir = creation_dir.replace('<runname>', args[1])
        partial_area = args[2]
    else:
        partial_area = None

    return arcgis_env, script, creation_dir, partial_area
    

# function that builds folders for a CSB run
def BuildFolders(creation_dir, workflow):
    creation_folders = ['Merge','Vectors_In','Vectors_LL',
                        'Vectors_Out','Vectors_temp','log','Raster_Out']
    prep_folders = ['National_Subregion_gdb','Subregion_gdb','National_gdb','log']
    distribute_folders = ['National_Final_gdb','State_gdb','State','log','State/tif_state_extent']
    
    run_folder = creation_dir.split('/')[-1]
    base_dir = creation_dir.replace(run_folder,'')
    files = [f for f in os.listdir(base_dir) if f.startswith(run_folder)]
    
    # get folder name
    if len(files) > 0:
        # condition for picking right file
        version = [int(f.split('_')[-1]) for f in files if f.startswith(run_folder)]
        
        new_version = max(version) + 1
        
    else:
        new_version = 1
            
    # actual run directory name
    run_dir = f'{base_dir}{run_folder}{new_version}'
    
    
    # build appropriate folders
    if workflow == 'create' or workflow == 'create_test':
        try:
            os.makedirs(run_dir)
        except:
            pass
        # create subfolders
        if os.path.exists(run_dir):
            for f in creation_folders:
                try:
                    os.mkdir(f'{run_dir}/{f}')
                except:
                    pass
        print(f'Directory built: {run_dir}')
        
    elif workflow == 'prep':
        try:
            os.makedirs(run_dir)
        except:
            pass
        
        if os.path.exists(run_dir):
            for f in prep_folders:
                try:
                    os.mkdir(f'{run_dir}/{f}')
                except:
                    pass
                    
                

    elif workflow == 'distribute':
        try:
            os.makedirs(run_dir)
        except:
            pass
        
        if os.path.exists(run_dir):
            for f in distribute_folders:
                try:
                    os.mkdir(f'{run_dir}/{f}')
                except:
                    pass  
    
    elif workflow == 'create_partial':
        run_dir = creation_dir
    
        
    else:
        print(f'"{workflow}" is not a valid workflow. Choose "create", "prep", or "distribute"')
         
    return run_dir
            
 
# This function determines which creation run folder to use for given
# csb prep params 
def GetRunFolder(workflow, start_year, end_year,version):
    cfg = GetConfig('default')
    data_path = f"{cfg['folders']['data']}"
    
    if workflow == 'prep':
        create_path = f'{data_path}/Creation'
        prefix = 'create'
    elif workflow == 'distribute':
        create_path =  f'{data_path}/Prep'
        prefix = 'prep'
    
    files_prefix = f'{prefix}_{str(start_year)[2:]}{str(end_year)[2:]}_{str(version)}'
    files = [f for f in os.listdir(create_path) if f.startswith(files_prefix) \
             and not f.endswith('BAD')]
    file_date_list = []
    for f in files:
        file_list = f.split('_')
        file_date = file_list[3]
        file_date = dt.datetime.strptime(file_date, '%Y%m%d')
        file_date_list.append(file_date)
    
    # print(file_date_list)
    latest_date = max(file_date_list)
    latest_indeces = [i for i,x in enumerate(file_date_list) if x == latest_date]
    latest_files = [files[i] for i in latest_indeces]
    # this assumes the latest is the last in the list from os.listdir
    if len(files) > 0:
        run_path = f'{create_path}/{latest_files[-1]}'
        return run_path
    else:
        print('No create directory found for given years')
        quit()


def DeletusGDBus(area, directory):
    # relevant folders are in create directory currently
    creation_folders = ['Combine','CombineAll','Merge','Vectors_In','Vectors_LL',
                        'Vectors_Out','Vectors_temp']
    
    print(f'Deleting old files for {area}')
    
    for folder in creation_folders:
        check_folder = f'{directory}/{folder}'
        for f in os.listdir(check_folder):
            if f.startswith( f'{area}_'):
                if f.endswith( '.gdb' ):
                    shutil.rmtree( f'{check_folder}/{f}')
                else:
                    os.remove( f'{check_folder}/{f}')
    
           
# determine multiprocessing batch size 
def GetBatch(workflow, batch_size):
    # needs work, set to defaults currently
    cpu_count = multiprocessing.cpu_count()
    cfg = GetConfig('default')
    cpu_perc = cfg['global']['cpu_perc']
    run_cpu = int(round( cpu_perc * cpu_count, 0))
    #print(run_cpu)
    return run_cpu

