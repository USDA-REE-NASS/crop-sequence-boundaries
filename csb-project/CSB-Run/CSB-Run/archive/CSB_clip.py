
import pandas as pd
import time
import sys
#import utils
import logging
import os

t0 = time.perf_counter()

# load ArcGIS packages keep, trying until it works (concurrent license problem)
arcpy_loaded = False
while arcpy_loaded is False:
    try:
        import arcpy
        arcpy.CheckOutExtension("Spatial")
        from arcpy.sa import *
        arcpy_loaded = True
        
    except RuntimeError as e:
        print(e)
        #logger.error(e)
        time.sleep(1)
        

def GetDistFolder(start_year, end_year):
    dist_path = r'X:\CSB-Project\CSB-Data\v2.5\Distribution'

    files_prefix = f'distribute_{start_year[2:]}{end_year[2:]}_'
    files = [f for f in os.listdir(dist_path) if f.startswith(files_prefix)]

    # temporary
    if len(files) > 0:
        run_path = f'{dist_path}/{files[0]}'
        return run_path
    


        
startYear = sys.argv[1]
endYear = sys.argv[2]

# params passed via csb-run + csb_default.ini config items
version, dataFolder = 'v2.5', r'X:\CSB-Project\CSB-Data'
csb_history = f'{str(startYear)[2:5]}{str(endYear)[2:5]}'

# Key directories
# Directory where the clip template file for clipping square shape around rasterized CSB (required for R validataion)
tmp_clip_dir = r'X:\CSB-Project\CSB-Run\ERDAS_batch\state_clip_table.csv'
# Contains data from completed CSB history/window that needs clipping 
csb_dir = GetDistFolder(startYear, endYear)
# Contains rasteried CSBs that need to be clipped
rasterized_csb_dir = f'{csb_dir}/State/tif'
# Directory where clipped CSB rasters are placed
clipped_csb_dir = f'{csb_dir}/State/tif_state_extent'

if os.path.exists(clipped_csb_dir):
    pass
else:
    try:
        os.makedirs(clipped_csb_dir)
    except:
        pass
    
if os.path.exists(clipped_csb_dir+r"/log"):
    pass
else:
    try:
        os.makedirs(clipped_csb_dir+r"/log")
    except:
        pass
        
#Logging
LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(filename=clipped_csb_dir + f'/log/CSB_Clip.log',
                    level=logging.DEBUG,  # by default it only log warming or above
                    format=LOG_FORMAT,
                    filemode='a')  # over write instead of appending
logger = logging.getLogger()

error_path = clipped_csb_dir + f'/log/overall_error.txt'


# create df for the clip template
logger.info("Start clipping tifs ...")
logger.info(f'{csb_dir}')
logger.info("Reading clip template...")
print("Start clipping tifs ...")
print(f'{csb_dir}')
print("Reading clip template...")
clip_df = pd.read_csv(tmp_clip_dir)


for index, row in clip_df.iterrows():
    state = row.State_Code
    boundary = row.boundary_str
    # Contains CDL extents used to clip square shape around rasterized CSB (required for R validataion)
    cdl_path = row.CDL_path


    # Files 
    rasterized_csb = f'{rasterized_csb_dir}/CSB{state}{csb_history}.tif'
    clipped_csb = f'{clipped_csb_dir}/CSB{state}{csb_history}_extent.tif'
    
    # print(state)
    # print(boundary)
    
    # print(f'CSB Raster to clip: {rasterized_csb}')
    # print(f'Extent file: {cdl_path}')
    
    t1 = time.perf_counter()
    logger.info(f'{state}: Start clipping')
    print(f'{state}: Start clipping')
    clipping = False
    
    while clipping == False:
        try:
            arcpy.management.Clip(rasterized_csb, boundary, 
                              clipped_csb, cdl_path, "-1", "NONE", "NO_MAINTAIN_EXTENT")
            clipping = True
            
        except Exception as e:
            error_msg = e.args
            logger.error(error_msg)
            f = open(error_path,'a')
            f.write(''.join(str(item) for item in error_msg))
            f.close()
           
        except:
            error_msg = arcpy.GetMessage(0)
            logger.error(error_msg)
            f = open(error_path,'a')
            f.write(''.join(str(item) for item in error_msg))
            f.close()
    
    # print(f'Clipped CSB, saved {clipped_csb}')
    logger.info(f'Clipped CSB, saved {clipped_csb}')
    print(f'Clipped CSB, saved {clipped_csb}')
    t2 = time.perf_counter()
    logger.info(f'{state}: Clipping process takes {round((t2 - t1), 2)} seconds')
    print(f'{state}: Clipping process takes {round((t2 - t1), 2)} seconds')

tf = time.perf_counter()
logger.info(f'Total time for clipping process takes {round((tf - t0) / 60, 2)} minutes')
print(f'Total time for clipping process takes {round((tf - t0) / 60, 2)} minutes')

    
    
    

