"""
Arcpy script to take raster data (yearly TIFs retrieved from GEE) 
and break it into the predefined shapes (A6, A7, etc.) to be used in CSB creation. 
"""
import arcpy
import datetime as dt 
import logging
import sys
import os
import time
import configparser


 
YEAR = sys.argv[1]

config_file = f"X:\CSB-Project\CSB-Run\config\csb_default.ini"
config = configparser.ConfigParser()
config.read(config_file)
data_dir = config["folders"]["data"]
version = config["global"]["Version"]

year_files = config["prep_tile"]["gee_file"].replace('<data>', data_dir).replace('<version>',version) + f"/{YEAR}"
combine_output = config["prep_tile"]["combine_gee"].replace('<data>', data_dir).replace('<version>',version) + f"/{YEAR}"
input_ras = combine_output+f"/Combined_CDL_{YEAR}_v2_4.tif"
output_splits = config["folders"]["split_rasters"]+f"/{YEAR}"
predefined_tiles_workspace =r"X:\CSB-Project\CSB-Run\TILES"

def get_workspace(workspace):
    arcpy.env.workspace = (workspace)    



## SETS WORKSPACE TO THE YEAR FOLDER FOR MosaicToNewRaster_management FUNCTION
get_workspace(year_files)
## LIST THE RASTERS IN year_files
raster_list = arcpy.ListRasters("*","TIF")
## ADD ';' BETWEEN EACH NAME IN RASTER LIST
sep_list = ";".join(raster_list)
start_step  = dt.datetime.now()
## PUTS THE 'sep_list' RASTERS INTO ONE NEW DATASET 
arcpy.MosaicToNewRaster_management(sep_list,combine_output,
                                   f"Combined_CDL_{YEAR}_v2_4.tif","",
                                   pixel_type = "8_BIT_UNSIGNED",number_of_bands=1)
## PRINTING END TIME FOR MosaicToNewRaster_management /CURRENT RUN TIME 1:42:04.745617
end_step  = dt.datetime.now()
total_time = end_step - start_step
print(f"MosaicToNewRaster_management run time :  {total_time}")



## SWITCH WORKSPACES TO TILES FOLDER FOR SplitRaster_management FUNCTION
get_workspace(predefined_tiles_workspace) 
## PUT PRE DEFINED AREA SHAPE FILES IN A LIST
fc_list = arcpy.ListFeatureClasses()
start_step  = dt.datetime.now() 
#iterating over the list
for fc in fc_list:
    start_loop =  dt.datetime.now()
    fc_desc = arcpy.da.Describe(fc)
    name = fc_desc['name']
    label_name = fc_desc['name'][:-4] + f"_{YEAR}_"
    #print(f"{name}")
    ## split large tif by number of polygons
    with arcpy.EnvManager(parallelProcessingFactor="90%",pyramid="NONE"):
        arcpy.SplitRaster_management(input_ras, output_splits, label_name, "POLYGON_FEATURES",
                             "TIFF","NEAREST", split_polygon_feature_class = fc)
    end_loop = dt.datetime.now()
    total_step =  end_loop-start_loop
    print(f"{name} run time : {total_step}")
end_step  = dt.datetime.now()
## PRINTING END TIME FOR SplitRaster_management /CURRENT RUN TIME 1:24:15.568832
total_time = end_step - start_step
print(f"SplitRaster_management run time :  {total_time}")