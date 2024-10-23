import time
import os
import shutil
from pathlib import Path
import argparse
import logging
import sys
import arcpy
from arcpy.sa import *
import utils

# Distribution
states = ["AL", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
          "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
          "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
          "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
          "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]

FIPS = ['01','04','05','06','08','09','10','12','13','16','17','18',
 '19', '20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35',
'36','37','38','39','40','41','42','44','45','46','47','48','49','50','51','53',
 '54','55','56']

time0 = time.perf_counter()
print('Starting CSB distribution code... ')

start_year = sys.argv[1]
end_year = sys.argv[2]
distribute_dir = sys.argv[3] # distribute_1421_20220511_1

csb_year = f'{str(start_year)[2:]}{str(end_year)[2:]}'
cfg = utils.GetConfig('default')
version = cfg['global']['version']

print(f'Directory: {distribute_dir}')
prep_dir = utils.GetRunFolder('distribute', start_year, end_year, version)
print(f'Using results from: {prep_dir}')


subregion_path = f'{prep_dir}/National_Subregion_gdb'
national_path = f'{prep_dir}/National_gdb'

#Logging
LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(filename=distribute_dir + f'/log/CSB_Distribute.log',
                    level=logging.DEBUG,  # by default it only log warming or above
                    format=LOG_FORMAT,
                    filemode='a')  # over write instead of appending
logger = logging.getLogger()
error_path = distribute_dir + f'/log/overall_error.txt'

# %%  Creating single file gdb for the National Subregion folder
national_sub_gdb = arcpy.CreateFileGDB_management(subregion_path, f'Sub_CSB{csb_year}' + '.gdb')[0]
# %%  Creating single file gdb for the National folder
national_gdb = arcpy.CreateFileGDB_management(national_path, f'CSB{csb_year}' + '.gdb')[0]
# %% Creating gdb for the final column rearrange
national_gdb2 = arcpy.CreateFileGDB_management(distribute_dir + "//" + 'National_Final_gdb', f'CSB{csb_year}' + '.gdb')[0]

# %% Creating  subregion gdb str and national gdb str
national_gdb_lyr = (national_path + "\\" + f'CSB{csb_year}' + '.gdb' + "\\" + f"CSB{csb_year}")


csbMerge_filePath = prep_dir + r'\Subregion_gdb'
file_obj = Path(csbMerge_filePath).rglob(f'*.gdb')
file_lst = [x.__str__() for x in file_obj]

t0 = time.perf_counter()
# Adding Acres and XY points
for gdb in file_lst:
    shapefile_name = gdb.split('\\')[-1].split('.')[0].split('_CSB')[0]

    t1 = time.perf_counter()
    logger.info(f'{gdb}: Adding Acres and XY points')
    process = False
    while process is False:
        try:
            arcpy.management.DeleteField(in_table=gdb + f'/{shapefile_name}_CNTY',
                                                drop_field="CSBACRES;INSIDE_X;INSIDE_Y;POLY_AREA",
                                                method="DELETE_FIELDS")
            arcpy.management.RepairGeometry(
                in_features=gdb + f'/{shapefile_name}_CNTY',
                delete_null="DELETE_NULL",
                validation_method="ESRI")
                
            arcpy.CalculateField_management(gdb + f'/{shapefile_name}_CNTY', "CSBACRES", "!SHAPE.AREA!/4046.86", "PYTHON_9.3")
            arcpy.CalculateField_management(gdb + f'/{shapefile_name}_CNTY', "INSIDE_X", "!SHAPE.labelPoint.X!", "PYTHON_9.3")
            arcpy.CalculateField_management(gdb + f'/{shapefile_name}_CNTY', "INSIDE_Y", "!SHAPE.labelPoint.Y!", "PYTHON_9.3")
            
            # final_fc = arcpy.management.AddGeometryAttributes(Input_Features=gdb + f'/{shapefile_name}_CNTY',
                                                              # #Geometry_Properties=("AREA", "CENTROID_INSIDE"),
                                                              # Geometry_Properties=("AREA"),
                                                              # Area_Unit="ACRES")
            # arcpy.management.AlterField(in_table=final_fc, field="POLY_AREA", new_field_name='CSBACRES',
                                        # new_field_alias='CSBACRES')

            #arcpy.CopyFeatures_management(final_fc, national_sub_gdb + '\\' + shapefile_name)
            arcpy.CopyFeatures_management(gdb + f'/{shapefile_name}_CNTY', national_sub_gdb + '\\' + shapefile_name)
            process = True
            

        except Exception as e:
            error_msg = e.args
            logger.error(error_msg)
            f = open(error_path,'a')
            f.write(''.join(str(item) for item in error_msg))
            f.close()
            print(f'first error: {error_msg}, {shapefile_name}')
            if 'ERROR 002598: Name: "CSBACRES" already exists' in error_msg:
                process = True

        except:
            error_msg = arcpy.GetMessage(0)
            logger.error(error_msg)
            f = open(error_path,'a')
            f.write(''.join(str(item) for item in error_msg))
            f.close()
            print(f'2nd error: {error_msg}')
            if 'ERROR 002598: Name: "CSBACRES" already exists' in error_msg:
                process = True
            
    t2 = time.perf_counter()

    logger.info(f'{shapefile_name}: Adding Acres and XY points takes {round((t2 - t1) / 60, 2)} minutes')
t2 = time.perf_counter()
logger.info(f'Total time for adding Acres and XY points takes {round((t2 - t0) / 60, 2)} minutes')

# merge code
t3 = time.perf_counter()
merge_process = False
while merge_process is False:
    try:
        arcpy.env.workspace = subregion_path + f'/Sub_CSB{csb_year}' + '.gdb'
        fcList = arcpy.ListFeatureClasses()
        arcpy.Merge_management(fcList, national_gdb_lyr)
        merge_process = True
        
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
       
        
t4 = time.perf_counter()

print(f'Merge total time:  {round((t4 - t3) / 60, 2)} minutes')
logger.info(f'{national_gdb_lyr}: Merge total time:  {round((t4 - t3) / 60, 2)} minutes')

# AddCSBID at national layer
t5 = time.perf_counter()
calculate_field = False
while calculate_field == False:
    try:
        arcpy.arcpy.management.CalculateField(in_table=national_gdb_lyr, field="CSBID",
                                              expression="!STATEFIPS!+!CSBYEARS!+str(!OBJECTID!).zfill(9)",
                                              expression_type="PYTHON3")
        calculate_field = True
        
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
       
        
t6 = time.perf_counter()
duration = str(round((t6 - t5) / 60, 2))
logger.info(f'{national_gdb_lyr} : Adding CSBID to national layer takes  {duration} minutes')

# %% Additional Merge to rearrange columns
# set workspace
t7 = time.perf_counter()
arcpy.env.workspace = national_path

fc = national_gdb_lyr
# Variable for years
crop_rotation_year_list = []
for year in range(int(start_year), int(end_year) + 1, 1):
    crop_rotation_year_list.append("" + "CDL" + (
        str(year)) + " \"" + "CDL" + (
                                       str(year)) + "\" true true false 4 Long 0 0,First,#,"
                                                        "" + fc + "," + "CDL" + (str(year)) + ",-1,-1;")
crop_rotation_year_str = "".join(crop_rotation_year_list)

merge_process = False
while merge_process is False:
    try:
        arcpy.Merge_management(inputs=fc, output=national_gdb2 + "//" + "" + "national" + "GIS" + "",
                               field_mappings="CSBID \"CSBID\" true true false 15 Text 0 0,First,#," + fc + ",CSBID,0,15;"
                                                                                                            "CSBYEARS \"CSBYEARS\" true true false 4 Text 0 0,First,#," + fc + ",CSBYEARS,0,4;"
                                                                                                                                                                               "CSBACRES \"CSBACRES\" true true false 8 Double 0 0,First,#," + fc + ",CSBACRES,-1,-1;"
                                              + crop_rotation_year_str +
                                              "STATEFIPS \"STATEFIPS\" true true false 2 Text 0 0,First,#," + fc + ",STATEFIPS,0,2;"
                                                                                                                   "STATEASD \"STATEASD\" true true false 10 Text 0 0,First,#," + fc + ",STATEASD,0,10;"
                                                                                                                                                                                       "ASD \"ASD\" true true false 2 Text 0 0,First,#," + fc + ",ASD,0,2;"
                                                                                                                                                                                                                                        "CNTY \"CNTY\" true true false 21 Text 0 0,First,#," + fc + ",CNTY,0,21;"
                                                                                                                                                                                                                                                                                                    "CNTYFIPS \"CNTYFIPS\" true true false 3 Text 0 0,First,#," + fc + ",CNTYFIPS,0,3;"
                                                                                                                                                                                                                                                                                                                                                                       "Shape_Length \"Shape_Length\" true true false 8 Double 0 0,First,#," + fc + ",Shape_Length,-1,-1;"
                                                                                                                                                                                                                                                                                                                                                                                                                                                "Shape_area \"Shape_area\" true true false 12 Double 0 0,First,#," + fc + ",Shape_Area,-1,-1;"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          "INSIDE_X \"INSIDE_X\" true true false 8 Double 0 0,First,#," + fc + ",INSIDE_X,-1,-1;"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               "INSIDE_Y \"INSIDE_Y\" true true false 8 Double 0 0,First,#," + fc + ",INSIDE_Y,-1,-1")
        merge_process = True
        
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
       

t8 = time.perf_counter()
duration = str(round((t8 - t7) / 60, 2))
logger.info(
    f'{national_gdb2} + "//" + "" + "national" + "GIS" + "": Additional Merge to rearrange columns takes  {duration} minutes')

# Total duration
t8 = time.perf_counter()
print(f"Total time:{round((t8 - t0) / 60, 2)} minutes ")
duration = str(round((t8 - t0) / 60, 2))
logger.info(f'Total time for combime takes {duration} minutes')


# create a state gdb
state_gdb = arcpy.management.CreateFileGDB(out_folder_path=distribute_dir+r'\State_gdb', out_name=f"State_CSB{csb_year}.gdb")[0]
t7 = time.perf_counter()
# state distribution
for STATE in states:
    t1 = time.perf_counter()
    print(f'{STATE}: Start')
    FIPS_INDEX = states.index(STATE)
    STFIPS = FIPS[FIPS_INDEX]
    
    state_distribute = False
    
    while state_distribute is False:
        try:
    
            select_state = arcpy.management.SelectLayerByAttribute(in_layer_or_view=national_gdb2 + "//" + "" + "national" + "GIS" + "",
                                                                   selection_type="NEW_SELECTION",
                                                                   where_clause=f"STATEFIPS = '{STFIPS}'",
                                                                   invert_where_clause="")

            arcpy.FeatureClassToFeatureClass_conversion(select_state,
                                                        state_gdb,
                                                        f'CSB{STATE}{csb_year}')
                                                        
            state_distribute = True
            
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
                   
    t2 = time.perf_counter()
    print(f'{STATE}: takes {round((t2 - t1) / 60, 2)} minutes')
    logger.info(f'{STATE}: takes {round((t2 - t1) / 60, 2)} minutes')
    
#Total duration
t8 = time.perf_counter()
print(f"Total time for split into different states:{round((t8 - t7) / 60, 2)} minutes ")

print('Creating state rasters and DBFs...')

# Overwrite files in output set (T/F)
arcpy.env.overwriteOutput = True

state_gdb = f'{distribute_dir}/State_gdb/State_CSB{csb_year}.gdb'
    
# Define out workspace for CMU raster
out_tif_dir = f'{distribute_dir}/State/tif'
out_shp_dir = f'{distribute_dir}/State/shp'
out_dbf_dir = f'{distribute_dir}/State/dbf'

if not os.path.exists(f'{distribute_dir}/State'):
    os.mkdir(f'{distribute_dir}/State')
# create out state tif directory
if not os.path.exists(out_tif_dir):
    os.mkdir(out_tif_dir)

if not os.path.exists(out_shp_dir):
    os.mkdir(out_shp_dir)
    
if not os.path.exists(out_dbf_dir):
    os.mkdir(out_dbf_dir)
    

# Define feature class list workspace
arcpy.env.workspace = state_gdb

CSBpolyLISTGIS = arcpy.ListFeatureClasses("", "Polygon")

for CSBpolyGIS in CSBpolyLISTGIS:
# Polygon to raster keep CSBID value
    value = "CSBID"
    # Polygon to raster saves to .tif
    outRaster = f'{out_tif_dir}/{CSBpolyGIS}.tif'
    # Run polygon to raster: Cell Center, 30m
    print(f'Raster conversion: {CSBpolyGIS}')
    arcpy.conversion.PolygonToRaster(CSBpolyGIS, value, outRaster, "CELL_CENTER", "NONE", 30)
    # Run feature to shapefile:
    outShp = f'{out_shp_dir}/{CSBpolyGIS}.shp'
    print(f'Shapefile conversion {outShp}')
    arcpy.Select_analysis(in_features=CSBpolyGIS,
                          out_feature_class=outShp)
    print(CSBpolyGIS, outShp)

print("End of loops for CSB_prep")


# switch to raster workspace
arcpy.env.workspace = out_tif_dir

CSBtifLISTGIS = arcpy.ListRasters("", "TIF")

for CSBtifGIS in CSBtifLISTGIS:
    #print(CSBtifGIS)
    
    # CSB tif dbf name

    CSBtifDBF = f'{out_dbf_dir}/{CSBtifGIS.replace("tif","dbf")}'
    
# Make dbfs for lookup
    t0 = time.time()
    arcpy.management.CopyRows(in_rows=CSBtifGIS, out_table=CSBtifDBF, config_keyword="")
    t1 = time.time()
    duration = str(round(t1 - t0, 1))
    print(CSBtifDBF, "" + CSBtifGIS + " dbf made")
    print("Next file in loop :)")

time1 = time.perf_counter()
duration = str(round((time1 - time0)/60, 2))
logger.info(f'Total time for distribution takes {duration} minutes')

# deleting folders 
print("Deleting extra files...")
shutil.rmtree(f'{prep_dir}\\National_gdb')
shutil.rmtree(f'{prep_dir}\\Subregion_gdb')
shutil.rmtree(f'{prep_dir}\\National_Subregion_gdb')
shutil.rmtree(f'{creation_dir}\\Raster_Out')
print("Complete deleting all the extra files")