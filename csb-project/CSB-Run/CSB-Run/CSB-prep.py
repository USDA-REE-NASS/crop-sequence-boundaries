import multiprocessing
import time
import os
from pathlib import Path
import argparse
import logging
import sys
# CSB-Run utility functions
import utils

# load ArcGIS packages, keep trying until it works
arcpy_loaded = False
while arcpy_loaded is False:
    try:
        import arcpy
        from arcpy.sa import *
        arcpy_loaded = True
        
    except RuntimeError:
        print('Arcpy not loaded. Trying again...')
        time.sleep(1)

#global vars
cfg = utils.GetConfig('default')
agdists = cfg['prep']['cnty_shp_file']
national_cdl_folder = cfg['prep']['national_cdl_folder']
cellsize = 30 # for polygon to raster line 256


def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def CSB_prep(file_path,shape_path,prep_path,CSByear,start_year,end_year):

    t_init = time.perf_counter()
    
    # issue with prep check not getting shapefile name from create path
    #shapefile_name = shape_path.split('\\')[-1].split('.')[0]
    shapefile_name = shape_path.split('/')[-1].split('.')[0]
    
    #set up logger
    LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"
    logging.basicConfig(filename=f'{prep_path}/log/{shapefile_name}.log',
                        level=logging.DEBUG, #by default it only log warming or above
                        format=LOG_FORMAT,
                        filemode='a')  # over write instead of appending
    logger = logging.getLogger()
    
    error_path = f'{prep_path}/log/overall_error.txt'

    # create each gdb for sub-tile level
    logger.info(f'{shapefile_name}: Creating geodatabase... ')
    t1 = time.perf_counter()
    merge_path = f'{prep_path}/Subregion_gdb'
    try:
        Merge_gdb = arcpy.management.CreateFileGDB(out_folder_path=merge_path, out_name=f"{shapefile_name}_CSB{CSByear}.gdb")[0]
    
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

    logger.info(f'{shapefile_name}: Create gdb takes {round((t2 - t1) / 60, 2)} minutes')
    
    arcpy.env.workspace = Merge_gdb
    arcpy.env.overwriteOutput = True
    arcpy.CheckOutExtension("Spatial")
    
    # Create feature class in gdb from shape file
    t1 = time.perf_counter()
    logger.info(f'{shapefile_name}: Convert to feature class')
    FCtoFC = False
    while FCtoFC==False:
        try:
            #arcpy.conversion.FeatureClassToFeatureClass(shape_path, Merge_gdb, shapefile_name)
            arcpy.conversion.FeatureClassToFeatureClass(shape_path, Merge_gdb, shapefile_name)
            subregion = f'{Merge_gdb}/{shapefile_name}'
            #subregion = fr'memory/{shapefile_name}'
            FCtoFC = True
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
    
    logger.info(f'{shapefile_name}: Convert to feature class takes {round((t2 - t1) / 60, 2)} minutes')
        

    # Add fields
    t1 = time.perf_counter()
    logger.info(f'{shapefile_name}: Add fields')
    addField = False
    while addField == False:
        try:
            arcpy.management.AddField(in_table=subregion, field_name="CSBID", field_type="TEXT", field_precision=None,
                                      field_scale=None, field_length=15, field_alias="", field_is_nullable="NULLABLE",
                                      field_is_required="NON_REQUIRED", field_domain="")
            arcpy.management.AddField(in_table=subregion, field_name="CSBYEARS", field_type="TEXT", field_precision=None,
                                      field_scale=None, field_length=4, field_alias="", field_is_nullable="NULLABLE",
                                      field_is_required="NON_REQUIRED", field_domain="")
            arcpy.management.AddField(in_table=subregion, field_name="OBID", field_type="TEXT", field_precision=None,
                                      field_scale=None, field_length=9, field_alias="", field_is_nullable="NULLABLE",
                                      field_is_required="NON_REQUIRED", field_domain="")
            addField = True
        
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
    
    logger.info(f'{shapefile_name}: Add fields take {round((t2 - t1) / 60, 2)} minutes')
    
    # Calculate fields
    t1 = time.perf_counter()
    logger.info(f'{shapefile_name}: Calculate fields')
    
    # truncate the leading 0 causing calculate field error
    if CSByear.startswith('0'):
        CSByear = CSByear[1:4]
    
    CF = False
    while CF == False:
        try:
            arcpy.management.CalculateField(in_table=subregion, field="CSBYEARS", expression=CSByear,
                                        expression_type="PYTHON3")
            CF = True
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
    
    logger.info(f'{shapefile_name}: Calculate field takes {round((t2 - t1) / 60, 2)} minutes')

    # Spatial join for ASD
    t1 = time.perf_counter()
    logger.info(f'{shapefile_name}: Spatial join ASD')
    
    spacialJoin_ads = False
    while spacialJoin_ads==False:
        try:
            arcpy.analysis.SpatialJoin(target_features=subregion, join_features=agdists, out_feature_class=subregion + '_ASD',
                                       join_operation="JOIN_ONE_TO_ONE", join_type="KEEP_ALL",
                                       match_option="LARGEST_OVERLAP")
            spacialJoin_ads = True
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
    
    logger.info(f'{shapefile_name}: ADS spatial join takes {round((t2 - t1) / 60, 2)} minutes')            

    # # Spatial join for CNTY
    logger.info(f'{shapefile_name}: Spatial join CNTY')
    t1 = time.perf_counter()
    CNTY = agdists
    CNTY1 = fr'memory/{shapefile_name}'
    SpatialJoin = False
    while SpatialJoin == False:
        try:
            arcpy.analysis.SpatialJoin(target_features=subregion + '_ASD', join_features=CNTY,
                                       out_feature_class=CNTY1, join_operation="JOIN_ONE_TO_ONE",
                                       join_type="KEEP_ALL",
                                       field_mapping="", match_option="LARGEST_OVERLAP",
                                       search_radius="", distance_field_name="")
              
            SpatialJoin = True
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
    
    logger.info(f'{shapefile_name}: CNTY spatial join takes {round((t2 - t1) / 60, 2)} minutes')         
    
    # Add code to create .tif
    t1 = time.perf_counter()
    logger.info(f'{shapefile_name}: Create .tif ')
    assignmentType = "CELL_CENTER"
    convert_raster = False
    while convert_raster == False:
        try:
            arcpy.conversion.PolygonToRaster(CNTY1, "OBJECTID",
                                             file_path+f'\Raster_Out\{shapefile_name}.tif',
                                             #fr'memory\{shapefile_name}',
                                             assignmentType, "NONE", cellsize)

            tif_raster_file = file_path+f'\Raster_Out\{shapefile_name}.tif'
            #tif_raster_file = fr'memory\{shapefile_name}'
            convert_raster = True

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
    
    logger.info(f'{shapefile_name}: Convert to .tif takes {round((t2 - t1) / 60, 2)} minutes')
        
    # Deleting fields that are extra
    t1 = time.perf_counter()
    logger.info(f'{shapefile_name}: Deleting extra field')
    dropFields = ["Join_Count", "Target_FID", "Join_Count_1", "Target_FID_1", "Id", "gridcode",
                  "Count0", "Shape_Leng", "Need_Merge", "OBID"]
                  
    delete_field = False
    while delete_field == False:
        try:
            arcpy.management.DeleteField(in_table=CNTY1, drop_field=dropFields)
            delete_field = True
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
    
    logger.info(f'{shapefile_name}: Delete extra field takes {round((t2 - t1) / 60, 2)} minutes')
    
    # Deleteing extra layers
    t1 = time.perf_counter()
    logger.info(f'{shapefile_name}: Deleting extra layers')
    layer_lst = [subregion + '_ASD', subregion]
    for i in layer_lst:
        arcpy.management.Delete(i)

    t2 = time.perf_counter()
    logger.info(f'{shapefile_name}: Delete extra layers takes {round((t2 - t1) / 60, 2)} minutes')

    # Zonal Statistic
    for y in range(int(start_year), int(end_year)+1):
        t1 = time.perf_counter()
        logger.info(f'{shapefile_name}_{y}: adding zonal Statistic for Year {y}')
        zonal = False
        while zonal == False:
            try:
                ZonalStatisticsAsTable(in_zone_data=tif_raster_file, zone_field='Value',
                                       in_value_raster=fr'{national_cdl_folder}\\{y}\\{y}_30m_cdls.tif',
                                       out_table=Merge_gdb + "//" + f"Raster_Out_{y}_30m_cdls",
                                       #out_table=memory+ "//" + f"Raster_Out_{y}_30m_cdls",
                                       ignore_nodata="NODATA", statistics_type="MAJORITY")
                zonal = True
            except Exception as e:
                error_msg = e.args
                logger.error(error_msg)
                f = open(error_path, 'a')
                f.write(''.join(str(item) for item in error_msg))
                f.close()
               
            except:
                error_msg = arcpy.GetMessage(0)
                logger.error(error_msg)
                f = open(error_path, 'a')
                f.write(''.join(str(item) for item in error_msg))
                f.close()
               
        
        alterField = False
        while alterField == False:
            try:
                arcpy.management.AlterField(#in_table=memory + "//" + f"Raster_Out_{y}_30m_cdls",
                                            in_table=Merge_gdb + "//" + f"Raster_Out_{y}_30m_cdls",
                                            field="MAJORITY", new_field_name="CDL" + str(f'{y}'),
                                            new_field_alias="CDL" + str(f'{y}'), field_type="LONG", field_length=4,
                                            field_is_nullable="NULLABLE", clear_field_alias="DO_NOT_CLEAR")
                alterField = True
                
            except Exception as e:
                error_msg = e.args
                logger.error(error_msg)
                f = open(error_path, 'a')
                f.write(''.join(str(item) for item in error_msg))
                f.close()
               
            except:
                error_msg = arcpy.GetMessage(0)
                logger.error(error_msg)
                f = open(error_path, 'a')
                f.write(''.join(str(item) for item in error_msg))
                f.close()
               

        join_field = False
        while join_field == False:
            try:                                          
                         
                arcpy.management.JoinField(in_data=CNTY1, in_field="OBJECTID",
                                           join_table=Merge_gdb + "//" + f"Raster_Out_{y}_30m_cdls",
                                           #join_table=memory + "//" + f"Raster_Out_{y}_30m_cdls",
                                           join_field="Value", fields="" + f"CDL{y}" + "")
                join_field = True
                
            except Exception as e:
                error_msg = e.args
                logger.error(error_msg)
                f = open(error_path, 'a')
                f.write(''.join(str(item) for item in error_msg))
                f.close()
               
            except:
                error_msg = arcpy.GetMessage(0)
                logger.error(error_msg)
                f = open(error_path, 'a')
                f.write(''.join(str(item) for item in error_msg))
                f.close()
               
            
        t2 = time.perf_counter()
        logger.info(f'{shapefile_name}_{y}: add zonal Statistic for Year {y} took {round((t2 - t1) / 60, 2)} minutes')
        
    
    #Deleting polygon that is small and does not contain crop rotation data
    t1 = time.perf_counter()
    logger.info(f'{shapefile_name}: Deleting null polygon')
    delete_polygon = False
    while delete_polygon == False:
        try:
            #arcpy.analysis.Select(subregion + '_CNTY1', subregion + '_CNTY', f"CDL{y} IS NOT NULL")
            arcpy.analysis.Select(CNTY1, subregion + '_CNTY', f"CDL{y} IS NOT NULL")
            delete_polygon = True
            
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
    logger.info(f'{shapefile_name}: Delete null polygon takes {round((t2 - t1) / 60, 2)} minutes')

 # Deleting CNTY1 file
    t1 = time.perf_counter()
    logger.info(f'{shapefile_name}: Deleting extra CNTY1 layer')
                  
    delete_layer = False
    while delete_layer == False:
        try:
            #arcpy.management.Delete(subregion + '_CNTY1')
            arcpy.management.Delete(CNTY1)
            delete_layer = True
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
    
    logger.info(f'{shapefile_name}: Delete CNTY1 file {round((t2 - t1) / 60, 2)} minutes')

    t3 = time.perf_counter()
    print(f'Total time for {shapefile_name}: {round((t3 - t_init) / 60, 2)} minutes')
    logger.info(f'Total time for {shapefile_name}: {round((t3 - t_init) / 60, 2)} minutes')


    return(f'Finished {shapefile_name}')


if __name__ == '__main__':

    time0 = time.perf_counter()
    print('Starting CSB prep CHECK code... ')
    cfg = utils.GetConfig('default')
    version = cfg['global']['version']
    
    start_year = sys.argv[1]
    end_year = sys.argv[2]
    prep_dir = sys.argv[3] # create_1421_20220511_1
    print(f'Directory: {prep_dir}')
    create_dir = utils.GetRunFolder('prep', start_year, end_year,version)
    print(f'Using results from: {create_dir}')

    csb_year = f'{str(start_year)[2:]}{str(end_year)[2:]}'

    cfg = utils.GetConfig('default')
    file_path = create_dir

    csb_filePath = f'{file_path}/Vectors_Out'
    file_obj = Path(csb_filePath).rglob(f'*.shp')

    list_of_files = sorted(file_obj, key=lambda x: os.stat(x).st_size)
    in_file_lst = [x.__str__().split(f'_{start_year}_{end_year}')[0].split("Vectors_Out")[-1][1:] for x in list_of_files]
    
    out_file_obj = Path(f'{prep_dir}/Subregion_gdb').rglob(f'*.gdb')
    out_file_lst = [x.__str__().split(f'_{start_year}_{end_year}')[0].split("Subregion_gdb")[-1][1:] for x in out_file_obj]
    
    process_file_lst = []
    for i in in_file_lst:
        if i not in out_file_lst:
            process_file_lst.append(i)
    
    
    
    while len(process_file_lst) != 0:
        processes = []
        
        for i in process_file_lst:
    
            shape_path = f'{create_dir}/Vectors_Out/{i}_{start_year}_{end_year}_Out.shp'
            
            p = multiprocessing.Process(target=CSB_prep, 
                                        args=[file_path, shape_path, prep_dir,
                                              csb_year, start_year, end_year])
            processes.append(p)
    
        # get number of CPUs to use in run
        cpu_prct = float(cfg['global']['cpu_prct'])
        run_cpu = int(round( cpu_prct * multiprocessing.cpu_count(), 0 ))
        print(f'Number of CPUs: {run_cpu}')
        for i in chunks(processes,run_cpu):
            for j in i:
                j.start()
            for j in i:
                j.join()
                
        out_file_obj = Path(f'{prep_dir}/Subregion_gdb').rglob(f'*.gdb')
        out_file_lst = [x.__str__().split(f'_{start_year}_{end_year}')[0].split("Subregion_gdb")[-1][1:] for x in out_file_obj]
        
        process_file_lst = []
        for i in in_file_lst:
            if i not in out_file_lst:
                process_file_lst.append(i)
    
    time_final = time.perf_counter()
    print(f'Total time to run CSB prep: {round((time_final - time0) / 60, 2)} minutes')