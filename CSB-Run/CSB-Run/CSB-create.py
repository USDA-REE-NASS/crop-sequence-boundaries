# V3 smooth newest
from pathlib import Path
import numpy as np
import shutil
import time
import multiprocessing
import logging
import sys
import os
import operator as op
# CSB-Run utility functions
import utils

# load ArcGIS packages, keep trying until it works (concurrent license problem)
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
        
# command line inputs passed by CSB-Run
start_year = sys.argv[1]
end_year = sys.argv[2]
creation_dir = sys.argv[3] # create_1421_20220511_1
partial_area = sys.argv[4] # partial run area e.g. G9 or 'None'

# projection
coor_str=r'PROJCS["USA_Contiguous_Albers_Equal_Area_Conic_USGS_version",GEOGCS["GCS_North_American_1983",DATUM["D_North_American_1983",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Albers"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",-96.0],PARAMETER["Standard_Parallel_1",29.5],PARAMETER["Standard_Parallel_2",45.5],PARAMETER["Latitude_Of_Origin",23.0],UNIT["Meter",1.0]]'

# projection for elimination
Output_Coordinate_System_2_ = "PROJCS[\"Albers_Conic_Equal_Area\",GEOGCS[\"GCS_North_American_1983\",DATUM[\"D_North_American_1983\",SPHEROID[\"GRS_1980\",6378137.0,298.257222101]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Albers\"],PARAMETER[\"false_easting\",0.0],PARAMETER[\"false_northing\",0.0],PARAMETER[\"central_meridian\",-96.0],PARAMETER[\"standard_parallel_1\",29.5],PARAMETER[\"standard_parallel_2\",45.5],PARAMETER[\"latitude_of_origin\",23.0],UNIT[\"Meter\",1.0]]"


# Main function that creates CSB datasets, performs elimination,run using multiprocessing
def CSB_process(start_year, end_year, area, count0):
    
    # Get config items, configure logger
    cfg = utils.GetConfig('default')
    
    LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"
    logging.basicConfig(filename= f'{creation_dir}/log/{area}.log',
                        level=logging.DEBUG, #by default it only log warming or above
                        format=LOG_FORMAT,
                        filemode='a')  # over write instead of appending
    logger = logging.getLogger()
    error_path = f'{creation_dir}/log/overall_error.txt'
    
    # Set up list of years covered in history
    year_lst = []
    for i in range(int(start_year), int(end_year) + 1):
        year_lst.append(i)

    # get file name for same area across different years
    year_file_lst = []
    for year in year_lst:
        filePath = fr'{cfg["folders"]["split_rasters"]}/{year}' 
        file_obj = Path(filePath).rglob(fr'{area}_{year}*.tif')
        file_lst = [x.__str__() for x in file_obj]
        sort_file_lst = []
        for i in range(len(file_lst)):
            path = fr'{filePath}/{area}_{year}_{i}.TIF'
            sort_file_lst.append(path)
        year_file_lst.append(sort_file_lst)

    createGDB = False
    while createGDB == False:
        try:
            t0 = time.perf_counter()
            print(f"{area}: Creating GDBs")
            logger.info(f"{area}: Creating GDBs")
            arcpy.CreateFileGDB_management(out_folder_path= f'{creation_dir}/Vectors_LL',
                                            out_name=f"{area}_{str(start_year)}-{str(end_year)}.gdb",
                                            out_version="CURRENT")
            arcpy.CreateFileGDB_management(out_folder_path= f'{creation_dir}/Vectors_Out',
                                            out_name=f"{area}_{str(start_year)}-{str(end_year)}_OUT.gdb",
                                            out_version="CURRENT")
            arcpy.CreateFileGDB_management(out_folder_path= f'{creation_dir}/Vectors_temp',
                                            out_name=f"{area}_{str(start_year)}-{str(end_year)}_temp.gdb",
                                            out_version="CURRENT")
            arcpy.CreateFileGDB_management(out_folder_path= f'{creation_dir}/Vectors_In/',
                                            out_name=f"{area}_{str(start_year)}-{str(end_year)}_In.gdb",
                                            out_version="CURRENT")
            createGDB = True
            
        except Exception as e:
            error_msg = e.args
            logger.error(error_msg)
            f = open(error_path,'a')
            f.write(''.join(str(item) for item in error_msg))
            f.close()
            sys.exit(0)
        except:
            error_msg = arcpy.GetMessage(0)
            logger.error(error_msg)
            f = open(error_path,'a')
            f.write(''.join(str(item) for item in error_msg))
            f.close()
            sys.exit(0)    

    combineFail = False
    while combineFail == False:
        try:
            t0 = time.perf_counter()
            print(f"{area}: Start Combine")
            logger.info(f"{area}: Start Combine")
            for i in range(len(file_lst)):
                lst = [j[i] for j in year_file_lst]
                input_path = ';'.join(lst)
                output_path = fr"in_memory\{area}_{i}"
                arcpy.gp.Combine_sa(input_path, output_path)
            logger.info(f"{area}_{i}: Combine Done, Adding Field")
            combineFail = True
            
        except Exception as e:
            error_msg = e.args
            logger.error(error_msg)
            f = open(error_path, 'a')
            f.write(''.join(str(item) for item in error_msg))
            f.close()
            time.sleep(2)
            print(f'{area}: try again add field')
            logger.info(f'{area}: try again add field')
            for i in range(len(file_lst)):
                lst = [j[i] for j in year_file_lst]
                input_path = ';'.join(lst)
                output_path = fr"in_memory\{area}_{i}"
                arcpy.gp.Combine_sa(input_path, output_path)
            logger.info(f"{area}_{i}: Combine Done, Adding Field")

        except:
            error_msg = arcpy.GetMessage(0)
            logger.error(error_msg)
            f = open(error_path, 'a')
            f.write(''.join(str(item) for item in error_msg))
            f.close()
            time.sleep(2)
            print(f'{area}: try again add field')
            logger.info(f'{area} try again add field')
            for i in range(len(file_lst)):
                lst = [j[i] for j in year_file_lst]
                input_path = ';'.join(lst)
                output_path = fr"in_memory\{area}_{i}"
                arcpy.gp.Combine_sa(input_path, output_path)
            logger.info(f"{area}_{i}: Combine Done, Adding Field")
            #sys.exit(0)
                               
        print(f'{area}: Combine Complete, Start Vector Processes')
        # Convert Raster to Vector
        logger.info(f"{area}_{i}: Convert Raster to Vector")
        out_feature_LL = fr"in_memory\{area}_{i}_LL"
        with arcpy.EnvManager(outputCoordinateSystem = coor_str):
            arcpy.RasterToPolygon_conversion(in_raster=output_path,
                                          out_polygon_features=out_feature_LL,
                                          simplify="SIMPLIFY", raster_field="Value",
                                          create_multipart_features="SINGLE_OUTER_PART", max_vertices_per_feature="")
        #NEED TO JOIN TABLES
        arcpy.management.JoinField(
            in_data=out_feature_LL,
            in_field="gridcode",
            join_table=output_path,
            join_field="Value",
            fields=None,
            fm_option="NOT_USE_FM",
            field_mapping=None)
            #index_join_fields="NO_INDEXES")

        #NEW PART FOR COUNT0 
        columnList = [i.name for i in arcpy.ListFields(out_feature_LL)]
        while 'COUNT0' not in columnList:
            try:
                arcpy.AddField_management(in_table=out_feature_LL, field_name="COUNT0", field_type="SHORT", field_precision="",
                                          field_scale="", field_length="", field_alias="", field_is_nullable="NULLABLE",
                                          field_is_required="NON_REQUIRED", field_domain="")
                columnList = [i.name for i in arcpy.ListFields(out_feature_LL)]
                
            except Exception as e:
                error_msg = e.args
                logger.error(error_msg)
                f = open(error_path, 'a')
                f.write(''.join(str(item) for item in error_msg))
                f.close()
                time.sleep(2)
                print(f'{area}: try again add field')
                logger.info(f'{area}: try again add field')
                arcpy.AddField_management(in_table=out_feature_LL, field_name="COUNT0", field_type="SHORT", field_precision="",
                                          field_scale="", field_length="", field_alias="", field_is_nullable="NON_NULLABLE",
                                          field_is_required="NON_REQUIRED", field_domain="")
                columnList = [i.name for i in arcpy.ListFields(out_feature_LL)]

            except:
                error_msg = arcpy.GetMessage(0)
                logger.error(error_msg)
                f = open(error_path, 'a')
                f.write(''.join(str(item) for item in error_msg))
                f.close()
                time.sleep(2)
                print(f'{area}: try again add field')
                logger.info(f'{area} try again add field')
                arcpy.AddField_management(in_table=out_feature_LL, field_name="COUNT0", field_type="SHORT", field_precision="",
                                          field_scale="", field_length="", field_alias="", field_is_nullable="NON_NULLABLE",
                                          field_is_required="NON_REQUIRED", field_domain="")
                columnList = [i.name for i in arcpy.ListFields(out_feature_LL)]

        # generate experession string
        logger.info(f'{area}_{i}: Calculate Field')
        calculate_field_lst = [r'!'+f'{area}_{j}_{i}'[0:10]+r'!' for j in year_lst]
        cal_expression = f"CountFieldsGreaterThanZero([{','.join(calculate_field_lst)}])"
        code = "def CountFieldsGreaterThanZero(fieldList): \n  counter = 0 \n  for field in fieldList: \n    if field > 0: \n      counter += 1 \n  return counter"
        try:
            arcpy.CalculateField_management(in_table=out_feature_LL,
                                            field="COUNT0",
                                            expression=cal_expression,
                                            code_block=code)
        except Exception as e:
            error_msg = e.args
            logger.error(error_msg)
            f = open(error_path, 'a')
            f.write(''.join(str(item) for item in error_msg))
            f.write(r'/n')
            f.close()
            sys.exit(0)
        except:
            error_msg = arcpy.GetMessage(0)
            logger.error(error_msg)
            f = open(error_path, 'a')
            f.write(''.join(str(item) for item in error_msg))
            f.write(r'/n')
            f.close()
            sys.exit(0)
        #NEW PART FOR COUNT barren(131 reclassed to 45) 
        columnList = [i.name for i in arcpy.ListFields(out_feature_LL)]
        while 'COUNT45' not in columnList:
            try:
                arcpy.AddField_management(in_table=out_feature_LL, field_name="COUNT45", field_type="SHORT", field_precision="",
                                          field_scale="", field_length="", field_alias="", field_is_nullable="NULLABLE",
                                          field_is_required="NON_REQUIRED", field_domain="")
                columnList = [i.name for i in arcpy.ListFields(out_feature_LL)]
                
            except Exception as e:
                error_msg = e.args
                logger.error(error_msg)
                f = open(error_path, 'a')
                f.write(''.join(str(item) for item in error_msg))
                f.close()
                time.sleep(2)
                print(f'{area}: try again add field')
                logger.info(f'{area}: try again add field')
                arcpy.AddField_management(in_table=out_feature_LL, field_name="COUNT45", field_type="SHORT", field_precision="",
                                          field_scale="", field_length="", field_alias="", field_is_nullable="NON_NULLABLE",
                                          field_is_required="NON_REQUIRED", field_domain="")
                columnList = [i.name for i in arcpy.ListFields(out_feature_LL)]

            except:
                error_msg = arcpy.GetMessage(0)
                logger.error(error_msg)
                f = open(error_path, 'a')
                f.write(''.join(str(item) for item in error_msg))
                f.close()
                time.sleep(2)
                print(f'{area}: try again add field')
                logger.info(f'{area} try again add field')
                arcpy.AddField_management(in_table=out_feature_LL, field_name="COUNT45", field_type="SHORT", field_precision="",
                                          field_scale="", field_length="", field_alias="", field_is_nullable="NON_NULLABLE",
                                          field_is_required="NON_REQUIRED", field_domain="")
                columnList = [i.name for i in arcpy.ListFields(out_feature_LL)]

        # generate experession string
        logger.info(f'{area}_{i}: Calculate Field')
        calculate_field_lst = [r'!'+f'{area}_{j}_{i}'[0:10]+r'!' for j in year_lst]
        cal_expression = f"CountFieldsGreaterThanZero([{','.join(calculate_field_lst)}])"
        code = "def CountFieldsGreaterThanZero(fieldList): \n  counter = 0 \n  for field in fieldList: \n    if field ==45: \n      counter += 1 \n  return counter"
        try:
            arcpy.CalculateField_management(in_table=out_feature_LL,
                                            field="COUNT45",
                                            expression=cal_expression,
                                            code_block=code)
        except Exception as e:
            error_msg = e.args
            logger.error(error_msg)
            f = open(error_path, 'a')
            f.write(''.join(str(item) for item in error_msg))
            f.write(r'/n')
            f.close()
            sys.exit(0)
        except:
            error_msg = arcpy.GetMessage(0)
            logger.error(error_msg)
            f = open(error_path, 'a')
            f.write(''.join(str(item) for item in error_msg))
            f.write(r'/n')
            f.close()
            sys.exit(0)

            arcpy.management.DeleteField(
                in_table=f"out_feature_LL",
                drop_field=f"calculate_field_lst",
                method="DELETE_FIELDS")
                
        #Move from memory to gdb
        arcpy.management.CopyFeatures(
            in_features= out_feature_LL,
            out_feature_class=f'{creation_dir}/Vectors_LL/{area}_{start_year}-{end_year}.gdb/{area}_{i}_In')      
            
        #Select Polygons to keep
        logger.info(f'{area}_{i}: Start Select')
        in_feature_gdb=f'{creation_dir}/Vectors_LL/{area}_{start_year}-{end_year}.gdb/{area}_{i}_In'
        out_feature_In = f'{creation_dir}/Vectors_In/{area}_{start_year}-{end_year}_In.gdb/{area}_{i}_In'
        #out_feature_In = f'memory/{area}_{i}_In'
        with arcpy.EnvManager(outputCoordinateSystem = coor_str):
            arcpy.analysis.Select(
                            in_features=in_feature_gdb,
                            out_feature_class=out_feature_In,
                            where_clause=f'(COUNT0 - COUNT45 >= {count0}) OR (Shape_Area >= 10000 AND COUNT0 - COUNT45 >= 1)') 
                             
        arcpy.management.Delete(in_data="out_feature_LL", data_type="")    

    #END COUNT0 PART  
    t1 = time.perf_counter()
    print(f'{area}: Start Elimination: {round((t1 - t0) / 60, 2)} minutes')
    logger.info(f'Time to finish all the steps before Elimination for {area}: {round((t1 - t0) / 60, 2)} minutes')

    logger.info(f"{area}: Elimination")#; print(f"{area}: Elimination")
    
    eliminate_success = False
    while eliminate_success == False:
        try:
            with arcpy.EnvManager(scratchWorkspace=f'memory', 
                                  workspace=f'memory'):
                CSBElimination(Input_Layers= f'{creation_dir}/Vectors_In/{area}_{start_year}-{end_year}_In.gdb',
                Workspace= f'{creation_dir}/Vectors_Out/{area}_{start_year}-{end_year}_OUT.gdb',
                Scratch= f'memory')
            eliminate_success = True
        
        except Exception as e:
                error_msg = e.args
                logger.error(error_msg); print(f'{area}: {error_msg}')
                f = open(error_path, 'a')
                f.write(''.join(str(item) for item in error_msg))
                f.write(r'/n')
                f.close()
                RepairTopology(f'{creation_dir}/Vectors_In/{area}_{start_year}-{end_year}_In.gdb',
                                f'{creation_dir}/Vectors_temp/{area}_{start_year}-{end_year}_temp.gdb',
                                area, logger)
        except:
                error_msg = arcpy.GetMessage(0)
                logger.error(error_msg)
                f = open(error_path, 'a')
                f.write(''.join(str(item) for item in error_msg))
                f.write(r'/n')
                f.close()
                sys.exit(0)
    
    t2 = time.perf_counter()
    logger.info(f'Time that Elimination takes for {area}: {round((t2 - t1) / 60, 2)} minutes')

    for i in range(len(file_lst)):
        logger.info(f"{area}_{i}: Select analysis")
        arcpy.Select_analysis(
            in_features=f'{creation_dir}/Vectors_Out/{area}_{start_year}-{end_year}_OUT.gdb/Out_{area}_{i}_In',
            out_feature_class=f'{creation_dir}/Vectors_temp/{area}_{i}_{start_year}_{end_year}_Out.shp',
            where_clause="Shape_Area >10000")
        # unsmoothed saved in Vectors_temp, smoothed saved to Vectors_out
        arcpy.cartography.SimplifyPolygon(
            in_features=f'{creation_dir}/Vectors_temp/{area}_{i}_{start_year}_{end_year}_Out.shp',
            out_feature_class=f'{creation_dir}/Vectors_out/{area}_{i}_{start_year}_{end_year}_Out.shp',
            algorithm="BEND_SIMPLIFY",
            tolerance="60 Meters",
            minimum_area="0 SquareMeters",
            error_option="RESOLVE_ERRORS",
            collapsed_point_option="NO_KEEP",
            in_barriers=None)
                      
    t3 = time.perf_counter()
    print(f'             Total processing time for {area}: {round((t3 - t0) / 60, 2)} minutes')
    logger.info(f'Total time for {area}: {round((t3 - t0) / 60, 2)} minutes')
    return(f'Finished {area}')


# Arcgis toolbox code that performs polygon elimination
def CSBElimination(Input_Layers, Workspace, Scratch):  

    # To allow overwriting outputs change overwriteOutput option to True.
    arcpy.env.overwriteOutput = True

    for FeatureClass, Name in FeatureClassGenerator(Input_Layers, "", "POLYGON", "NOT_RECURSIVE"):

        # Process: Make Feature Layer (Make Feature Layer) (management)
        _Name_Layer = f"{Name}"
        with arcpy.EnvManager(outputCoordinateSystem=Output_Coordinate_System_2_):
            arcpy.management.MakeFeatureLayer(in_features=FeatureClass, out_layer=_Name_Layer, where_clause="",
                                              workspace="", field_info="")

        # Process: Select Layer By Attribute (Select Layer By Attribute) (management)
        with arcpy.EnvManager(outputCoordinateSystem=Output_Coordinate_System_2_):
            Selected = arcpy.management.SelectLayerByAttribute(in_layer_or_view=_Name_Layer,
                                                               selection_type="NEW_SELECTION",
                                                               where_clause="Shape_Area <=100", invert_where_clause="")

        # Process: Eliminate (Eliminate) (management)
        _Name_temp1 = fr"{Scratch}\{Name}_temp1"
        if Selected:
            with arcpy.EnvManager(outputCoordinateSystem=Output_Coordinate_System_2_):
                arcpy.management.Eliminate(in_features=Selected, out_feature_class=_Name_temp1, selection="LENGTH",
                                           ex_where_clause="", ex_features="")

        # Process: Make Feature Layer (2) (Make Feature Layer) (management)
        input2 = f"{Name}_temp1_Layer"
        if Selected:
            with arcpy.EnvManager(outputCoordinateSystem=Output_Coordinate_System_2_):
                arcpy.management.MakeFeatureLayer(in_features=_Name_temp1, out_layer=input2, where_clause="",
                                                  workspace="", field_info="")

        # Process: Select Layer By Attribute (2) (Select Layer By Attribute) (management)
        if Selected:
            with arcpy.EnvManager(outputCoordinateSystem=Output_Coordinate_System_2_):
                Selected_2_ = arcpy.management.SelectLayerByAttribute(in_layer_or_view=input2,
                                                                      selection_type="NEW_SELECTION",
                                                                      where_clause="Shape_Area <=1000",
                                                                      invert_where_clause="")

        # Process: Eliminate (2) (Eliminate) (management)
        _Name_temp2 = fr"{Scratch}\{Name}_temp2"
        if Selected and Selected_2_:
            with arcpy.EnvManager(outputCoordinateSystem=Output_Coordinate_System_2_):
                arcpy.management.Eliminate(in_features=Selected_2_, out_feature_class=_Name_temp2, selection="LENGTH",
                                           ex_where_clause="", ex_features="")

        # Process: Make Feature Layer (3) (Make Feature Layer) (management)
        temp2_Layer = f"{Name}_temp2_Layer"
        if Selected and Selected_2_:
            arcpy.management.MakeFeatureLayer(in_features=_Name_temp2, out_layer=temp2_Layer, where_clause="",
                                              workspace="", field_info="")

        # Process: Select Layer By Attribute (3) (Select Layer By Attribute) (management)
        if Selected and Selected_2_:
            Selected_3_ = arcpy.management.SelectLayerByAttribute(in_layer_or_view=temp2_Layer,
                                                                  selection_type="NEW_SELECTION",
                                                                  where_clause="Shape_Area <=10000",
                                                                  invert_where_clause="")

        # Process: Eliminate (3) (Eliminate) (management)
        _Name_temp3 = fr"{Scratch}\{Name}_temp3"
        if Selected and Selected_2_ and Selected_3_:
            arcpy.management.Eliminate(in_features=Selected_3_, out_feature_class=_Name_temp3, selection="LENGTH",
                                       ex_where_clause="", ex_features="")

        # Process: Make Feature Layer (4) (Make Feature Layer) (management)
        input2_2_ = f"{Name}_temp3_Layer"
        if Selected and Selected_2_ and Selected_3_:
            arcpy.management.MakeFeatureLayer(in_features=_Name_temp3, out_layer=input2_2_, where_clause="",
                                              workspace="", field_info="")

        # Process: Select Layer By Attribute (4) (Select Layer By Attribute) (management)
        if Selected and Selected_2_ and Selected_3_:
            Selected_4_ = arcpy.management.SelectLayerByAttribute(in_layer_or_view=input2_2_,
                                                                  selection_type="NEW_SELECTION",
                                                                  where_clause="Shape_Area <=10000",
                                                                  invert_where_clause="")

        # Process: Eliminate (4) (Eliminate) (management)
        Out_Name_ = fr"{Workspace}\Out_{Name}"
        if Selected and Selected_2_ and Selected_3_ and Selected_4_:
            arcpy.management.Eliminate(in_features=Selected_4_, out_feature_class=Out_Name_, selection="LENGTH",
                                       ex_where_clause="", ex_features="")
                                       
        arcpy.management.Delete(in_data="_Name_Layer;_Name_temp1;_Name_temp2;_Name_temp3", data_type="")
        

# FeatureClassGenerator function used by CSBElimination arc toolbox
def FeatureClassGenerator(workspace, wild_card, feature_type, recursive) :
  with arcpy.EnvManager(workspace = workspace):

    dataset_list = [""]
    if recursive:
      datasets = arcpy.ListDatasets()
      dataset_list.extend(datasets)

    for dataset in dataset_list:
      featureclasses = arcpy.ListFeatureClasses(wild_card, feature_type, dataset)
      for fc in featureclasses:
        yield os.path.join(workspace, dataset, fc), fc


# this inspects the <area>_temp.gdb where the topology error happened
# identifies the problem area and repairs the it in the <area>_In.gdb
def RepairTopology(in_gdb, temp_gdb, area, area_logger):
    arcpy.env.workspace = temp_gdb
    temp_FCs = arcpy.ListFeatureClasses()
    
    # find the area that doesn't have 3 FCs in temp (eg one that failed)
    area_FCs = []
    for fc in temp_FCs:
        split_fc = fc.split('_')
        new_fc = f'{split_fc[0]}_{split_fc[1]}'
        area_FCs.append(new_fc)
        
    areas = np.unique(area_FCs)
    for a in areas:
        freq = op.countOf(area_FCs, a)
        if freq < 3:
            repair_area = a
    
    repair_msg = f'{repair_area}: Running repair geometry'
    print(repair_msg); area_logger.info(repair_msg)
    
    arcpy.RepairGeometry_management(f'{in_gdb}/{repair_area}_In')
    
    success_msg = f'{repair_area}: Repair geometry successful. Running Elimination again'
    print(success_msg); area_logger.info(success_msg)


# Magic function used in kicking off and joining multiprocessing items
def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


if __name__ == '__main__':

    # Get Creation and Split_raster paths from csb-default.ini
    cfg = utils.GetConfig('default')
    split_rasters = f'{cfg["folders"]["split_rasters"]}'
    print(f'Split raster folder: {split_rasters}')
    count0 = f'{cfg["global"]["count0"]}'
    print(f'Count of cropland for this run: {count0}')
    # get list of area files 
    file_obj = Path(f'{split_rasters}/{start_year}/').rglob(f'*.tif')
    file_lst = np.unique([x.__str__().split(f'{start_year}')[1][1:-1] for x in file_obj])
    print(len(file_lst))
    
    # delete old files from previous run if doing partial run
    if partial_area != 'None':
        file_lst = [x for x in file_lst if x == partial_area]
        csb_yrs = creation_dir.split('_')[-3]
        start_year = f'20{csb_yrs[0:2]}'
        end_year = f'20{csb_yrs[2:5]}'
        utils.DeletusGDBus(partial_area, creation_dir)
    
    
    # check the Area gdb
    vec_gdb_obj = Path(f'{creation_dir}/Vectors_Out/').rglob(f'*.gdb')
    vec_gdb_lst = [x.__str__().split(f'_{start_year}-{end_year}')[0].split(r'Vectors_Out')[-1][1:] for x in vec_gdb_obj]
    
    # compare raster and vector_out gdb list
    run_area_lst = []
    for i in file_lst:
        if i not in vec_gdb_lst:
            run_area_lst.append(i)
    
    while len(run_area_lst) != 0:
        # Kick off multiple instances of CSB_Process by area
        processes = []
        for area in run_area_lst: 
            p = multiprocessing.Process(target=CSB_process, args=[start_year, end_year, area, count0])
            processes.append(p)  
        
        # get number of CPUs to use in run
        cpu_prct = float(cfg['global']['cpu_prct'])
        run_cpu = int(round( cpu_prct * multiprocessing.cpu_count(), 0 ))
        print(f'Number of CPUs: {run_cpu}')
        for i in chunks(processes, run_cpu):
            for j in i:
                j.start()
            for j in i:
                j.join()
                
        # check the Area gdb
        vec_gdb_obj = Path(f'{creation_dir}/Vectors_Out/').rglob(f'*.gdb')
        vec_gdb_lst = [x.__str__().split(f'_{start_year}-{end_year}')[0].split(r'Vectors_Out')[-1][1:] for x in vec_gdb_obj]
        
        # compare raster and vector_out gdb list
        run_area_lst = []
        for i in file_lst:
            if i not in vec_gdb_lst:
                run_area_lst.append(i)
        
        #remove after partial run
        run_area_lst = []
    # check that all areas were created successfully, copy log file to file share if true
    shp_files = [f for f in os.listdir(f'{creation_dir}/Vectors_Out') if f.endswith('*.shp')]
    total_tif_lst = [x for x in file_obj]
        
    # deleting folders 
    print("Deleting extra files...")
    shutil.rmtree(f'{creation_dir}\\Merge')
    shutil.rmtree(f'{creation_dir}\\Vectors_In')
    shutil.rmtree(f'{creation_dir}\\Vectors_LL')
    
    # deleting gdbs in vector out folder
    vec_gdb_obj = Path(f'{creation_dir}/Vectors_Out/').rglob(f'*.gdb')
    temp_gdb_obj = Path(f'{creation_dir}/Vectors_temp/').rglob(f'*.gdb')
    for i in vec_gdb_obj:
        shutil.rmtree(i.__str__())
    for i in temp_gdb_obj:
        shutil.rmtree(i.__str__())
    print("Complete deleting all the extra files")