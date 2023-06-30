import arcpy
import os

dist_dir = r'X:\CSB-Project\CSB-Data\v2.4\Distribution\distribute_1421_20220620_3'
state_gdb = f'{dist_dir}/State_gdb/State_CSB1421.gdb'

out_tif_dir = f'{dist_dir}/State_raster_test'

arcpy.env.workspace = state_gdb

StateFCs = arcpy.ListFeatureClasses("", "Polygon")


# create out state tif directory
if not os.path.exists(out_tif_dir):
    os.mkdir(out_tif_dir)

assignmentType = "CELL_CENTER"
cellsize = 30
for stateFC in StateFCs:
    print(stateFC)
    arcpy.conversion.PolygonToRaster(stateFC, "OBJECTID",
                                     f'{out_tif_dir}/{stateFC}.tif',
                                     assignmentType, "NONE", cellsize)

   # tif_raster_file = file_path+f'\Raster_Out\{shapefile_name}.tif'