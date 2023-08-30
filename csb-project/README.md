# csb-project
Repository containing the code to create Crop Sequence Boundaries (CSB). 

# Set up environment
After git clone the repository to the local directory, you can set up CSB-project folder with following steps:
1. Open command prompt and navigate to the CSB-project directory
2. Set up folder directories:					
	
	If bash is available:	
	1. Run `bash Setup_env.bash`
	2. Enter "Y" if you would like to set up the data folder; enther "N" if you would prefer to skip this step

	If bash is not available:
	1. Run following commands if you want to set up data folder:
		- mkdir CSB-Data
		- mkdir CSB-Data\\v2.5
		- mkdir CSB-Data\\v2.5\\Creation
		- mkdir CSB-Data\\v2.5\\Creation\\GEE
		- mkdir CSB-Data\\v2.5\\Creation\\GEE\\AreaTiles
		- mkdir CSB-Data\\v2.5\\Creation\\GEE\\Year
		- mkdir CSB-Data\\v2.5\\Distribution
		- mkdir CSB-Data\\v2.5\\Prep
		- mkdir CSB-Data\\v2.5\\Production
		- mkdir CSB-Data\\v2.5\\Split-Rasters



3. Modify the configuration file:
	- config/csb_default.ini contains input and output directories for the data
	- Mainly modify the [global] and [folders] section
	- Obtain national CDL file. The CDL folder/file structure should be /{year}/{year}_30m_cdls.tif. ex: /2019/2019_30m_cdls.tif
	- Add file path for cnty_shp_file, national_cdl_folder under [prep]
	
# Steps for creating the CSB data
1. Obtain CDL, reclassify, and filter as needed
	- Split the rasters into smaller subregions for computational efficiency following naming convention region_subregion_year.tif
  	(example ../2008/A1_0_2008.tif,  ../2008/A1_1_2008.tif, etc.)

	If bash is available:
	1. Create the CSB data. 
		1. Open command prompt and navigate to csb-project folder
		2. Run "bash CSB-Run.bash" and then enter the start and end year of CSB year as prompted.
	
	If bash is not available:
	1. Open command prompt and navigate to the CSB-project directory
	2. Run following command to create CSB data:
		- python CSB-Run/CSB-Run.py create "$start_year" "$end_year"
		- python CSB-Run/CSB-Run.py prep "$start_year" "$end_year"
		- python CSB-Run/CSB-Run.py distribute "$start_year" "$end_year"
		(replace "$start_year" and "$end_year" with the year you would like to create CSB for.) 
		Ex: python CSB-Run/CSB-Run.py create 2015 2022
	
# Software requirements
	- ArcGIS pro 2.9+
	- Anaconda 
