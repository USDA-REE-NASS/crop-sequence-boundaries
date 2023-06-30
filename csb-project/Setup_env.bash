#!/bin/bash

# Read the user input for folder creation
echo "Do you want to create data folder? (Y/N) "
read first_response

if [ "$first_response" == 'Y' ]
then
	mkdir CSB-Data
	mkdir CSB-Data\\v2.5
	mkdir CSB-Data\\v2.5\\Creation
	mkdir CSB-Data\\v2.5\\Creation\\GEE
	mkdir CSB-Data\\v2.5\\Creation\\GEE\\AreaTiles
	mkdir CSB-Data\\v2.5\\Creation\\GEE\\Year
	mkdir CSB-Data\\v2.5\\Distribution
	mkdir CSB-Data\\v2.5\\Prep
	mkdir CSB-Data\\v2.5\\Production
	mkdir CSB-Data\\v2.5\\Split-Rasters
	mkdir CSB-Data\\v2.5\\Validation
	mkdir CSB-Data\\v2.5\\Validation\\Temp
	echo "CSB-Data folder has created"
else
echo "CSB-Data folder was not created"
fi


