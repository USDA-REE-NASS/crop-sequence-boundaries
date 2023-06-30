ECHO Starting getTif.cmd

@ECHO off
set /p year="Enter CDL year to download: "

ECHO Navigating to download folder

cd "X:\\CSB-Project\\CSB-Data\\v2.4\\Creation\\GEE\Year\\%year%"

ECHO Activating shared_dsw_base

CALL C:\ProgramData\Anaconda3/condabin/conda.bat activate shared_dsw_base

ECHO starting gsutil config

gsutil config 

ECHO Downloading all tifs files in bucket

gsutil -m cp -R gs://nass-csb-bucket/CDL_%year%*.tif .


PAUSE