#!/bin/sh

echo "Start moving file from Distribution folder to Production Folder"

echo "Please enter CSB year (ex: 1320): "
read CSB_yr

cd ..

dist_file="/CSB-Data/v2.5/Distribution/distribute_${CSB_yr}"

for f in *
do
	if[ "$f" = "$dist_file"*]
		echo f
done
