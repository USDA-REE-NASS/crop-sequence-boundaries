#!/bin/bash

echo "Please enter the start year: "
read start_year

echo "Please enter the end year: "
read end_year


python CSB-Run/CSB-Run.py create "$start_year" "$end_year"

python CSB-Run/CSB-Run.py prep "$start_year" "$end_year"

python CSB-Run/CSB-Run.py distribute "$start_year" "$end_year"

#bash CSB-Validation_clip.bash "$start_year" "$end_year"

