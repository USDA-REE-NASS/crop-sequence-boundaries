# csb-project
Repository containing the code to create Crop Sequence Boundaries (CSB). 

# Set up environment
1. Git clone the repository to the local directory:
`git clone https://github.com/USDA-REE-NASS/crop-sequence-boundaries.git` 

2. Create folders listed in `config/csb_default.ini`


# Creating the CSB product
Run scripts to create CSB product

1. Open python command prompt and navigate to csb-project folder
2. Run each step of the CSB creation process:
	- `python CSB-Run/CSB-Run.py create START_YEAR END_YEAR`
	- `python CSB-Run/CSB-Run.py prep START_YEAR END_YEAR`
	- `python CSB-Run/CSB-Run.py distribute START_YEAR END_YEAR`
	
	
# Software requirements
	- ArcGIS pro 3.1 tested