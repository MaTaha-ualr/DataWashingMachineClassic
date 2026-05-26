DWM Refactor V1 is a Python code repository for the Data Washing Machine (DWM)
The creation of this repository was supported by NSF Grant Award No. OIA-1946391 under ESPSCoR Program.
This version of the DWM performs unsupervised data correction and entity resolution (ER) on a file contianing multiple sources of the same information. The repository contains eighteen sample file S1 to S18 with varying degrees of data quality problems and some with mixed record layouts.
The modules are written as Anaconda notebooks.
The driver module is DWM00_Driver
When you run DWM00_Driver it will ask for the name of Paramter File.
The Parameter file is text file defining
- the name of the input file to be processed
- the values for the parameter necessary to process the input file
Example parameter files have been provided to run samples S2G (S2-parms.txt) and S8P (S8-parms.txt)
The parameters are explained in the template parameter file "parms__File_Template.txt"
