# Data Folder

This folder contains the datasets, truth files, and parameter files used by the CODA/DWM benchmark runners.

Parameter files in this folder use paths relative to `data/`, which lets the benchmark runner resolve them cleanly. The S12 presets inside `DWM_colab_bundle/` point here through `../data/...`.

Generated link-index files, zip staging artifacts, logs, result spreadsheets, and data-capture folders should not be committed.
