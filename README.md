# Data Washing Machine (Classic)

**Maintainer (this fork):** Taha Mohammed  

Python implementation of the **Data Washing Machine (DWM)** — unsupervised data cleaning and **entity resolution (ER)** on tabular inputs where the same real-world entities may appear with inconsistent formatting or quality. The pipeline uses blocking, linking, transitive closure, and optional global or block-level token correction; it reports cluster profiles and ER-oriented metrics.

## Upstream source (cite this)

The **original DWM refactor** this tree comes from is hosted on Bitbucket:

**[oysterer / dwm-refactor-v1](https://bitbucket.org/oysterer/dwm-refactor-v1/src/master/)**

When you cite or write about the core algorithm and modules, **credit and link that project** as the primary software source. This GitHub repository is a **fork with local extensions** (data capture, logging, and metrics); see [Changes in this fork](#changes-in-this-fork-from-upstream) below.

Development of related work was supported in part by **NSF** award **OIA-1946391** (EPSCoR).

**Repository layout:** application code, sample inputs, and parameter examples live under:

`oysterer-dwm-refactor-v1-30e2f7557bf1/`

(See [`ReadMe.txt`](./oysterer-dwm-refactor-v1-30e2f7557bf1/ReadMe.txt) in that folder for the short guide that shipped with the refactor.)

## Changes in this fork (from upstream)

These are the main **additions and behavioral changes** relative to the Bitbucket `dwm-refactor-v1` baseline (see also [`Changesmade to the code.txt`](./oysterer-dwm-refactor-v1-30e2f7557bf1/Changesmade%20to%20the%20code.txt)):

| Area | What changed |
|------|----------------|
| **`DWM_DataCapture.py` (new)** | Saves intermediate pipeline state to CSV/JSON: `refDict`, link index, token frequencies, block pairs, linked pairs, clusters, etc., under `data_capture/<inputBase>_<timestamp>/`, with per-iteration subfolders `iteration_01`, `iteration_02`, … |
| **Truth / ground truth** | Optional truth file is loaded for captures; exports can include **truth IDs** alongside refs to analyze linking errors (see `load_truth_dict` and save helpers that take `truthDict`). |
| **`DWM00_Driver.py`** | Creates the capture tree; wires capture calls after each major step; **log file** name `DWM_Log_<inputBase>_<tag>.txt` and **Excel** `DWM_Results_<inputBase>_<tag>.xlsx` include the input basename and run tag (not generic names only). |
| **`DWM99_ERmetrics.py`** | Adds **`generateBlockingMetrics(blockPairList, iterationNum, refDict)`** — precision / recall / F-measure for **blocking** at **each iteration** (candidate pairs vs ground-truth pairs), logged with the iteration number. |
| **Reporting** | Reporting paths align with the named outputs above; `DWM100_ReportData` uses `DWMDataCaptureHeader.csv` where applicable. |

Parameter experiments and metric notes from tuning runs are summarized in [`Changesinmetrics.txt`](./oysterer-dwm-refactor-v1-30e2f7557bf1/Changesinmetrics.txt).

## Licensing

This repository is **not** released under a blanket MIT license on the whole codebase, because it **builds on upstream DWM** whose terms are set on Bitbucket. Read **[LICENSE](./LICENSE)** for attribution requirements and how to treat upstream vs local changes.

## Quick start

### 1. Clone

```bash
git clone https://github.com/MaTaha-ualr/DataWashingMachineClassic.git
cd DataWashingMachineClassic
```

### 2. Environment

Python 3.10+ recommended (3.8+ generally works). Create a virtual environment and install dependencies:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Run the driver

From the package directory (so imports resolve):

```bash
cd oysterer-dwm-refactor-v1-30e2f7557bf1
python DWM00_Driver.py
```

The driver prompts for:

1. **Single parameter file** or a **list** of parameter files.  
2. The parameter file name(s).

Parameter files are plain text: they set the input CSV path and processing options. Examples included in the repo include `S2-parms.txt` and `S8-parms.txt`. See [`parms_File_Template.txt`](./oysterer-dwm-refactor-v1-30e2f7557bf1/parms_File_Template.txt) or inline comments in sample `*-parms.txt` files for field meanings.

After a run, check the **`data_capture/`** folder (created next to the working directory) for exported snapshots.

## What is in this repository

| Item | Description |
|------|-------------|
| `DWM00_Driver.py` | Main entry point (includes capture hooks) |
| `DWM_DataCapture.py` | **Fork-specific** export utilities |
| `DWM*.py` | Processing stages (blocking, linking, clustering, metrics, reporting) |
| `S*.txt`, `*-parms.txt` | Sample data and example parameter files |
| `ReadMe.txt` | Brief usage notes from the refactor package |

Modules were authored from Jupyter/Anaconda notebook workflows; runnable sources are the `.py` files.

## How to cite

1. **Original DWM refactor (required for the core method):**  
   [https://bitbucket.org/oysterer/dwm-refactor-v1](https://bitbucket.org/oysterer/dwm-refactor-v1) — use the citation or author information from that project if they provide it.

2. **This fork (if you use the data capture, blocking iteration metrics, or other changes here):**  
   [https://github.com/MaTaha-ualr/DataWashingMachineClassic](https://github.com/MaTaha-ualr/DataWashingMachineClassic)  
   You can also use metadata in [`CITATION.cff`](./CITATION.cff), which lists **both** the upstream reference and this repository.
