# Data Washing Machine (Classic)

**Author:** Taha Mohammed  

Python implementation of the **Data Washing Machine (DWM)** — unsupervised data cleaning and **entity resolution (ER)** on tabular inputs where the same real-world entities may appear with inconsistent formatting or quality. The pipeline uses blocking, linking, transitive closure, and optional global or block-level token correction; it reports cluster profiles and ER-oriented metrics.

Development of this line of work was supported in part by **NSF** award **OIA-1946391** (EPSCoR).

**Repository layout:** application code, sample inputs, and parameter examples live under:

`oysterer-dwm-refactor-v1-30e2f7557bf1/`

(See [`ReadMe.txt`](./oysterer-dwm-refactor-v1-30e2f7557bf1/ReadMe.txt) in that folder for the original short guide.)

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

## What is in this repository

| Item | Description |
|------|-------------|
| `DWM00_Driver.py` | Main entry point |
| `DWM*.py` | Processing stages (blocking, linking, clustering, metrics, reporting) |
| `S*.txt`, `*-parms.txt` | Sample data and example parameter files |
| `ReadMe.txt` | Brief usage notes from the refactor package |

Modules were authored from Jupyter/Anaconda notebook workflows; runnable sources are the `.py` files.

## License

Code in this repository is released under the [MIT License](./LICENSE).

## Citation

If you cite this software academically, you can use the metadata in [`CITATION.cff`](./CITATION.cff) or cite the repository URL:  
https://github.com/MaTaha-ualr/DataWashingMachineClassic
