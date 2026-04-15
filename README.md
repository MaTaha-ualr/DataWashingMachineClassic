# Data Washing Machine (Classic)

**Maintainer (this fork):** Taha Mohammed

This repository now serves two related purposes:

1. It preserves the classic `dwm-refactor-v1` codebase from the Oysterer Bitbucket project.
2. It ships a cleaned `DWM_colab_bundle/` for running the newer comparator experiments, including `TahaComparator-CX`, locally or in Google Colab.

At a high level, the Data Washing Machine (DWM) is an iterative unsupervised entity resolution pipeline. Each pass selects unresolved records, generates candidate pairs through blocking, links pairs that exceed the matching threshold, computes transitive closure, accepts high-quality clusters, and removes accepted clusters before the next pass. The comparator is the key decision point in that loop.

## Upstream source

The original DWM refactor this repository builds on is hosted on Bitbucket:

- [oysterer / dwm-refactor-v1](https://bitbucket.org/oysterer/dwm-refactor-v1/src/master/)

When you cite or describe the core DWM algorithm, credit that upstream project as the primary software source. This GitHub repository is a fork with local extensions, benchmark tooling, and the cleaned Colab bundle described below.

Development of related work was supported in part by NSF award `OIA-1946391` (EPSCoR).

## What Is In This Repository

| Path | Purpose |
|------|---------|
| `oysterer-dwm-refactor-v1-30e2f7557bf1/` | Preserved classic DWM refactor snapshot plus earlier local capture and reporting changes |
| `DWM_colab_bundle/` | Clean upload-ready DWM package for comparator experiments and Colab runs |
| `Data files/` | Benchmark datasets, truth files, and parameter files used by the current experiment runners |
| `build_colab_upload_bundle.py` | Rebuilds a clean Colab upload zip from the tracked bundle and data files |
| `CITATION.cff`, `LICENSE` | Citation and licensing metadata for the fork and upstream attribution |

## Comparators In This Fork

The cleaned bundle supports the classic DWM comparators plus the Taha variants:

| Comparator | Summary |
|-----------|---------|
| `Cosine` | Bag-of-words cosine similarity over token lists |
| `MongeElkan` | Best-match token alignment from the shorter record into the longer record |
| `ScoringMatrixStd` | Token similarity matrix with positional weighting |
| `ScoringMatrixKris` | Refined scoring-matrix comparator |
| `TahaComparator` | Name/address decomposition, deterministic rules, and optional LLM review |
| `TahaComparator-CX` | Data-adaptive Taha variant that removes dataset-specific comparator constants |

## What TahaComparator-CX Adds

`TahaComparator-CX` keeps the DWM pipeline and the Taha-style decision flow, but replaces hardcoded comparator constants with three adaptive mechanisms:

- Statistical token role inference from the dataset's own frequency distribution
- Self-weighted evidence scoring, where each evidence channel weights itself by its own strength
- Score-based provisional context inside the current unresolved pool

The design notes and rationale are documented in [`DWM_colab_bundle/TahaComparator_CX_Documentation.md`](./DWM_colab_bundle/TahaComparator_CX_Documentation.md).

## S12 Benchmark Snapshot

The documentation includes a direct S12 comparison under the same DWM pipeline and blocking configuration:

| Variant | Precision | Recall | F1 | Notes |
|--------|-----------|--------|----|-------|
| `ScoringMatrixKris` | 0.9251 | 0.7056 | 0.8006 | Strongest classic non-LLM comparator on S12 |
| `TahaComparator + LLM review` | 0.9469 | 0.7746 | 0.8521 | Tuned baseline with about 2000 LLM reviews |
| `TahaComparator-CX v2` | 0.9556 | 0.7674 | 0.8512 | No LLM review and no dataset-specific comparator constants |

That result is the main point of the CX work: precision improves over the tuned baseline while keeping essentially the same F1, and the comparator no longer depends on a hand-tuned constant set.

## Quick Start

### 1. Clone

```powershell
git clone https://github.com/MaTaha-ualr/DataWashingMachineClassic.git
cd DataWashingMachineClassic
```

### 2. Install Dependencies

For the legacy classic package:

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

For the cleaned comparator bundle and benchmark workflow:

```powershell
pip install -r DWM_colab_bundle/requirements-colab.txt
```

### 3. Run The Classic Snapshot

The legacy snapshot lives under `oysterer-dwm-refactor-v1-30e2f7557bf1/` and keeps the older interactive driver flow:

```powershell
cd oysterer-dwm-refactor-v1-30e2f7557bf1
python DWM00_Driver.py
```

### 4. Run The Clean Bundle Locally

From the cleaned bundle directory:

```powershell
cd DWM_colab_bundle
python DWM00_Driver.py --parms-file S12-parms.cx-cpu.txt
```

That command runs the data-adaptive CX configuration without OpenAI review, using CPU-safe embedding settings.

If you want the tuned baseline with LLM review, set `OPENAI_API_KEY` first and run:

```powershell
$env:OPENAI_API_KEY="your_key_here"
cd DWM_colab_bundle
python DWM00_Driver.py --parms-file S12-parms.txt
```

## Comparator Benchmarks

Run these commands from the repository root.

### Single-dataset benchmark

```powershell
python DWM_colab_bundle/DWM_Comparator_Benchmark.py `
  --base-parms DWM_colab_bundle/S12-parms.txt `
  --variants cosine monge-elkan scoring-matrix-std scoring-matrix-kris taha-baseline taha-cx `
  --disable-openai-review `
  --force-embedding-device cpu `
  --output-root benchmark_runs `
  --run-label s12_compare
```

### All-datasets benchmark

```powershell
python DWM_colab_bundle/DWM_AllDatasets_Benchmark.py `
  --parms-glob "Data files/*-parms.txt" `
  --variants cosine monge-elkan scoring-matrix-std scoring-matrix-kris taha-baseline taha-cx `
  --disable-openai-review `
  --force-embedding-device cpu `
  --output-root benchmark_runs `
  --run-label all_datasets_compare
```

For Colab usage and the recommended GPU-oriented workflow, see [`DWM_colab_bundle/COLAB_UPLOAD_README.md`](./DWM_colab_bundle/COLAB_UPLOAD_README.md).

## Notes On The Clean Colab Bundle

`DWM_colab_bundle/` is intentionally tracked without generated outputs. The committed bundle excludes:

- `__pycache__/`
- `data_capture/`
- `DWM_Log_*.txt`
- `DWM_Results_*.xlsx`
- generated `*-LinkIndex.txt` files

If you want to recreate the zip that gets uploaded to Colab, run:

```powershell
python build_colab_upload_bundle.py
```

## Earlier Fork Changes Relative To Upstream

Earlier changes from the upstream Bitbucket refactor are still present in the legacy snapshot, including:

- `DWM_DataCapture.py` export hooks
- richer log and result naming in `DWM00_Driver.py`
- blocking metrics support in `DWM99_ERmetrics.py`
- local reporting and capture helpers

Those older notes are still summarized in:

- [`oysterer-dwm-refactor-v1-30e2f7557bf1/Changesmade to the code.txt`](./oysterer-dwm-refactor-v1-30e2f7557bf1/Changesmade%20to%20the%20code.txt)
- [`oysterer-dwm-refactor-v1-30e2f7557bf1/Changesinmetrics.txt`](./oysterer-dwm-refactor-v1-30e2f7557bf1/Changesinmetrics.txt)

## Citation

Use the upstream DWM refactor as the primary citation for the classic algorithm, and cite this repository when your work depends on the local benchmark tooling, capture changes, or TahaComparator-CX additions.

See [`CITATION.cff`](./CITATION.cff) for the repository metadata.

## Licensing

This repository is not presented as a blanket relicense of the upstream DWM code. Read [`LICENSE`](./LICENSE) for the attribution and usage expectations that apply to the upstream base and this fork's additions.
