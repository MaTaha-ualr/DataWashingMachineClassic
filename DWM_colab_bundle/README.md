# DWM Colab Bundle

This folder is the clean runnable package for CODA / `TahaComparator-CX` experiments inside the Data Washing Machine (DWM). It is designed to work after downloading the repository or after uploading the generated zip to Google Colab.

## What Is Included

- DWM pipeline modules used by the comparator experiments
- CODA / `TahaComparator-CX` implementation in `DWM67_Tahacomparator.py`
- single-dataset and all-datasets benchmark runners
- S12 example data, truth file, and parameter files
- CPU-safe and local-test parameter presets
- Colab dependency file: `requirements-colab.txt`
- detailed CODA documentation: `TahaComparator_CX_Documentation.md`

Generated logs, Excel result files, link-index files, `data_capture/`, and `__pycache__/` are intentionally not tracked.

## CODA In One Page

CODA stands for **Context-Driven Adaptive Comparator**. It is a data-adaptive comparator for DWM that improves pair decisions without requiring dataset-specific comparator constants.

CODA uses three mechanisms:

1. **Statistical token role inference**: tokens are assigned soft identity/location/numeric/volatile roles from the dataset's own frequency distribution.
2. **Self-weighted evidence scoring**: identity, context, numeric, and similarity evidence weight themselves by their own strength instead of a hand-tuned vector.
3. **Ephemeral provisional context**: strong edges in the current DWM iteration form a temporary local graph that adjusts ambiguous pair scores. The normal `mu` threshold still makes the final decision.

Across 22 benchmark datasets, CODA averages `P=0.932`, `R=0.845`, and `F1=0.881`, winning 19 of 22 F1 comparisons against the classic DWM comparator set without LLM review.

## Install

From the repository root:

```powershell
pip install -r DWM_colab_bundle/requirements-colab.txt
```

In Colab, run the same command after unzipping the repository or upload bundle.

## Run CODA On S12

From this folder:

```powershell
python DWM00_Driver.py --parms-file S12-parms.cx-cpu.txt
```

This preset enables:

- `tahaUseSoftRoleScoring=True`
- `tahaUseProvisionalContext=True`
- `tahaUseOpenAIReview=False`
- `embeddingDevice=cpu`

## Run A Single-Dataset Benchmark

From the repository root:

```powershell
python DWM_colab_bundle/DWM_Comparator_Benchmark.py `
  --base-parms DWM_colab_bundle/S12-parms.txt `
  --variants cosine monge-elkan scoring-matrix-std scoring-matrix-kris taha-cx `
  --disable-openai-review `
  --force-embedding-device cpu `
  --output-root benchmark_runs `
  --run-label s12_coda_compare
```

Use `--force-embedding-device cuda` in Colab or on a CUDA GPU machine.

## Run All Datasets

```powershell
python DWM_colab_bundle/DWM_AllDatasets_Benchmark.py `
  --parms-glob "Data files/*-parms.txt" `
  --variants cosine monge-elkan scoring-matrix-std scoring-matrix-kris taha-cx `
  --disable-openai-review `
  --force-embedding-device cpu `
  --output-root benchmark_runs `
  --run-label all_datasets_coda_compare
```

The all-datasets runner writes a master summary plus per-dataset benchmark folders.

## Main Files

| File | Purpose |
|------|---------|
| `DWM00_Driver.py` | DWM driver entry point |
| `DWM55_LinkBlockPairs.py` | two-pass comparator orchestration |
| `DWM67_Tahacomparator.py` | CODA/Taha comparator implementation |
| `DWM_Comparator_Benchmark.py` | single-dataset comparator benchmark runner |
| `DWM_AllDatasets_Benchmark.py` | multi-dataset benchmark runner |
| `S12-parms.cx-cpu.txt` | CPU-safe CODA parameter file |
| `COLAB_UPLOAD_README.md` | Colab-oriented run notes |
