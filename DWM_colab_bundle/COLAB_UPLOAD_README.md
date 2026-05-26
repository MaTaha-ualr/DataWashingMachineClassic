# Colab Upload Workflow

This repository can be packaged into a clean Colab-ready zip with:

```bash
python build_colab_upload_bundle.py
```

The zip contains the runnable DWM/CODA source bundle, benchmark datasets, parameter files, requirements, and documentation. It excludes generated logs, result spreadsheets, data-capture folders, caches, benchmark output folders, and link-index files.

## Recommended Colab Steps

1. Upload `DataWashingMachineClassic_ColabUpload.zip` to Colab or Google Drive.
2. Unzip it under `/content`.
3. Change into the extracted project root.
4. Install dependencies:

```bash
pip install -r DWM_colab_bundle/requirements-colab.txt
```

5. Run benchmarks with `--force-embedding-device cuda`.

## Single-Dataset CODA Example

```bash
python DWM_colab_bundle/DWM_Comparator_Benchmark.py \
  --base-parms DWM_colab_bundle/S12-parms.txt \
  --variants cosine monge-elkan scoring-matrix-std scoring-matrix-kris taha-cx \
  --disable-openai-review \
  --force-embedding-device cuda \
  --output-root /content/drive/MyDrive/dwm_benchmarks \
  --run-label s12_gpu_coda_compare
```

## All-Datasets Example

```bash
python DWM_colab_bundle/DWM_AllDatasets_Benchmark.py \
  --parms-glob "Data files/*-parms.txt" \
  --variants cosine monge-elkan scoring-matrix-std scoring-matrix-kris taha-cx \
  --disable-openai-review \
  --force-embedding-device cuda \
  --output-root /content/drive/MyDrive/dwm_benchmarks \
  --run-label all_datasets_gpu_coda_compare
```

The main outputs are:

- `summary.csv`
- `summary.md`
- per-dataset benchmark folders under `dataset_runs/`

## Resume After Disconnect

If Colab disconnects mid-run, rerun the same all-datasets command with `--resume`:

```bash
python DWM_colab_bundle/DWM_AllDatasets_Benchmark.py \
  --parms-glob "Data files/*-parms.txt" \
  --variants cosine monge-elkan scoring-matrix-std scoring-matrix-kris taha-cx \
  --disable-openai-review \
  --force-embedding-device cuda \
  --output-root /content/drive/MyDrive/dwm_benchmarks \
  --run-label all_datasets_gpu_coda_compare \
  --resume
```
