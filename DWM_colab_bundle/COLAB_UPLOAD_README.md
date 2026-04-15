# Colab Upload Bundle

This bundle is the clean Colab package for running the DWM comparator experiments.

## Included

- DWM pipeline code
- single-dataset comparator benchmark runner
- all-datasets comparator benchmark runner
- `DWM_colab_bundle` datasets and truth files
- `Data files` datasets and parameter files
- alias dictionary and word list
- documentation
- reference result workbooks/text files where present

## Excluded

- old logs
- old result spreadsheets
- old data capture folders
- `__pycache__`
- generated link-index outputs

## Recommended Colab Workflow

1. Upload this zip to Google Drive or directly to the Colab session.
2. Unzip it under `/content`.
3. `cd` into the extracted project root.
4. Install dependencies from `DWM_colab_bundle/requirements-colab.txt`.
5. Run the benchmark script with `--force-embedding-device cuda`.

## Single-Dataset Example

```bash
python DWM_colab_bundle/DWM_Comparator_Benchmark.py \
  --base-parms DWM_colab_bundle/S12-parms.txt \
  --variants cosine monge-elkan scoring-matrix-std scoring-matrix-kris taha-baseline taha-cx \
  --disable-openai-review \
  --force-embedding-device cuda \
  --output-root /content/drive/MyDrive/dwm_benchmarks \
  --run-label s12_gpu_compare
```

## All-Datasets Example

This command benchmarks every parameter file in `Data files` against the full comparator set and writes one master summary plus per-dataset subfolders:

```bash
python DWM_colab_bundle/DWM_AllDatasets_Benchmark.py \
  --parms-glob "Data files/*-parms.txt" \
  --variants cosine monge-elkan scoring-matrix-std scoring-matrix-kris taha-baseline taha-cx \
  --disable-openai-review \
  --force-embedding-device cuda \
  --output-root /content/drive/MyDrive/dwm_benchmarks \
  --run-label all_datasets_gpu_compare
```

The main outputs are:

- `/content/drive/MyDrive/dwm_benchmarks/all_datasets_gpu_compare/summary.csv`
- `/content/drive/MyDrive/dwm_benchmarks/all_datasets_gpu_compare/summary.md`
- per-dataset benchmark folders under `dataset_runs/`

## Resume Example

If Colab disconnects mid-run, rerun the same command with `--resume`:

```bash
python DWM_colab_bundle/DWM_AllDatasets_Benchmark.py \
  --parms-glob "Data files/*-parms.txt" \
  --variants cosine monge-elkan scoring-matrix-std scoring-matrix-kris taha-baseline taha-cx \
  --disable-openai-review \
  --force-embedding-device cuda \
  --output-root /content/drive/MyDrive/dwm_benchmarks \
  --run-label all_datasets_gpu_compare \
  --resume
```
