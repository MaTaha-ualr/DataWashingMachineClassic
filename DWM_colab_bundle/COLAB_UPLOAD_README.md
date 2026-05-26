# Colab Upload Workflow

Create the upload package from the repository root:

```bash
python build_colab_upload_bundle.py
```

The generated zip keeps the project layout clean:

- code and docs in `DWM_colab_bundle/`
- datasets and truth files in `data/`
- generated outputs excluded

## Recommended Colab Steps

1. Upload `DataWashingMachineClassic_ColabUpload.zip` to Colab or Google Drive.
2. Unzip it under `/content`.
3. Change into the extracted project root.
4. Install dependencies:

```bash
pip install -r DWM_colab_bundle/requirements-colab.txt
```

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
  --parms-glob "data/*-parms.txt" \
  --variants cosine monge-elkan scoring-matrix-std scoring-matrix-kris taha-cx \
  --disable-openai-review \
  --force-embedding-device cuda \
  --output-root /content/drive/MyDrive/dwm_benchmarks \
  --run-label all_datasets_gpu_coda_compare
```

Resume after a disconnect by rerunning the same all-datasets command with `--resume`.
