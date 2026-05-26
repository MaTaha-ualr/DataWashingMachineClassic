# DWM Colab Bundle

This folder contains the runnable DWM/CODA code. Datasets and truth files are intentionally kept outside the code folder under `../data/`.

## Contents

- `DWM00_Driver.py`: DWM driver
- `DWM67_Tahacomparator.py`: CODA / TahaComparator-CX implementation
- `DWM55_LinkBlockPairs.py`: comparator orchestration
- `DWM_Comparator_Benchmark.py`: single-dataset benchmark runner
- `DWM_AllDatasets_Benchmark.py`: multi-dataset benchmark runner
- `S12-parms*.txt`: S12 runnable parameter presets that point to `../data/`
- `TahaComparator_CX_Documentation.md`: CODA mechanism and benchmark notes
- `COLAB_UPLOAD_README.md`: Colab workflow

This folder should not contain generated logs, Excel result files, data captures, generated link indexes, or dataset copies.

## Run CODA On S12

From this folder:

```powershell
python DWM00_Driver.py --parms-file S12-parms.cx-cpu.txt
```

The CPU preset enables:

- `tahaUseSoftRoleScoring=True`
- `tahaUseProvisionalContext=True`
- `tahaUseOpenAIReview=False`
- `embeddingDevice=cpu`

## Run Benchmarks

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

```powershell
python DWM_colab_bundle/DWM_AllDatasets_Benchmark.py `
  --parms-glob "data/*-parms.txt" `
  --variants cosine monge-elkan scoring-matrix-std scoring-matrix-kris taha-cx `
  --disable-openai-review `
  --force-embedding-device cpu `
  --output-root benchmark_runs `
  --run-label all_datasets_coda_compare
```
