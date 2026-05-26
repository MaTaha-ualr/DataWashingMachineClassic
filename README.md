# CODA Data Washing Machine Comparator

**Maintainer:** Taha Mohammed

This repository is a clean, runnable research package for CODA, the **Context-Driven Adaptive Comparator** for the Data Washing Machine (DWM) entity-resolution pipeline.

CODA is implemented in code as the `TahaComparator-CX` configuration. It keeps the DWM peel-off workflow but makes the comparator more adaptive: token roles are inferred from the dataset, evidence channels weight themselves, and ambiguous pairs are refined with temporary local context from the current unresolved pool.

## Repository Layout

| Path | Purpose |
|------|---------|
| `DWM_colab_bundle/` | Runnable DWM/CODA code, comparator benchmark scripts, and CODA documentation |
| `data/` | Datasets, truth files, and dataset parameter files |
| `build_colab_upload_bundle.py` | Creates a clean zip for Colab or sharing |
| `requirements.txt` | Minimal classic runtime dependencies |
| `CITATION.cff`, `LICENSE` | Citation and attribution metadata |

Generated artifacts are intentionally excluded from Git: logs, Excel results, `data_capture/`, benchmark output folders, caches, generated link-index files, and upload zips.

## What CODA Adds

CODA improves pair-level matching decisions through three mechanisms.

### 1. Statistical Token Role Inference

Each token receives soft identity/location/numeric/volatile role weights from observable dataset statistics. The main rarity signal is:

```text
rarity = 1 - log(1 + freq) / log(1 + max_freq_in_dataset)
```

This lets CODA treat a rare surname differently from a common state abbreviation without hardcoded lists of names, street suffixes, or locations.

### 2. Self-Weighted Evidence

CODA computes identity, context, numeric, and similarity evidence, then lets each channel weight itself:

```text
total = identity + context + numeric + similarity
base_score = (identity^2 + context^2 + numeric^2 + similarity^2) / total
```

Contradiction is penalized most when context is strong but identity is weak, which helps avoid same-household false matches.

### 3. Ephemeral Provisional Context

Inside each DWM iteration, CODA builds a temporary graph from strong pass-1 edges. Review-band pairs receive a small score adjustment from local support/conflict, then the normal DWM `mu` threshold still decides.

The graph is discarded after the iteration, preserving DWM's peel-off semantics.

## Benchmark Headline

Across 22 datasets (`S1` through `S18`, plus `S12PX_R1` through `S12PX_R6`), CODA was benchmarked against the classic DWM comparator set under the same blocking and DWM parameters:

| Comparator | Average Precision | Average Recall | Average F1 |
|------------|------------------:|---------------:|-----------:|
| CODA / TahaComparator-CX | 0.932 | 0.845 | 0.881 |
| ScoringMatrixKris | - | - | 0.842 |

CODA wins 19 of 22 datasets on F-measure with zero LLM calls and no dataset-specific comparator constants.

Detailed notes are in [`DWM_colab_bundle/TahaComparator_CX_Documentation.md`](./DWM_colab_bundle/TahaComparator_CX_Documentation.md).

## Quick Start

```powershell
git clone https://github.com/MaTaha-ualr/DataWashingMachineClassic.git
cd DataWashingMachineClassic
python -m venv .venv
.venv\Scripts\activate
pip install -r DWM_colab_bundle/requirements-colab.txt
```

Run CODA on the included S12 sample:

```powershell
cd DWM_colab_bundle
python DWM00_Driver.py --parms-file S12-parms.cx-cpu.txt
```

The S12 parameter files reference data through `../data/...`, so code and datasets stay separate.

## Benchmarks

Run from the repository root.

Single dataset:

```powershell
python DWM_colab_bundle/DWM_Comparator_Benchmark.py `
  --base-parms DWM_colab_bundle/S12-parms.txt `
  --variants cosine monge-elkan scoring-matrix-std scoring-matrix-kris taha-cx `
  --disable-openai-review `
  --force-embedding-device cpu `
  --output-root benchmark_runs `
  --run-label s12_coda_compare
```

All datasets:

```powershell
python DWM_colab_bundle/DWM_AllDatasets_Benchmark.py `
  --parms-glob "data/*-parms.txt" `
  --variants cosine monge-elkan scoring-matrix-std scoring-matrix-kris taha-cx `
  --disable-openai-review `
  --force-embedding-device cpu `
  --output-root benchmark_runs `
  --run-label all_datasets_coda_compare
```

Use `--force-embedding-device cuda` on a CUDA machine or in Colab.

## Build A Clean Upload Zip

```powershell
python build_colab_upload_bundle.py
```

The zip includes `DWM_colab_bundle/`, `data/`, root documentation, citation metadata, license text, and requirements. It excludes generated outputs and local caches.

## Citation

Use [`CITATION.cff`](./CITATION.cff) when citing this repository. Cite CODA when using the adaptive comparator, benchmark runners, cleaned data/code layout, or Colab bundle.

## License And Attribution

See [`LICENSE`](./LICENSE). This repository includes DWM-derived code plus local CODA comparator work, benchmark tooling, and documentation.
