# Data Washing Machine Classic + CODA Comparator

**Maintainer of this fork:** Taha Mohammed

This repository preserves the classic Data Washing Machine (DWM) refactor and adds a clean, download-ready comparator bundle for CODA / `TahaComparator-CX` experiments.

DWM is an iterative unsupervised entity-resolution pipeline. Each pass selects unresolved records, creates candidate pairs through blocking, links pairs that meet the comparator threshold, computes transitive closure, accepts high-quality clusters, and removes those accepted clusters before the next pass.

The research contribution in this fork is the CODA comparator, also exposed in code as `TahaComparator-CX`: a data-adaptive comparator that improves matching decisions without dataset-specific comparator constants and without requiring LLM review.

## Repository Map

| Path | Purpose |
|------|---------|
| `oysterer-dwm-refactor-v1-30e2f7557bf1/` | Preserved classic DWM refactor snapshot with earlier local capture/reporting changes |
| `DWM_colab_bundle/` | Clean runnable bundle for CODA, TahaComparator-CX, classic comparator benchmarks, and Colab/local runs |
| `Data files/` | Benchmark datasets, truth files, and parameter files for multi-dataset runs |
| `build_colab_upload_bundle.py` | Rebuilds a clean zip for Google Colab upload |
| `CITATION.cff`, `LICENSE` | Citation, upstream attribution, and usage notes |

The root repository is intended to be readable and runnable after download. Generated logs, result workbooks, data-capture folders, caches, and link-index outputs are excluded from Git.

## Upstream Source

The classic DWM refactor this repository builds on is hosted on Bitbucket:

- [oysterer / dwm-refactor-v1](https://bitbucket.org/oysterer/dwm-refactor-v1/src/master/)

Use that upstream project as the primary source when citing or describing the original DWM algorithm. Cite this GitHub repository when using the local CODA comparator, benchmark tooling, capture hooks, cleaned Colab bundle, or local documentation.

Development of related work was supported in part by NSF award `OIA-1946391` under the EPSCoR program.

## CODA / TahaComparator-CX

CODA stands for **Context-Driven Adaptive Comparator**. In the code and parameter files, the same comparator is selected as `TahaComparator` with the CX options enabled:

- `tahaUseSoftRoleScoring=True`
- `tahaUseProvisionalContext=True`
- `tahaUseOpenAIReview=False`

CODA keeps the DWM peel-off pipeline, but changes how ambiguous record pairs are scored.

### Mechanism 1: Statistical Token Role Inference

Classic comparators often treat every token as equally informative. CODA does not. It infers soft token roles from the dataset's own token frequency distribution:

- identity evidence: rare name-like tokens
- location evidence: address/city/state-like tokens
- numeric evidence: street numbers, zip codes, SSN-like values
- volatile evidence: short/common/suffix-like tokens

The core rarity signal is:

```text
rarity = 1 - log(1 + freq) / log(1 + max_freq_in_dataset)
```

This makes the comparator portable. A rare surname and a common state abbreviation naturally receive different evidence weight without a hand-coded list of names, states, or street suffixes.

### Mechanism 2: Self-Weighted Evidence

CODA combines evidence channels by their own strength instead of a fixed hand-tuned weight vector:

```text
total = identity + context + numeric + similarity
base_score = (identity^2 + context^2 + numeric^2 + similarity^2) / total
```

Contradiction is penalized most strongly when address/context evidence is high but identity evidence is weak. This directly targets same-household false matches where two different people share an address.

### Mechanism 3: Ephemeral Provisional Context

CODA uses a two-pass comparator flow inside each DWM iteration:

1. Score all candidate pairs and classify strong edges, weak edges, review-band pairs, and rejects.
2. Build a temporary union-find graph from strong edges in the current unresolved pool.
3. Use that local graph to adjust review-band pair scores.
4. Let the normal DWM threshold `mu` make the final link decision.

The context graph is discarded after the iteration. This matters because DWM is a peel-off process: accepted records leave the unresolved pool, so a permanent global graph would not match the algorithm's semantics.

### Key Result

Across 22 datasets (`S1` through `S18`, plus `S12PX_R1` through `S12PX_R6`), CODA was benchmarked against the four classic DWM comparators under the same blocking and DWM parameters:

| Comparator Family | Average Precision | Average Recall | Average F1 |
|-------------------|------------------:|---------------:|-----------:|
| CODA / TahaComparator-CX | 0.932 | 0.845 | 0.881 |
| ScoringMatrixKris | - | - | 0.842 |

CODA wins 19 of 22 datasets on F-measure, with zero LLM calls and zero dataset-specific comparator constants inside the comparator.

The detailed mechanism writeup is in [`DWM_colab_bundle/TahaComparator_CX_Documentation.md`](./DWM_colab_bundle/TahaComparator_CX_Documentation.md).

## Quick Start

### 1. Clone

```powershell
git clone https://github.com/MaTaha-ualr/DataWashingMachineClassic.git
cd DataWashingMachineClassic
```

### 2. Install Dependencies

For the legacy classic snapshot:

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

For the CODA comparator bundle:

```powershell
pip install -r DWM_colab_bundle/requirements-colab.txt
```

### 3. Run CODA Locally On S12

```powershell
cd DWM_colab_bundle
python DWM00_Driver.py --parms-file S12-parms.cx-cpu.txt
```

This CPU-safe run enables soft role scoring and provisional context, disables OpenAI review, and uses the included S12 sample files.

### 4. Run The Classic Snapshot

```powershell
cd oysterer-dwm-refactor-v1-30e2f7557bf1
python DWM00_Driver.py
```

The classic driver prompts for parameter-file input.

## Comparator Benchmarks

Run these commands from the repository root.

### Single-Dataset Benchmark

```powershell
python DWM_colab_bundle/DWM_Comparator_Benchmark.py `
  --base-parms DWM_colab_bundle/S12-parms.txt `
  --variants cosine monge-elkan scoring-matrix-std scoring-matrix-kris taha-cx `
  --disable-openai-review `
  --force-embedding-device cpu `
  --output-root benchmark_runs `
  --run-label s12_coda_compare
```

### All-Datasets Benchmark

```powershell
python DWM_colab_bundle/DWM_AllDatasets_Benchmark.py `
  --parms-glob "Data files/*-parms.txt" `
  --variants cosine monge-elkan scoring-matrix-std scoring-matrix-kris taha-cx `
  --disable-openai-review `
  --force-embedding-device cpu `
  --output-root benchmark_runs `
  --run-label all_datasets_coda_compare
```

For Colab/GPU usage, see [`DWM_colab_bundle/COLAB_UPLOAD_README.md`](./DWM_colab_bundle/COLAB_UPLOAD_README.md).

## Build A Clean Colab Upload Zip

```powershell
python build_colab_upload_bundle.py
```

The generated zip includes source code, data files, docs, citation metadata, and requirements. It excludes regenerated outputs such as logs, workbooks, data-capture folders, caches, benchmark output folders, and link-index files.

## Citation

Use the upstream DWM refactor as the primary citation for the classic DWM algorithm. Cite this repository when using the CODA comparator, benchmark scripts, Colab bundle, data-capture changes, or local documentation.

See [`CITATION.cff`](./CITATION.cff) for repository metadata.

## Licensing

This repository is not presented as a blanket relicense of the upstream DWM code. Read [`LICENSE`](./LICENSE) for attribution and usage expectations that apply to the upstream base and this fork's additions.
