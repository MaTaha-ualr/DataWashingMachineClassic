# CODA / TahaComparator-CX Documentation

CODA means **Context-Driven Adaptive Comparator**. In this repository it is implemented as the CX configuration of `TahaComparator`.

The goal is direct: improve DWM record-link decisions without hardcoding dataset-specific comparator constants and without relying on LLM review for the benchmarked configuration.

## 1. DWM Setting

The Data Washing Machine (DWM) is an iterative unsupervised entity-resolution pipeline. Each iteration:

1. selects unresolved records from the current pool
2. generates candidate pairs through blocking
3. compares candidate pairs and links accepted edges
4. computes transitive closure over linked pairs
5. accepts high-quality clusters
6. removes accepted clusters before the next iteration

This peel-off behavior is important. A comparator cannot rely on a permanent global graph, because records accepted in one iteration are absent from later iterations. Any context used by CODA must be local to the current unresolved pool and discarded after the iteration.

## 2. Pipeline Location

| Stage | Module | Role |
|-------|--------|------|
| Tokenization | `DWM14_BuildRefDict.py` | reads records and flattens fields into token lists |
| Token frequencies | `DWM16_BuildTokenFreqDict.py` | computes frequency statistics used by CODA |
| Global correction | `DWM25_Global_Token_Replace.py` | normalizes token variants |
| Blocking | `DWM42_BuildBlockPairs.py` | creates candidate pairs with embedding KNN |
| Comparator | `DWM55_LinkBlockPairs.py`, `DWM67_Tahacomparator.py` | scores candidate pairs and decides links |
| Transitive closure | `DWM80_TransitiveClosure.py` | forms connected components |
| Cluster acceptance | `DWM90_IterateClusters.py` | accepts clusters above `epsilon` |
| Metrics | `DWM99_ERmetrics.py` | reports precision, recall, and F-measure |

CODA operates at the comparator stage. Blocking sets the recall ceiling; CODA decides how much of that ceiling is realized at high precision.

## 3. Records And Evidence

The benchmark records are PII-like token sequences: names, addresses, and sometimes SSN or date of birth. The tokenizer flattens all fields into one ordered list:

```text
A965806: [TOMMY, ALAN, NOEL, 754, EMPIRE, AVE, VENTURA, CA, 93003, ...]
         |---- name ----|    |--------- address ---------|
```

The name/address boundary is not explicitly marked. CODA reconstructs likely token roles from observable signals such as position, frequency, token length, digit content, and rarity.

## 4. What CODA Replaces

The earlier Taha comparator improved over flat token scoring by splitting name and address evidence, applying deterministic guard rules, and optionally sending review-band pairs to an LLM.

That worked well, but the implementation depended on many comparator-internal constants tuned around one dataset. CODA keeps the useful structure and replaces those internal constants with adaptive computations.

CODA keeps:

- ScoringMatrix-style token similarity
- name/address decomposition
- deterministic guard rules for clear nonmatches
- optional LLM review support
- DWM's normal `mu` threshold

CODA adds:

- frequency-derived token roles
- self-weighted evidence scoring
- temporary provisional context inside each DWM iteration

## 5. Mechanism 1: Statistical Token Role Inference

The first problem CODA solves is that a flat token list does not say what each token means. In a record such as `TOMMY ALAN NOEL 754 EMPIRE AVE VENTURA CA 93003`, the name tokens and address tokens are mixed into one sequence. A normal token comparator can see that two records share `CA` or `754`, but it does not know whether that evidence is strong identity evidence or weak context evidence.

CODA handles this by assigning each token a soft distribution over four roles:

- `identity`: names and rare identity-like values
- `location`: address, city, state, and street-like values
- `numeric`: street numbers, ZIP codes, SSN-like values, and other digit-bearing tokens
- `volatile`: short, common, suffix-like, or weak evidence

The word **soft** matters. A token does not need to be only one thing. For example, a street name can be mostly location evidence but still carry some identity-like value if it is rare. A state abbreviation can be location evidence and volatile evidence at the same time because it is meaningful but very common.

The central signal is rarity:

```text
rarity = 1 - log(1 + freq) / log(1 + max_freq_in_dataset)
```

This scales automatically to the current dataset. A token that appears once receives high rarity; a state abbreviation appearing thousands of times receives low rarity.

Observable signals then contribute to soft roles:

| Signal | Typical Effect |
|--------|----------------|
| before first digit | increases identity evidence |
| after first digit | increases location evidence |
| contains digits | increases numeric evidence |
| high rarity | strengthens identity-like evidence |
| very common token | increases volatile evidence |
| short token | increases volatile evidence |
| rare long alphabetic token | strengthens identity-like evidence |

The final role weights are normalized so each token has an interpretable identity/location/numeric/volatile profile. The comparator then uses those role weights when deciding whether shared tokens are actually useful.

### Why This Helps

Two records sharing `NOEL` is very different from two records sharing `CA`. A hardcoded comparator has to encode that distinction with fixed rules or fixed thresholds. CODA derives the distinction from the dataset:

- rare alphabetic tokens before the address region tend to become strong identity evidence
- common short tokens tend to become volatile evidence
- digit-bearing tokens become numeric evidence, but are not treated as names
- address-region alphabetic tokens become location evidence unless their rarity says they are unusually informative

This makes the same code more portable across datasets. If a token is common in one dataset and rare in another, its role changes with the data.

### Example Interpretation

For an S12-style record:

```text
[TOMMY, ALAN, NOEL, 754, EMPIRE, AVE, VENTURA, CA, 93003]
```

CODA should treat the tokens roughly as follows:

| Token Type | Expected Role Interpretation |
|------------|------------------------------|
| `TOMMY`, `ALAN`, `NOEL` | mostly identity |
| `754`, `93003` | mostly numeric, with location support |
| `EMPIRE`, `VENTURA` | mostly location, but more useful than very common address tokens |
| `AVE`, `CA` | location plus volatile/common-token evidence |

The important point is that CODA does not need a hand-written list saying that `CA` is a state or that `AVE` is a street suffix. The role inference is based on frequency, position, token shape, and digit content.

## 6. Mechanism 2: Self-Weighted Evidence

The second problem CODA solves is evidence balancing. Entity-resolution pairs are not all ambiguous in the same way. Some pairs have strong name agreement and weak address agreement. Others have the same address but conflicting names. A fixed formula such as "60% name, 30% address, 10% numeric" forces the same tradeoff on every pair.

CODA computes evidence channels for each candidate pair:

- identity evidence
- context/location evidence
- numeric evidence
- base token similarity
- contradiction evidence

Instead of combining the positive channels with fixed coefficients, CODA lets each channel weight itself by its own strength:

```text
total = identity + context + numeric + similarity
base_score = (identity^2 + context^2 + numeric^2 + similarity^2) / total
```

If identity evidence is very strong and context evidence is weak, identity dominates. If both are moderate, they contribute similarly. This avoids imposing the same weight vector on every dataset and every pair.

Contradiction is penalized most strongly when context is stronger than identity:

```text
penalty = 0.5 * contradiction * max(0, context - identity)
```

This is designed for same-household errors: two records may share an address, but if their names strongly disagree, address agreement should not be allowed to dominate.

### What The Channels Mean

| Channel | What It Measures | Why It Matters |
|---------|------------------|----------------|
| identity | agreement among name-like and rare identity-like tokens | separates true person matches from same-address nonmatches |
| context | agreement among location/address-like tokens | helps recover moved/abbreviated/noisy records when identity is plausible |
| numeric | agreement among numeric values | captures strong signals such as street numbers, ZIP codes, and ID-like values |
| similarity | general token-level similarity | preserves the useful ScoringMatrix-style baseline behavior |
| contradiction | active disagreement, especially in names | prevents address-only matches from becoming false positives |

### True Match Versus Same-Household Pair

Consider two pairs:

```text
True match:
R1 = TOMMY ALAN NOEL 754 EMPIRE AVE VENTURA CA 93003
R2 = TOMMY      NOEL 754 EMPIRE AVENUE VENTYRA CA 93003

Same-household nonmatch:
R1 = TOMMY ALAN NOEL 754 EMPIRE AVE VENTURA CA 93003
R3 = JAMES LEE CHEN  754 EMPIRE AVENUE VENTURA CA 93003
```

A flat comparator can be fooled because both pairs share many address tokens. CODA separates them:

- The true match has strong identity evidence, strong numeric evidence, and manageable spelling/address noise.
- The same-household pair has strong context evidence but weak identity evidence and high contradiction.

That distinction is the reason contradiction is not just another negative number. It is applied most strongly when context is trying to overpower identity.

## 7. Mechanism 3: Ephemeral Provisional Context

The third problem CODA solves is the borderline-pair problem. Some pairs are not strong enough to accept from pairwise evidence alone, but they are not weak enough to reject safely. In the original comparator, these pairs were candidates for LLM review. CODA can recover some of them by looking at local structure inside the current DWM iteration.

CODA uses a two-pass process inside `DWM55_LinkBlockPairs.py`.

### Pass 1

Every candidate pair is scored and classified into an edge type:

- `must_link`
- `likely_link`
- `review_band`
- `context_only_overlap`
- `likely_nonmatch`
- `cannot_link`

Strong edges are saved for provisional context. Ambiguous review-band pairs are held for pass 2. Rejected pairs do not become graph evidence.

### Pass 2

CODA builds a union-find graph from strong pass-1 edges in the current unresolved pool. For each review-band pair, it computes:

- `local_support`: whether nearby strong edges support the match
- `local_conflict`: whether local evidence contradicts the match
- `cluster_impact`: whether accepting the edge improves or harms the provisional component

The graph does not make the decision directly. It adjusts the score:

```text
context_adjustment = (local_support - local_conflict) * abs(cluster_impact)
revised_score = base_score + context_adjustment
```

Then the normal DWM threshold decides:

```text
accept if revised_score >= mu and identity >= context
```

The provisional graph is discarded after the iteration.

### Why The Graph Is Temporary

DWM is not a one-shot global clustering algorithm. It repeatedly accepts high-quality clusters and removes those records from the unresolved pool. That means a global graph from iteration 1 would be stale in iteration 2.

CODA respects this by building context only from the current unresolved pool:

- build strong provisional components for the current iteration
- use those components only to adjust review-band pairs
- discard the components after the iteration completes

This gives CODA useful local context without changing DWM into a different algorithm.

### What Context Can And Cannot Do

Context can:

- nudge a borderline pair upward when both records connect to the same reliable local component
- nudge a pair downward when local evidence shows conflict
- help recover pairs with minor spelling, abbreviation, or missing-middle-name noise

Context cannot:

- override `mu`
- turn context-only/address-only agreement into a match when identity is weaker than context
- persist across iterations

This is the core rule: **context adjusts the score; it does not replace the comparator decision.**

## 8. Complete Decision Flow

For each candidate pair:

1. compute token similarity
2. split tokens into name/address regions
3. compute name and address similarity details
4. infer soft token roles from dataset frequency statistics
5. compute identity, context, numeric, similarity, and contradiction evidence
6. compute the self-weighted base score
7. apply deterministic guard rules
8. classify the pair after pass 1
9. build provisional components from strong pass-1 edges
10. adjust review-band scores using local context
11. accept only when the revised score satisfies `mu` and identity evidence is not dominated by context

The design principle is: **context refines the score; `mu` still decides.**

## 9. How The Three Mechanisms Work Together

The mechanisms are designed to solve different parts of the same decision problem:

| Problem | CODA Mechanism | Result |
|---------|----------------|--------|
| flat tokens do not expose meaning | statistical token role inference | name-like, address-like, numeric, and weak tokens are separated without schema labels |
| fixed weights do not fit every pair | self-weighted evidence | the strongest evidence channel for a pair naturally has more influence |
| pairwise evidence misses borderline matches | ephemeral provisional context | local graph support can refine ambiguous scores without overriding `mu` |

The flow is intentionally conservative. CODA first asks what each token is likely to mean, then asks which evidence channels are strong for this pair, then uses local context only for pairs that remain ambiguous.

This keeps the comparator auditable. A decision can be explained in terms of:

- token roles
- identity/context/numeric/contradiction evidence
- edge type
- any provisional-context adjustment
- final comparison to `mu`

## 10. Benchmark Results

CODA was benchmarked against the classic DWM comparators on 22 datasets:

- `S1` through `S18`
- `S12PX_R1` through `S12PX_R6`

All runs used the same DWM parameters, embedding-based KNN blocking, `topK=10`, and no LLM review.

| Result | Value |
|--------|------:|
| CODA average precision | 0.932 |
| CODA average recall | 0.845 |
| CODA average F1 | 0.881 |
| next-best classic comparator average F1 (`ScoringMatrixKris`) | 0.842 |
| datasets won by CODA on F1 | 19 / 22 |
| LLM calls used by CODA benchmark | 0 |
| dataset-specific comparator constants | 0 |

The main interpretation is that CODA improves the balance of precision and recall across datasets while staying inside the DWM comparator interface.

## 11. Novelty Claim

The individual ideas of blocking, context-aware ER, and evidence decomposition are not claimed as new on their own.

The contribution is the combination inside the DWM peel-off setting:

> A data-adaptive comparator for DWM that combines embedding-based candidate generation, frequency-derived soft token roles, self-weighted evidence scoring, and ephemeral unresolved-pool context to improve pair decisions without dataset-specific comparator constants or LLM review.

## 12. Code Locations

| Component | File | Key Functions |
|-----------|------|---------------|
| token role inference | `DWM67_Tahacomparator.py` | `_compute_dataset_stats`, `_infer_soft_token_roles` |
| evidence scoring | `DWM67_Tahacomparator.py` | `_soft_role_evidence` |
| provisional context | `DWM67_Tahacomparator.py` | `_build_provisional_components`, `_finalize_context_decision` |
| two-pass orchestration | `DWM55_LinkBlockPairs.py` | `linkBlockPairs` |
| deterministic guard rules | `DWM67_Tahacomparator.py` | `_deterministic_rule_decision` |
| pair comparison entry | `DWM67_Tahacomparator.py` | `compare_pair` |
| edge classification | `DWM67_Tahacomparator.py` | `_edge_type_from_scores` |

## 13. Reproduction

Run CODA on S12 from this folder:

```powershell
python DWM00_Driver.py --parms-file S12-parms.cx-cpu.txt
```

Run a single-dataset benchmark from the repository root:

```powershell
python DWM_colab_bundle/DWM_Comparator_Benchmark.py `
  --base-parms DWM_colab_bundle/S12-parms.txt `
  --variants cosine monge-elkan scoring-matrix-std scoring-matrix-kris taha-cx `
  --disable-openai-review `
  --force-embedding-device cpu `
  --output-root benchmark_runs `
  --run-label s12_coda_compare
```

Run all datasets from the repository root:

```powershell
python DWM_colab_bundle/DWM_AllDatasets_Benchmark.py `
  --parms-glob "data/*-parms.txt" `
  --variants cosine monge-elkan scoring-matrix-std scoring-matrix-kris taha-cx `
  --disable-openai-review `
  --force-embedding-device cpu `
  --output-root benchmark_runs `
  --run-label all_datasets_coda_compare
```

Use `--force-embedding-device cuda` for Colab/GPU runs.

## 14. Status

Completed:

- data-adaptive token role inference
- self-weighted evidence scoring
- provisional context pass
- 22-dataset benchmark
- clean Colab/download bundle

Recommended next work:

- run an ablation study: soft roles only, provisional context only, both
- test optional LLM review for recall recovery on review-band pairs
- inspect the few datasets where classic comparators remain very close
