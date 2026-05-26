# CODA / TahaComparator-CX Technical Documentation

CODA means **Context-Driven Adaptive Comparator**. In this repository it is implemented as the CX configuration of `TahaComparator`.

This document is written as the technical foundation for a paper. It explains the problem, the DWM pipeline context, the CODA mechanisms, the implementation, the evaluation setup, the results, and the limits of the current work.

## Abstract

Entity-resolution systems must decide whether noisy records refer to the same real-world entity. In the Data Washing Machine (DWM), that decision is made inside an iterative peel-off pipeline: each iteration generates candidate pairs, links accepted pairs, forms clusters through transitive closure, accepts high-quality clusters, and removes accepted records before the next iteration. This creates a specific constraint for comparator design. The comparator can use local context from the current unresolved pool, but it cannot rely on a permanent global graph because the graph changes after every peel-off step.

CODA addresses this setting with three connected ideas. First, it infers soft token roles from the dataset's own frequency distribution, so tokens behave as identity, location, numeric, or volatile evidence without requiring schema labels or hand-written token lists. Second, it combines identity, context, numeric, and general similarity evidence through self-weighted scoring, so the strongest evidence for a pair naturally has more influence. Third, it builds an ephemeral provisional graph from confident first-pass edges and uses that graph only to adjust ambiguous pair scores before the normal DWM `mu` threshold makes the final decision.

Across 22 datasets, CODA achieved average precision 0.932, average recall 0.845, and average F1 0.881, winning 19 of 22 F1 comparisons against the classic DWM comparator set without LLM review and without dataset-specific comparator constants.

## 1. Terminology

| Term | Meaning |
|------|---------|
| record | one input reference to a possible real-world entity |
| token | normalized unit extracted from a record, such as a name, address word, number, ZIP code, or state |
| candidate pair | two records selected by blocking for detailed comparison |
| comparator | module that scores a candidate pair and decides whether it should become an edge |
| edge | accepted link between two records |
| cluster | connected component produced by transitive closure over accepted edges |
| unresolved pool | records not yet accepted into final clusters |
| peel-off iteration | one DWM cycle that accepts clusters and removes them from the unresolved pool |
| `mu` | DWM pair-link threshold |
| `epsilon` | DWM cluster-acceptance threshold |
| review band | score region where a pair is neither clear accept nor clear reject |
| CODA | Context-Driven Adaptive Comparator, implemented as `TahaComparator-CX` |

## 2. Problem Statement

The comparator receives two flattened token lists and must decide whether they refer to the same entity. The difficult cases are not the obvious matches or obvious nonmatches. The difficult cases are pairs where some evidence agrees and some evidence conflicts:

```text
True match:
R1 = TOMMY ALAN NOEL 754 EMPIRE AVE    VENTURA CA 93003
R2 = TOMMY      NOEL 754 EMPIRE AVENUE VENTYRA CA 93003

Same-household nonmatch:
R1 = TOMMY ALAN NOEL 754 EMPIRE AVE    VENTURA CA 93003
R3 = JAMES LEE  CHEN 754 EMPIRE AVENUE VENTURA CA 93003
```

Both pairs share many address tokens. A flat token comparator can overvalue the shared address and undervalue the name disagreement. CODA is designed to separate these cases by asking:

1. What kind of evidence does each token provide?
2. Which evidence channel is strongest for this pair?
3. Is there reliable local context from the current iteration that should refine an ambiguous score?

## 3. Design Goals

CODA is guided by these design goals:

| Goal | Design Response |
|------|-----------------|
| avoid dataset-specific comparator constants | derive token roles from dataset frequency statistics |
| preserve DWM semantics | keep `mu`, `epsilon`, transitive closure, and peel-off iteration unchanged |
| reduce false positives from shared address/context | penalize contradiction when context dominates identity |
| recover borderline true matches | use provisional context only for review-band pairs |
| remain auditable | emit edge types and evidence values that explain a decision |
| run without LLM review | keep optional LLM review disabled in the reported CODA benchmark |

CODA does not try to replace DWM. It changes the pair-comparison decision while keeping the surrounding pipeline intact.

## 4. DWM Pipeline Context

DWM is an iterative unsupervised entity-resolution pipeline. Each iteration:

1. selects unresolved records
2. tokenizes records and computes token frequencies
3. applies optional global token correction
4. creates candidate pairs through blocking
5. compares candidate pairs
6. links accepted pairs
7. computes transitive closure
8. accepts clusters whose quality is at least `epsilon`
9. removes accepted clusters before the next iteration

The comparator sits between blocking and transitive closure.

| Stage | Module | CODA-Relevant Role |
|-------|--------|--------------------|
| Tokenization | `DWM14_BuildRefDict.py` | creates token lists used by CODA |
| Token frequencies | `DWM16_BuildTokenFreqDict.py` | provides frequency statistics for token roles |
| Blocking | `DWM42_BuildBlockPairs.py` | sets the candidate-pair recall ceiling |
| Pair linking | `DWM55_LinkBlockPairs.py` | orchestrates CODA comparison and provisional context pass |
| Comparator | `DWM67_Tahacomparator.py` | implements role inference, evidence scoring, edge typing, and context adjustment |
| Transitive closure | `DWM80_TransitiveClosure.py` | turns accepted edges into clusters |
| Cluster acceptance | `DWM90_IterateClusters.py` | accepts clusters above `epsilon` |
| Metrics | `DWM99_ERmetrics.py` | computes precision, recall, and F-measure |

### Why Peel-Off Matters

DWM is not a static graph algorithm. After an iteration accepts a cluster, those records leave the unresolved pool. A graph built in iteration 1 may not be valid in iteration 2. This affects comparator design:

- persistent global context would violate the changing unresolved-pool structure
- local context must be recomputed each iteration
- context should refine pair decisions, not create a separate global clustering rule

CODA's provisional graph is intentionally temporary for this reason.

## 5. Data Representation

The benchmark records contain PII-like fields: names, addresses, and sometimes SSN or date of birth. The tokenizer flattens fields into a single ordered token list:

```text
A965806: [TOMMY, ALAN, NOEL, 754, EMPIRE, AVE, VENTURA, CA, 93003, ...]
         |---- name ----|    |--------- address ---------|
```

The field boundary is not explicitly represented inside the comparator. CODA must infer evidence roles from observable signals:

- token position
- first digit position
- token frequency
- token length
- alphabetic versus numeric shape
- rarity within the dataset
- name/address similarity details from the Taha comparator foundation

This is why CODA is described as schema-light. It can use the ordering and token statistics without requiring a formal schema alignment step.

## 6. Baseline Comparator Problem

The classic DWM comparators all produce a score in `[0, 1]`:

| Comparator | General Behavior |
|------------|------------------|
| Cosine | bag-of-words similarity over token lists |
| MongeElkan | best token alignment from shorter record to longer record |
| ScoringMatrixStd | edit-distance matrix with greedy token matching |
| ScoringMatrixKris | refined scoring matrix with positional weighting |
| TahaComparator baseline | name/address decomposition, deterministic rules, optional LLM review |

These approaches are useful, but the difficult cases require more structure than one flat score. The key limitations are:

- shared common tokens can be overvalued
- address agreement can hide name disagreement
- fixed evidence weights do not fit every pair
- ambiguous pairs need context, but DWM cannot use a permanent global graph

CODA addresses these limitations directly.

## 7. CODA Architecture Overview

CODA has three mechanisms:

1. **Statistical token role inference**: infer soft token roles from dataset statistics.
2. **Self-weighted evidence scoring**: combine evidence channels according to their own strength.
3. **Ephemeral provisional context**: refine ambiguous scores using temporary local graph evidence.

The high-level flow is:

```text
candidate pair
  -> token/name/address comparison
  -> soft token role inference
  -> identity/context/numeric/contradiction evidence
  -> self-weighted base score
  -> deterministic guard rules
  -> edge type classification
  -> provisional context adjustment for review-band pairs
  -> final comparison to mu
```

The design principle is:

```text
context refines the score; mu still decides
```

## 8. Mechanism 1: Statistical Token Role Inference

### 8.1 Motivation

In a flattened record, tokens do not announce whether they are names, address words, ID numbers, or weak common tokens. CODA infers that role from data instead of hardcoding it.

Two records sharing `NOEL` is more meaningful than two records sharing `CA`. Two records sharing `754` may be strong address evidence, but not necessarily identity evidence. CODA needs these distinctions before it can compare pairs correctly.

### 8.2 Role Types

Each token receives soft weights over four roles:

| Role | Meaning |
|------|---------|
| identity | name-like or rare entity-identifying evidence |
| location | address, city, state, street, or place-like evidence |
| numeric | digit-bearing evidence such as street number, ZIP code, SSN-like value |
| volatile | weak, common, short, suffix-like, or low-discrimination evidence |

The weights are soft, not hard. A token can be partly location and partly volatile. A rare street name can be mostly location with some identity-like value.

### 8.3 Rarity Signal

CODA computes rarity from the token frequency distribution:

```text
rarity(t) = 1 - log(1 + freq(t)) / log(1 + max_freq)
```

Where:

- `freq(t)` is the frequency of token `t`
- `max_freq` is the highest token frequency in the dataset

This scales automatically. If a token is rare in the current dataset, rarity is high. If it is common, rarity is low. The formula avoids fixed thresholds such as "frequency <= 8 is rare" as the primary decision rule.

### 8.4 Observable Signals

CODA combines several signals:

| Signal | Typical Contribution |
|--------|----------------------|
| before first digit | identity |
| after first digit | location |
| contains digits | numeric |
| high rarity | identity |
| low rarity and short length | volatile |
| alphabetic after digit | location |
| rare long alphabetic token | identity support |
| suffix-like short token | volatile |

### 8.5 Role-Inference Pseudocode

```text
for token in record_tokens:
    role = {identity: 0, location: 0, numeric: 0, volatile: 0}

    rarity = rarity_from_frequency(token)
    position = token_index / record_length
    is_numeric = token_contains_digit(token)
    is_short = token_length <= 2
    before_address = token_index < first_digit_index

    if before_address:
        role.identity += position_adjusted_identity_signal
    else:
        role.location += address_region_signal

    if is_numeric:
        role.numeric += numeric_signal
        role.location += numeric_location_support

    if token_is_alphabetic:
        role.identity += rarity_signal
        role.location += common_address_signal

    if is_short or token_is_very_common:
        role.volatile += weak_token_signal

    normalize role so all role weights sum to 1
```

The actual code is in `_compute_dataset_stats` and `_infer_soft_token_roles` in `DWM67_Tahacomparator.py`.

### 8.6 Example Interpretation

For:

```text
[TOMMY, ALAN, NOEL, 754, EMPIRE, AVE, VENTURA, CA, 93003]
```

CODA should interpret the tokens roughly as:

| Token Type | Expected Interpretation |
|------------|-------------------------|
| `TOMMY`, `ALAN`, `NOEL` | mostly identity |
| `754`, `93003` | mostly numeric with location support |
| `EMPIRE`, `VENTURA` | mostly location, potentially informative if rare |
| `AVE`, `CA` | location plus volatile/common-token evidence |

The point is not that CODA knows these labels from a dictionary. The point is that the dataset statistics and token shape make these roles emerge.

## 9. Mechanism 2: Self-Weighted Evidence Scoring

### 9.1 Motivation

Fixed evidence weights are fragile. A pair with strong name evidence and weak address evidence should not be treated the same as a pair with weak name evidence and strong address evidence. CODA lets evidence channels influence the score according to their own strength.

### 9.2 Evidence Channels

CODA computes these channels:

| Channel | Measures | Risk If Ignored |
|---------|----------|-----------------|
| identity | agreement among name-like and rare tokens | true identity evidence is diluted |
| context | agreement among location/address-like tokens | useful address evidence is lost |
| numeric | agreement among digit-bearing values | strong numeric agreement is underused |
| similarity | general token similarity | baseline edit/token similarity is discarded |
| contradiction | active disagreement, especially in identity evidence | false positives from shared context increase |

### 9.3 Positive Evidence Formula

The positive channels are combined as:

```text
total = identity + context + numeric + similarity
base_score = (identity^2 + context^2 + numeric^2 + similarity^2) / total
```

This is strength-proportional. A channel with value `0.90` contributes `0.81`; a channel with value `0.20` contributes `0.04`. Strong evidence naturally dominates weak evidence.

If all channels are weak, the score stays weak. If multiple channels are strong, the score rises. If one channel is strong and the others are weak, the score reflects that imbalance.

### 9.4 Contradiction Penalty

Contradiction is not applied equally in every case. It matters most when context is trying to overpower identity:

```text
penalty = 0.5 * contradiction * max(0, context - identity)
revised_base = base_score - penalty
```

This targets same-household false positives. If two people share an address, context can be high even when identity is low. CODA penalizes that situation.

### 9.5 True Match Versus Same-Household Pair

```text
True match:
R1 = TOMMY ALAN NOEL 754 EMPIRE AVE    VENTURA CA 93003
R2 = TOMMY      NOEL 754 EMPIRE AVENUE VENTYRA CA 93003

Same-household nonmatch:
R1 = TOMMY ALAN NOEL 754 EMPIRE AVE    VENTURA CA 93003
R3 = JAMES LEE  CHEN 754 EMPIRE AVENUE VENTURA CA 93003
```

The true match has:

- strong identity agreement
- strong numeric agreement
- address spelling/abbreviation noise that is recoverable
- low contradiction

The same-household pair has:

- strong context agreement
- strong numeric/address agreement
- weak identity agreement
- high contradiction

A flat comparator may score both pairs similarly because both share address tokens. CODA separates them because it understands that shared address is not enough when identity conflicts.

### 9.6 Code Location

The main evidence logic is in `_soft_role_evidence` in `DWM67_Tahacomparator.py`. Pair-level orchestration is handled by `compare_pair`.

## 10. Mechanism 3: Ephemeral Provisional Context

### 10.1 Motivation

Some pairs land just below `mu`. They may be real matches with small spelling errors, missing middle names, abbreviations, or OCR noise. Pairwise evidence alone may not be enough.

The earlier comparator could send some of these pairs to LLM review. CODA instead uses local graph evidence from the current DWM iteration, while still allowing optional LLM review if configured.

### 10.2 Two-Pass Comparator

CODA uses two passes:

```text
Pass 1:
    compare every candidate pair
    compute evidence and base score
    apply deterministic guard rules
    assign edge type
    keep strong edges for provisional context
    hold ambiguous review-band pairs

Pass 2:
    build provisional components from strong edges
    compute local support/conflict for review-band pairs
    adjust the score
    compare revised score to mu
```

This happens inside `DWM55_LinkBlockPairs.py` and `DWM67_Tahacomparator.py`.

### 10.3 Edge Types

Each pair receives an interpretable edge type:

| Edge Type | Meaning |
|-----------|---------|
| `must_link` | very strong match evidence |
| `likely_link` | strong enough to help provisional context |
| `review_band` | ambiguous pair that may need context |
| `context_only_overlap` | context is stronger than identity, risky for false positives |
| `likely_nonmatch` | weak evidence |
| `cannot_link` | strong contradiction or very weak match evidence |

Only strong edges are allowed to form provisional components. Rejected or risky context-only pairs do not become graph support.

### 10.4 Provisional Component Logic

CODA builds a temporary union-find graph:

```text
for decision in pass_1_decisions:
    if decision.edge_type in {must_link, strong likely_link}:
        union(record_a, record_b)

for component in union_find_components:
    build a component profile from member token roles
```

For a review-band pair, CODA checks whether local structure supports the link:

- do the records connect to the same provisional component?
- do their tokens align with the other component's profile?
- do they share reliable provisional neighbors?
- does local evidence suggest conflict?

### 10.5 Context Adjustment

Context adjusts the score:

```text
context_adjustment = (local_support - local_conflict) * abs(cluster_impact)
revised_score = base_score + context_adjustment
```

Then DWM still decides:

```text
accept if revised_score >= mu and identity >= context
```

This prevents local context from becoming a separate matching rule.

### 10.6 What Context Can And Cannot Do

Context can:

- rescue a borderline true match when local evidence supports it
- lower confidence when local evidence conflicts
- help with missing middle names, abbreviations, and minor spelling noise

Context cannot:

- override `mu`
- create a permanent graph across DWM iterations
- turn address-only agreement into a match when identity is weaker than context
- replace identity evidence with neighborhood evidence

This is the main safety property of CODA's context mechanism.

## 11. Deterministic Guard Rules

CODA keeps deterministic guard rules from the Taha comparator because some cases should not rely on the softer evidence score.

| Rule | Purpose |
|------|---------|
| poison address reject | reject high-address-similarity pairs when name evidence is too weak |
| core name conflict reject | reject pairs with strong first/last-name conflict |
| strong name accept | accept very strong identity matches when other evidence is sufficient |

These rules handle clear cases before provisional context. They are guardrails, not the main novelty. CODA's main contribution is replacing fixed internal comparator constants with adaptive role inference, self-weighted evidence, and temporary context.

## 12. Score Bands And Decision Outcomes

Before provisional context is applied, each candidate pair receives an initial decision. This stage uses the raw token similarity, the DWM `mu` threshold, deterministic guard rules, and CODA evidence values.

The main bands are:

| Band | Meaning | Typical Outcome |
|------|---------|-----------------|
| below reject cutoff | pair is too weak for normal review | reject, unless identity evidence clearly deserves review |
| review band | pair is ambiguous | hold for provisional context and optional review |
| above auto-accept region | pair is strong | accept unless the evidence is context-only |

The review-band design is important because CODA does not want every uncertain pair to become a graph edge. Ambiguous pairs are delayed until the provisional context pass has enough information from strong first-pass edges.

### Identity Bridge

Some pairs fall below the usual review band but still have stronger identity evidence than the raw token similarity suggests. CODA can mark these as review candidates instead of rejecting them immediately. This is useful when:

- names match strongly but some address tokens are missing
- the token list is short
- spelling or abbreviation differences reduce raw similarity
- identity evidence dominates context evidence

### Context-Only Review

Some pairs score high because the address is very similar, but the identity evidence is weak. These are dangerous because they often represent roommates, family members, or unrelated people at the same address. CODA marks these as context-only overlap cases and avoids treating them as confident matches.

The policy is conservative:

```text
high score + identity-driven evidence -> candidate for accept
high score + context-driven evidence  -> review or reject
```

This is one of the main differences between CODA and a flat similarity comparator.

## 13. Complete Decision Flow

For each candidate pair:

1. read flattened token lists
2. compute ScoringMatrix-style token similarity
3. split records into likely name and address regions
4. compute name and address similarity details
5. infer soft token roles from dataset frequency statistics
6. compute identity, context, numeric, similarity, and contradiction evidence
7. compute self-weighted base score
8. apply deterministic guard rules
9. assign edge type
10. build provisional components from strong pass-1 edges
11. adjust review-band pair scores using local context
12. accept only if revised score satisfies `mu` and identity is not dominated by context
13. pass accepted edges to transitive closure

The key invariant is:

```text
CODA can change the score, but DWM's mu threshold makes the final pair-link decision.
```

## 14. How The Three Mechanisms Work Together

| Problem | CODA Mechanism | Effect |
|---------|----------------|--------|
| tokens lack explicit meaning | statistical role inference | gives each token an evidence profile |
| evidence differs by pair | self-weighted scoring | lets strong channels dominate weak channels |
| borderline pairs need more signal | provisional context | adds local support/conflict without overriding `mu` |

The mechanisms are sequential:

1. role inference defines what evidence each token can provide
2. evidence scoring uses those roles to compute pair-level signals
3. provisional context uses first-pass pair decisions to refine only ambiguous pairs

This makes the comparator explainable. A decision can be traced through token roles, evidence channels, edge type, context adjustment, and final threshold comparison.

## 15. Implementation Map

| Component | File | Key Functions |
|-----------|------|---------------|
| dataset statistics | `DWM67_Tahacomparator.py` | `_compute_dataset_stats` |
| soft role inference | `DWM67_Tahacomparator.py` | `_infer_soft_token_roles` |
| evidence scoring | `DWM67_Tahacomparator.py` | `_soft_role_evidence` |
| edge typing | `DWM67_Tahacomparator.py` | `_edge_type_from_scores` |
| deterministic guard rules | `DWM67_Tahacomparator.py` | `_deterministic_rule_decision` |
| provisional graph | `DWM67_Tahacomparator.py` | `_build_provisional_components` |
| context decision | `DWM67_Tahacomparator.py` | `_finalize_context_decision`, `apply_provisional_context_pass` |
| pair entry point | `DWM67_Tahacomparator.py` | `compare_pair` |
| two-pass orchestration | `DWM55_LinkBlockPairs.py` | `linkBlockPairs` |
| benchmark runner | `DWM_Comparator_Benchmark.py` | single-dataset benchmark |
| multi-dataset runner | `DWM_AllDatasets_Benchmark.py` | 22-dataset benchmark |

## 16. Parameterization And Runtime Behavior

CODA still runs inside DWM, so there are two categories of parameters.

The first category is normal DWM pipeline control:

- `mu`: pair-link threshold
- `epsilon`: cluster-acceptance threshold
- `beta`, `sigma`: blocking and stop-word behavior
- iteration deltas for raising thresholds across peel-off passes

The second category controls CODA behavior:

- `tahaUseSoftRoleScoring`: enables statistical token roles and self-weighted evidence
- `tahaUseProvisionalContext`: enables the second-pass local graph adjustment
- `tahaUseOpenAIReview`: optional review for unresolved ambiguous pairs
- edge thresholds such as `tahaMustLinkThreshold` and `tahaLikelyLinkThreshold`
- guard-rule thresholds for poison-address and core-name conflict cases

The important claim is not that CODA removes every parameter from the system. DWM still has dataset-level pipeline parameters. The claim is narrower and more precise: CODA removes the need for dataset-specific comparator-internal constants such as fixed role weights, fixed rarity thresholds, and fixed evidence vectors.

Runtime flow:

1. `DWM55_LinkBlockPairs.py` calls `start_run(tokenFreqDict)` so CODA can compute dataset statistics once for the current run.
2. Each candidate pair is processed by `compare_pair`.
3. `compare_pair` computes token similarity, name/address details, evidence channels, edge type, and initial decision.
4. After all pairs are scored, `apply_provisional_context_pass` builds the temporary graph and revises review-band decisions.
5. Accepted edges return to DWM for transitive closure.

## 17. Evaluation Protocol

The reported benchmark compares CODA to the classic DWM comparator set across 22 datasets:

- `S1` through `S18`
- `S12PX_R1` through `S12PX_R6`

All benchmarked CODA runs use:

- embedding-based KNN blocking
- `topK = 10`
- no OpenAI/LLM review
- same DWM pipeline structure
- same truth-based ER metrics

The main metrics are:

| Metric | Definition |
|--------|------------|
| precision | fraction of linked pairs that are true matches |
| recall | fraction of true matching pairs recovered |
| F1 | harmonic mean of precision and recall |

Blocking affects the maximum possible recall because the comparator can only link candidate pairs it receives. CODA's contribution is therefore measured under the same candidate-generation regime as the baselines.

## 18. Benchmark Results And Interpretation

| Result | Value |
|--------|------:|
| CODA average precision | 0.932 |
| CODA average recall | 0.845 |
| CODA average F1 | 0.881 |
| next-best classic comparator average F1 (`ScoringMatrixKris`) | 0.842 |
| CODA F1 wins | 19 / 22 datasets |
| LLM calls in reported CODA benchmark | 0 |
| dataset-specific comparator constants | 0 |

The result should be read carefully:

- CODA improves the balance of precision and recall across datasets.
- CODA is not simply trading precision for recall; its role/evidence/context structure helps with both in many cases.
- The benchmark is whole-system, so blocking still sets a ceiling on recall.
- No LLM review was used in the reported CODA configuration.
- DWM parameters still exist outside the comparator. The claim is not that DWM needs no dataset-level parameters. The claim is that CODA removes dataset-specific comparator-internal constants.

## 19. Threats To Validity

Important limitations:

- The datasets are from the available DWM benchmark family; broader external datasets should be tested.
- The current results are whole-system results, not isolated comparator-only measurements.
- Embedding-based blocking influences recall and may hide comparator behavior on missed candidate pairs.
- Some datasets are very small or nearly solved, making F1 differences less informative.
- The current reported CODA benchmark disables LLM review; optional LLM review may change recall/precision behavior.

These limitations do not invalidate the result, but they define the next experiments needed for a paper.

## 20. Suggested Ablation Study

To isolate CODA's mechanisms, run:

| Variant | Soft Roles | Provisional Context | Purpose |
|---------|------------|---------------------|---------|
| baseline Taha | off | off | original comparator behavior |
| soft-only | on | off | measures token-role and evidence-scoring effect |
| context-only | off | on | measures provisional graph effect alone |
| CODA full | on | on | measures combined system |

The benchmark runner already supports variants such as `taha-soft-only`, `taha-context-only`, and `taha-cx`.

## 21. Reproduction

Run CODA on S12 from `DWM_colab_bundle/`:

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

Use `--force-embedding-device cuda` for Colab or GPU runs.

## 22. Paper Outline From This Work

A paper can be structured as:

1. **Introduction**: entity resolution in DWM, challenge of comparator generalization, CODA overview.
2. **Background**: DWM peel-off pipeline, candidate generation, classic comparators.
3. **Problem Definition**: noisy PII-style records, flattened tokens, same-household false positives, review-band ambiguity.
4. **Method**: role inference, self-weighted evidence, provisional context, deterministic guardrails.
5. **Implementation**: two-pass flow, runtime parameters, decision traces, and computational behavior.
6. **Experimental Setup**: datasets, baselines, metrics, blocking configuration, no-LLM condition.
7. **Results**: aggregate results, per-dataset wins, precision/recall interpretation.
8. **Ablation**: soft-only, context-only, full CODA.
9. **Discussion**: why the mechanisms help, where CODA fails, relationship to DWM semantics.
10. **Limitations And Future Work**: more datasets, LLM review recovery, learned role models, alternative blocking.
11. **Conclusion**: CODA as a data-adaptive comparator inside DWM.

## 23. Current Status

Completed:

- data-adaptive token role inference
- self-weighted evidence scoring
- provisional context pass
- deterministic guardrails
- 22-dataset benchmark

Recommended next work:

- run and document ablation results
- add per-dataset result tables to this document or a separate results appendix
- test CODA with optional LLM review enabled for review-band recall recovery
- evaluate on additional external ER datasets
