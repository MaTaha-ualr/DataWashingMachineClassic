# CODA / TahaComparator-CX Technical Documentation

CODA means **Context-Driven Adaptive Comparator**. In this repository, CODA is implemented as the CX configuration of `TahaComparator`.

This document explains the mechanism in enough detail to support a paper draft. It focuses on the comparator design, not repository housekeeping.

## Abstract

Entity resolution must decide whether two noisy records refer to the same real-world entity. In the Data Washing Machine (DWM), that decision happens inside an iterative peel-off pipeline: candidate pairs are generated, accepted edges are linked, connected components are formed, high-quality clusters are accepted, and accepted records are removed before the next iteration. This creates an important constraint. A comparator can use local context from the current unresolved pool, but it cannot rely on a permanent global graph because the active record pool changes after every peel-off step.

CODA addresses this setting with three mechanisms. First, it infers soft token roles from the dataset's own token-frequency distribution, so tokens can behave as identity, location, numeric, or volatile evidence without requiring schema labels. Second, it converts those roles into pair-level identity, context, numeric, similarity, and contradiction evidence, then combines the positive channels with self-weighted scoring. Third, it builds an ephemeral provisional graph from confident first-pass edges and uses that graph only to revise ambiguous pairs before the normal DWM `mu` threshold makes the final link decision.

Across 22 benchmark datasets, CODA achieved average precision `0.932`, average recall `0.845`, and average F1 `0.881`. It won 19 of 22 F1 comparisons against the classic DWM comparator set in the reported no-LLM configuration.

## 1. Core Claim

CODA is not a new end-to-end entity-resolution system. It is a comparator-level method for DWM.

The central claim is:

```text
CODA improves DWM pair decisions by making token meaning, evidence weighting,
and local context data-adaptive while preserving DWM's mu threshold,
epsilon threshold, transitive closure, and peel-off iteration semantics.
```

The claim is intentionally narrow. DWM still has pipeline parameters such as `mu`, `epsilon`, blocking settings, and iteration controls. CODA's contribution is that the comparator does not need dataset-specific internal constants for deciding which tokens behave like identity evidence, which tokens behave like context, and how ambiguous pairs should be revised by temporary local structure.

## 2. Terminology

| Term | Meaning |
|------|---------|
| record | one input reference to a possible real-world entity |
| token | normalized unit extracted from a record, such as a name, address word, number, ZIP code, or state |
| candidate pair | two records selected by blocking for detailed comparison |
| comparator | module that scores a candidate pair and decides whether it should become an edge |
| edge | accepted link between two records |
| cluster | connected component produced by transitive closure over accepted edges |
| unresolved pool | records that have not yet been accepted into final clusters |
| peel-off iteration | one DWM cycle that accepts clusters and removes them from the unresolved pool |
| `mu` | DWM pair-link threshold |
| `epsilon` | DWM cluster-acceptance threshold |
| review band | score region where a pair is neither an obvious accept nor an obvious reject |
| CODA | Context-Driven Adaptive Comparator, implemented as `TahaComparator-CX` |

## 3. Problem Setting

The comparator receives two flattened token lists and must decide whether they identify the same entity.

```text
True match:
R1 = TOMMY ALAN NOEL 754 EMPIRE AVE    VENTURA CA 93003
R2 = TOMMY      NOEL 754 EMPIRE AVENUE VENTYRA CA 93003

Same-household nonmatch:
R1 = TOMMY ALAN NOEL 754 EMPIRE AVE    VENTURA CA 93003
R3 = JAMES LEE  CHEN 754 EMPIRE AVENUE VENTURA CA 93003
```

Both pairs share address evidence. A flat token comparator can overvalue the shared address and undervalue the name disagreement. CODA separates these cases by asking four questions:

1. What role does each token appear to play in this dataset?
2. How much identity, context, numeric, and general similarity evidence does this pair have?
3. Is there active contradiction, especially identity contradiction hidden by address agreement?
4. If the pair is ambiguous, does local provisional context support or conflict with the link?

## 4. Design Goals

| Goal | CODA Design Response |
|------|----------------------|
| preserve DWM behavior | keep `mu`, `epsilon`, transitive closure, and peel-off unchanged |
| reduce false positives from shared context | separate identity evidence from address/location evidence |
| recover borderline true matches | use provisional local context only for ambiguous pairs |
| avoid schema dependence | infer token roles from frequency, position, shape, and rarity |
| avoid dataset-specific comparator constants | derive token role behavior from the active token-frequency distribution |
| remain auditable | store evidence scores, edge type, decision band, context adjustment, and final reason |
| benchmark without LLM dependence | disable OpenAI review in the reported CODA benchmark |

## 5. DWM Pipeline Context

DWM is iterative:

1. select unresolved records
2. tokenize records and compute token frequencies
3. apply optional global token correction
4. generate candidate pairs through blocking
5. compare candidate pairs
6. link accepted pairs
7. compute transitive closure
8. accept clusters whose quality is at least `epsilon`
9. remove accepted clusters before the next iteration

CODA sits at step 5. Blocking determines which pairs CODA can see; CODA determines which candidate pairs become accepted edges.

| Stage | Module | CODA-Relevant Role |
|-------|--------|--------------------|
| Tokenization | `DWM14_BuildRefDict.py` | creates token lists |
| Token frequencies | `DWM16_BuildTokenFreqDict.py` | supplies frequency statistics for role inference |
| Blocking | `DWM42_BuildBlockPairs.py` | sets the candidate-pair recall ceiling |
| Pair linking | `DWM55_LinkBlockPairs.py` | calls `compare_pair`, then applies provisional context |
| Comparator | `DWM67_Tahacomparator.py` | implements CODA mechanisms |
| Transitive closure | `DWM80_TransitiveClosure.py` | turns accepted edges into connected components |
| Cluster acceptance | `DWM90_IterateClusters.py` | accepts clusters above `epsilon` |
| Metrics | `DWM99_ERmetrics.py` | computes precision, recall, and F-measure |

### Why Temporary Context Is Necessary

In a one-shot graph algorithm, it may be natural to build a global graph and reason over it. DWM is different. After each iteration, accepted records are removed from the unresolved pool. A global context graph from one iteration can become stale in the next.

CODA therefore uses **ephemeral context**:

- build provisional components only from the current iteration's confident edges
- use those components only to refine ambiguous pairs
- discard the provisional graph after the iteration
- let DWM's normal transitive closure and cluster acceptance make the final cluster-level decisions

This is the key compatibility point between CODA and DWM.

## 6. Record Representation

The benchmark records contain PII-like fields: names, addresses, and sometimes SSN or date of birth. The comparator sees flattened token lists:

```text
A965806: [TOMMY, ALAN, NOEL, 754, EMPIRE, AVE, VENTURA, CA, 93003]
         |---- name ----|    |--------- address ---------|
```

The field boundary is not explicitly stored in the comparator input. CODA reconstructs evidence roles from observable signals:

- token position
- first digit position
- token frequency
- token length
- alphabetic, numeric, or mixed shape
- token rarity in the active dataset
- name and address similarity details computed by the Taha comparator foundation

CODA is therefore schema-light. It benefits from the common ordering of name-like tokens before address-like tokens, but it does not require formal field labels for every token.

## 7. Baseline Comparator Limitation

Classic DWM comparators produce a score in `[0, 1]`.

| Comparator | General Behavior |
|------------|------------------|
| Cosine | bag-of-words overlap over token lists |
| MongeElkan | best token alignment from one record to another |
| ScoringMatrixStd | edit-distance matrix with greedy token matching |
| ScoringMatrixKris | scoring matrix with additional positional behavior |
| TahaComparator baseline | name/address decomposition, deterministic rules, optional LLM review |

These methods are useful, but difficult cases require more than one flat score:

- common tokens can be overvalued
- address agreement can hide name disagreement
- a fixed name/address weighting is fragile across datasets
- local context can help ambiguous pairs, but DWM cannot use a permanent global graph

CODA keeps the useful Taha comparator foundation and adds adaptive role inference, adaptive evidence scoring, and temporary context.

## 8. CODA Architecture

CODA has three main mechanisms:

1. **Statistical token role inference**
   Each token receives soft weights over identity, location, numeric, and volatile roles.

2. **Self-weighted evidence scoring**
   Pair-level identity, context, numeric, and similarity channels are combined according to their own strength, with a contradiction penalty when context tries to overpower identity.

3. **Ephemeral provisional context**
   Strong first-pass edges form temporary components. Ambiguous pairs are revised using local support and conflict, then compared to `mu`.

The high-level flow is:

```text
candidate pair
  -> token similarity and name/address details
  -> soft token role inference
  -> identity/context/numeric/contradiction evidence
  -> self-weighted base edge score
  -> deterministic guard rules and initial decision band
  -> provisional component construction from confident edges
  -> context adjustment for ambiguous pairs only
  -> final decision by revised score versus mu
```

The invariant is:

```text
context can revise the score; mu still decides the pair link.
```

## 9. Mechanism 1: Statistical Token Role Inference

### 9.1 Motivation

Tokens do not all carry the same evidence. Sharing `NOEL` is stronger identity evidence than sharing `CA`. Sharing `754` is useful numeric/address evidence, but it is not the same as sharing a last name. CODA needs this distinction before it can reason about a pair.

Instead of using a hand-written dictionary, CODA infers soft roles from the active dataset.

### 9.2 Role Types

Each token receives normalized weights over four roles.

| Role | Meaning |
|------|---------|
| identity-like | name-like or rare entity-identifying evidence |
| location-like | address, street, city, state, or place-like evidence |
| numeric-like | digit-bearing values such as street numbers, ZIP codes, SSN-like values, unit numbers |
| volatile/noise | short, common, suffix-like, weak, or unstable evidence |

The roles are soft. A token can be partly location-like and partly volatile. A rare street token can be mostly location-like while still having some identity value because it helps discriminate records.

### 9.3 Dataset Statistics

At the start of a TahaComparator run, `start_run(tokenFreqDict)` computes dataset-level frequency statistics:

- frequency percentiles: p25, p50, p75, p90, p95
- maximum frequency
- log maximum frequency
- total token count
- unique token count
- average frequency

The main rarity signal is:

```text
rarity(t) = 1 - log(1 + freq(t)) / log(1 + max_freq)
```

High rarity means the token is uncommon in the current dataset. Low rarity means the token is common. This makes role inference relative to the current dataset instead of tied to one fixed benchmark.

### 9.4 Signals Used For Roles

| Signal | Role Effect |
|--------|-------------|
| before first digit | increases identity-like weight |
| after first digit | increases location-like weight |
| contains digit | increases numeric-like weight and some location support |
| alphabetic before first digit | increases identity-like weight |
| alphabetic after first digit | increases location-like weight |
| high rarity | increases identity-like evidence |
| high frequency | increases volatile/noise evidence |
| short token | increases volatile/noise evidence |
| mixed alphanumeric token | increases numeric/location behavior and volatility |
| name suffix token | increases volatility and reduces identity weight |

The implementation normalizes the accumulated role weights so each token has an interpretable role distribution.

### 9.5 Role-Inference Pseudocode

```text
for token in normalized_record_tokens:
    rarity = rarity_from_dataset_frequency(token)
    before_digit = token_index < first_digit_index(record)
    has_digit = token_contains_digit(token)
    alpha = token_is_alphabetic(token)
    short = token_length <= 2

    identity = 0
    context = 0
    numeric = 0
    volatile = 0

    if before_digit:
        identity += position_weighted_identity_signal
    else:
        context += address_region_signal

    if has_digit:
        numeric += numeric_signal
        context += numeric_location_support
    else if alpha and before_digit:
        identity += alphabetic_name_region_signal
    else if alpha:
        context += alphabetic_address_region_signal

    if alpha:
        identity += rarity_signal
        context += commonness_as_context_signal

    if token_is_common_or_short_or_suffix_like:
        volatile += weak_token_signal

    normalize(identity, context, numeric, volatile)
```

Code location: `_compute_dataset_stats`, `_infer_soft_token_roles`, `_role_weight_map`, and `_weighted_overlap_from_maps` in `DWM67_Tahacomparator.py`.

### 9.6 Example

For:

```text
[TOMMY, ALAN, NOEL, 754, EMPIRE, AVE, VENTURA, CA, 93003]
```

CODA should tend toward:

| Token | Expected Role Behavior |
|-------|------------------------|
| `TOMMY`, `ALAN`, `NOEL` | mostly identity-like |
| `754`, `93003` | mostly numeric-like with location support |
| `EMPIRE`, `VENTURA` | mostly location-like, with more value if rare |
| `AVE`, `CA` | location-like but also volatile/common |

The important point is not that CODA knows a state dictionary or street-suffix dictionary. It is that frequency, position, and token shape make these behaviors emerge.

## 10. Mechanism 2: Self-Weighted Evidence Scoring

### 10.1 Motivation

A fixed evidence formula such as "60 percent name, 30 percent address, 10 percent numeric" assumes all ambiguous pairs have the same evidence pattern. They do not.

Some pairs have strong names and noisy addresses. Some have identical addresses and different people. Some have strong numeric agreement but weak identity evidence. CODA therefore computes evidence channels first, then lets their strengths determine the combined score.

### 10.2 Weighted Token Overlap

For a role-specific token map, CODA computes overlap with a weighted Jaccard-style formula:

```text
overlap(role) = sum_t min(weight_1(t), weight_2(t))
                / sum_t max(weight_1(t), weight_2(t))
```

This is computed separately for identity-like, location-like, and numeric-like roles.

### 10.3 Evidence Channels

| Channel | What It Measures |
|---------|------------------|
| identity evidence | structured name similarity blended with identity-role overlap and rare shared identity tokens |
| context evidence | structured address similarity blended with location and numeric overlap |
| numeric evidence | overlap among digit-bearing role weights |
| similarity evidence | baseline ScoringMatrix-style token similarity |
| contradiction | name disagreement, amplified when context is stronger than identity |

### 10.4 Identity Evidence

The implementation first computes structured identity from name-level details:

```text
structured_identity = mean(name_similarity,
                           last_name_similarity,
                           name_positional_similarity)
```

Then it blends structured identity with identity-role overlap and a rare-identity bonus:

```text
identity_evidence =
    mean(structured_identity, identity_overlap, rare_identity_bonus)
```

If there is no role overlap and no rare-identity bonus, CODA falls back to structured identity.

### 10.5 Context Evidence

The implementation computes structured context from address similarity and address-number similarity:

```text
structured_context = mean(address_similarity,
                          address_number_similarity)
```

Then it blends structured context with location-role and numeric-role overlap:

```text
context_evidence =
    mean(structured_context, context_overlap, numeric_overlap)
```

If there is no context or numeric overlap, CODA falls back to structured context.

### 10.6 Contradiction

Contradiction is mainly identity disagreement. It is especially dangerous when address/context evidence is high.

```text
name_conflict = mean(1 - last_name_similarity,
                     1 - first_name_similarity)

context_identity_gap = max(0, context_evidence - identity_evidence)

contradiction =
    name_conflict * (0.5 + 0.5 * context_identity_gap)
```

This is how CODA targets same-household false positives. If address agreement is high but first/last names disagree, the pair should not be treated as a confident match.

### 10.7 Self-Weighted Base Edge Score

CODA combines positive evidence channels with weights proportional to their own values:

```text
total = identity + context + numeric + similarity

base_score =
    (identity^2 + context^2 + numeric^2 + similarity^2) / total
    - 0.5 * contradiction * max(0, context - identity)
```

This formula has two useful properties:

- strong evidence contributes more than weak evidence
- context-driven matches are penalized when identity evidence is weak and contradiction is high

If all channels are weak, the score remains weak. If several channels agree, the score rises. If address evidence is strong but identity conflicts, the contradiction penalty pushes the score down.

### 10.8 Example Interpretation

For the true match:

```text
TOMMY ALAN NOEL 754 EMPIRE AVE VENTURA CA 93003
TOMMY      NOEL 754 EMPIRE AVENUE VENTYRA CA 93003
```

CODA should see:

- strong identity evidence from `TOMMY` and `NOEL`
- strong numeric evidence from `754` and `93003`
- usable context evidence despite spelling and abbreviation noise
- low identity contradiction

For the same-household nonmatch:

```text
TOMMY ALAN NOEL 754 EMPIRE AVE VENTURA CA 93003
JAMES LEE  CHEN 754 EMPIRE AVENUE VENTURA CA 93003
```

CODA should see:

- strong address/context evidence
- strong numeric evidence
- weak identity evidence
- high identity contradiction

This separation is the main reason CODA is more robust than a flat token overlap score.

Code location: `_soft_role_evidence` in `DWM67_Tahacomparator.py`.

## 11. Mechanism 3: Ephemeral Provisional Context

### 11.1 Motivation

Some true matches land near the decision boundary because of missing tokens, spelling noise, abbreviation, or uneven tokenization. Pairwise evidence alone may be inconclusive.

CODA uses local context, but only in a restricted way:

- it builds context from confident first-pass edges
- it applies context only to ambiguous pairs
- it discards context after the current DWM iteration
- it still requires the revised score to satisfy `mu`

### 11.2 Two-Pass Flow

`DWM55_LinkBlockPairs.py` calls `compare_pair` for every TahaComparator candidate pair. These first-pass decisions are stored in `tahaDecisionList`. After all pairs are scored, `DWM55_LinkBlockPairs.py` calls:

```python
Class.apply_provisional_context_pass(tahaDecisionList, refDict)
```

The two-pass flow is:

```text
Pass 1:
    score every candidate pair
    compute name/address details
    compute CODA evidence
    apply deterministic guard rules
    assign initial decision and edge type

Pass 2:
    build provisional components from confident edges
    compute local support and local conflict for each decision row
    revise only ambiguous review-band decisions
    compare revised score to mu
```

### 11.3 Initial Decisions Versus Edge Types

The implementation separates **initial decision bands** from **edge types**.

Initial decisions include:

| Initial Decision | Meaning |
|------------------|---------|
| `accept` | first-pass score/rule is strong enough to accept |
| `reject` | pair is below cutoff or rejected by a rule |
| `llm_review` | pair is ambiguous and should be deferred for context or optional review |
| `pending_llm` | optional review is enabled but not yet resolved |

Edge types include:

| Edge Type | Meaning |
|-----------|---------|
| `must_link` | very strong evidence with low contradiction |
| `likely_link` | strong evidence that may support provisional context |
| `context_only_overlap` | score is driven by context more than identity |
| `likely_nonmatch` | weak or insufficient evidence |
| `cannot_link` | very weak score or strong contradiction |

This distinction matters. The review band is a routing decision; it is not itself an edge type.

### 11.4 Provisional Component Construction

CODA builds a temporary union-find graph from confident first-pass edges.

An edge can support the provisional graph if it is:

- `must_link`, or
- a strong `likely_link` with enough score, identity support, and low contradiction, or
- above the must-link threshold with low contradiction

Risky context-only edges and rejected pairs do not become provisional graph support.

The graph is used to build component profiles:

```text
component profile =
    stable identity tokens
    context tokens
    numeric tokens
    component size
```

Stable identity tokens are not just shared tokens. They must appear consistently enough inside the component and carry enough identity-like role weight.

### 11.5 Local Support And Local Conflict

For a review-band pair, CODA compares each record against the other record's provisional component profile.

It computes:

| Signal | Meaning |
|--------|---------|
| profile alignment | how well a record fits the other side's component profile |
| shared neighbor ratio | whether both records connect to similar provisional neighbors |
| local support | blended profile alignment and shared-neighbor support |
| local conflict | missing identity alignment or context-dominated disagreement |
| cluster impact | `local_support - local_conflict`, clamped to `[-1, 1]` |

Identity support has the largest weight in profile alignment. Context and numeric support help, but they cannot replace identity alignment.

### 11.6 Context Adjustment

For ambiguous review-band pairs, CODA revises the score:

```text
context_adjustment =
    (local_support - local_conflict) * abs(cluster_impact)

revised_score =
    clamp(base_edge_score + context_adjustment)
```

Then the final decision is made:

```text
if revised_score >= mu and identity_evidence > context_evidence:
    accept
else:
    reject or optional review
```

If OpenAI review is disabled, context-driven pairs above `mu` are rejected when identity does not dominate context. This preserves CODA's safety rule: address/context cannot become a match by itself.

Code location: `_build_provisional_components`, `_component_context_summary`, `_finalize_context_decision`, and `apply_provisional_context_pass` in `DWM67_Tahacomparator.py`.

## 12. Deterministic Guard Rules

CODA keeps deterministic guard rules because some cases should not depend on a soft score.

| Rule | Purpose |
|------|---------|
| poison address reject | reject high-address-similarity pairs when name evidence is too weak |
| core name conflict reject | reject pairs with strong first/last-name conflict |
| strong name accept | accept very strong identity matches when the total similarity is not too low |

These rules are guardrails. The CODA contribution is the adaptive evidence system around them.

## 13. Complete Decision Flow

For each candidate pair:

1. remove stop words according to DWM blocking/comparison settings
2. compute ScoringMatrix-style token similarity
3. compute name similarity details
4. compute address similarity details
5. infer soft token roles from dataset statistics
6. compute identity, context, numeric, similarity, and contradiction evidence
7. compute the self-weighted base edge score
8. apply deterministic guard rules
9. assign first-pass initial decision and edge type
10. build provisional components from confident first-pass edges
11. compute local support, conflict, shared-neighbor ratio, and cluster impact
12. revise review-band pair scores only
13. accept only when the final score satisfies `mu` and identity evidence dominates context evidence
14. pass accepted edges back to DWM for transitive closure

## 14. Decision Trace Fields

CODA is designed to be inspectable. Each decision row can store:

| Field | Meaning |
|-------|---------|
| `similarity` | baseline token similarity |
| `name_similarity` | structured name similarity |
| `first_name_similarity` | first-name similarity |
| `last_name_similarity` | last-name similarity |
| `name_positional_similarity` | positional name agreement |
| `address_similarity` | structured address similarity |
| `address_number_similarity` | numeric address agreement |
| `identity_evidence_score` | CODA identity channel |
| `context_evidence_score` | CODA context channel |
| `contradiction_score` | identity conflict, amplified by context dominance |
| `base_edge_score` | self-weighted score before provisional context |
| `final_edge_score` | score after context pass |
| `local_support_score` | provisional context support |
| `local_conflict_score` | provisional context conflict |
| `cluster_impact` | net local effect |
| `shared_neighbor_ratio` | overlap of provisional neighbors |
| `edge_type` | interpretable evidence type |
| `initial_decision` | first-pass routing decision |
| `final_decision` | final accept/reject/pending decision |
| `reason` | final decision reason |

This trace is important for paper analysis because it allows examples to be explained mechanically rather than only by aggregate F1.

## 15. Implementation Map

| Component | File | Key Functions |
|-----------|------|---------------|
| dataset statistics | `DWM67_Tahacomparator.py` | `_compute_dataset_stats`, `start_run` |
| soft role inference | `DWM67_Tahacomparator.py` | `_infer_soft_token_roles`, `_role_weight_map` |
| weighted overlap | `DWM67_Tahacomparator.py` | `_weighted_overlap_from_maps` |
| evidence scoring | `DWM67_Tahacomparator.py` | `_soft_role_evidence` |
| edge typing | `DWM67_Tahacomparator.py` | `_edge_type_from_scores` |
| deterministic guard rules | `DWM67_Tahacomparator.py` | `_deterministic_rule_decision`, `_name_rule_reject` |
| provisional graph | `DWM67_Tahacomparator.py` | `_build_provisional_components` |
| component profile | `DWM67_Tahacomparator.py` | `_build_profile_from_members`, `_profile_alignment` |
| context decision | `DWM67_Tahacomparator.py` | `_component_context_summary`, `_finalize_context_decision`, `apply_provisional_context_pass` |
| pair entry point | `DWM67_Tahacomparator.py` | `compare_pair` |
| two-pass orchestration | `DWM55_LinkBlockPairs.py` | `linkBlockPairs` |
| single-dataset benchmark | `DWM_Comparator_Benchmark.py` | benchmark CLI |
| multi-dataset benchmark | `DWM_AllDatasets_Benchmark.py` | 22-dataset benchmark CLI |

## 16. Parameterization

CODA runs inside DWM, so there are two parameter layers.

### 16.1 DWM Pipeline Parameters

- `mu`: pair-link threshold
- `epsilon`: cluster-acceptance threshold
- `sigma`: stop-word frequency threshold
- blocking controls such as `topK`
- iteration controls that raise thresholds across peel-off passes

These are not removed by CODA.

### 16.2 CODA Comparator Parameters

- `tahaUseSoftRoleScoring`: enables soft token roles and self-weighted evidence
- `tahaUseProvisionalContext`: enables the second-pass local graph adjustment
- `tahaUseOpenAIReview`: enables optional unresolved-pair review
- `tahaMustLinkThreshold`: strong edge threshold
- `tahaLikelyLinkThreshold`: likely edge threshold
- `tahaContextOnlyThreshold`: context-dominance threshold
- `tahaCannotLinkThreshold`: very weak edge threshold
- deterministic-rule thresholds for poison-address, name-conflict, and strong-name cases

The important distinction is that CODA uses these as comparator behavior controls, not dataset-specific hand tuning for token roles.

## 17. Evaluation Protocol

The reported benchmark compares CODA to classic DWM comparators across 22 datasets:

- `S1` through `S18`
- `S12PX_R1` through `S12PX_R6`

The reported CODA benchmark uses:

- embedding-based KNN blocking
- `topK = 10`
- no OpenAI/LLM review
- the same DWM pipeline structure
- truth-based ER metrics

Metrics:

| Metric | Meaning |
|--------|---------|
| precision | fraction of linked pairs that are true matches |
| recall | fraction of true matching pairs recovered |
| F1 | harmonic mean of precision and recall |

Blocking sets the candidate-pair recall ceiling. CODA can only link pairs that blocking sends to the comparator.

## 18. Benchmark Results

| Result | Value |
|--------|------:|
| CODA average precision | `0.932` |
| CODA average recall | `0.845` |
| CODA average F1 | `0.881` |
| next-best classic comparator average F1, `ScoringMatrixKris` | `0.842` |
| CODA F1 wins | `19 / 22` datasets |
| LLM calls in reported CODA benchmark | `0` |

Interpretation:

- CODA improves the precision/recall balance across the benchmark set.
- The improvement is not explained by LLM review, because the reported run disables it.
- The result is a whole-system DWM result, so blocking quality still affects recall.
- The result supports the CODA mechanism, but ablation studies are still needed to isolate each component.

## 19. Suggested Ablation Study

To write a stronger paper, the next experimental step is an ablation table.

| Variant | Soft Roles | Provisional Context | Purpose |
|---------|------------|---------------------|---------|
| baseline Taha | off | off | original comparator behavior |
| soft-only | on | off | isolates role inference and self-weighted evidence |
| context-only | off | on | isolates provisional graph adjustment |
| CODA full | on | on | measures the complete method |

The benchmark runner supports variants such as `taha-soft-only`, `taha-context-only`, and `taha-cx`.

Recommended paper tables:

- aggregate precision, recall, F1 by comparator
- per-dataset F1 wins/losses
- ablation average metrics
- examples of false positives removed by contradiction/context-only logic
- examples of true matches recovered by provisional context

## 20. Threats To Validity

Important limitations:

- The benchmark uses the available DWM dataset family; external ER datasets should be tested.
- Results are whole-system results, not isolated comparator-only scores.
- Embedding-based blocking affects maximum achievable recall.
- Some datasets may be small or nearly solved, making F1 differences less informative.
- Optional LLM review is disabled in the reported benchmark; enabling it may change behavior.
- CODA still assumes token order carries some information, especially that name-like tokens often appear before address-like tokens.

These limitations do not invalidate the result. They define what should be tested next.

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

## 22. Paper Structure From This Documentation

A paper can be organized as:

1. **Introduction**
   Explain DWM, the comparator generalization problem, and CODA's three mechanisms.

2. **Background**
   Describe entity resolution, DWM peel-off iteration, candidate generation, and classic comparators.

3. **Problem Definition**
   Define flattened token comparison, same-household false positives, context ambiguity, and review-band decisions.

4. **Method**
   Present role inference, evidence scoring, contradiction, provisional context, and guard rules.

5. **Implementation**
   Explain the two-pass comparator flow, decision traces, runtime parameters, and interaction with DWM.

6. **Experimental Setup**
   List datasets, baselines, metrics, blocking configuration, and the no-LLM condition.

7. **Results**
   Present aggregate metrics, per-dataset wins, and interpretation.

8. **Ablation**
   Compare baseline, soft-only, context-only, and full CODA.

9. **Discussion**
   Explain why CODA helps, where it fails, and how temporary context preserves DWM semantics.

10. **Limitations And Future Work**
    Add external datasets, more ablation, optional review experiments, and alternative blocking.

11. **Conclusion**
    Summarize CODA as a data-adaptive comparator for DWM.

## 23. Summary

CODA improves the TahaComparator by making three decisions adaptive:

- token meaning is inferred from dataset statistics
- evidence channels are weighted by their own strength
- ambiguous pairs are revised using temporary local context

The method is conservative. It does not let address context override identity, it does not replace `mu`, and it does not build a permanent graph outside DWM's peel-off process. That combination is the main technical contribution.
