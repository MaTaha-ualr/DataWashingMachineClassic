#!/usr/bin/env python
# coding: utf-8

"""
DWM_DataCapture.py - Utility module for capturing intermediate data after each DWM process step.

This module provides functions to save data structures to CSV files,
allowing users to understand how data transforms through the pipeline.
"""

import os
import csv
import json
import re
import DWM10_Parms
from textdistance import Cosine, MongeElkan
import DWM65_ScoringMatrixStd
import DWM66_ScoringMatrixKris



def load_truth_dict(truthFileName):
    """
    Load truth dictionary from truth file.

    Args:
        truthFileName: Path to the truth file (CSV with RecID,idtruth)

    Returns:
        Dictionary where key=refID, value=truthID
    """
    if not truthFileName or truthFileName == '':
        return {}

    truthDict = {}
    try:
        with open(truthFileName, 'r') as truthFile:
            line = truthFile.readline().strip()  # Skip header
            line = truthFile.readline().strip()
            while line != '':
                part = line.split(',')
                recID = part[0].strip()
                truthID = part[1].strip()
                truthDict[recID] = truthID
                line = truthFile.readline().strip()
    except FileNotFoundError:
        print(f'  Warning: Truth file not found: {truthFileName}')
        return {}

    return truthDict


def create_capture_folder(base_name, tag):
    """
    Create the data capture folder structure.

    Args:
        base_name: The input file base name (e.g., 'S2G')
        tag: The timestamp tag (e.g., '20260123_14_30')

    Returns:
        The path to the created capture folder
    """
    capture_folder = os.path.join('data_capture', f'{base_name}_{tag}')
    os.makedirs(capture_folder, exist_ok=True)
    return capture_folder


def create_iteration_folder(capture_folder, iteration_num):
    """
    Create a subfolder for an iteration.

    Args:
        capture_folder: The main capture folder path
        iteration_num: The iteration number (1, 2, 3, ...)

    Returns:
        The path to the created iteration folder
    """
    iteration_folder = os.path.join(capture_folder, f'iteration_{str(iteration_num).zfill(2)}')
    os.makedirs(iteration_folder, exist_ok=True)
    return iteration_folder


def save_ref_dict(refDict, filepath):
    """
    Save refDict to CSV file.

    Args:
        refDict: Dictionary where key=refID, value=list of tokens
        filepath: Full path to the output CSV file
    """
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['refID', 'tokens'])
        for refID, tokens in sorted(refDict.items()):
            # Join tokens with space for readability
            writer.writerow([refID, ', '.join(tokens)])
    print(f'  Data captured: {filepath}')


def save_link_index(linkIndex, filepath, refDict=None):
    """
    Save linkIndex to CSV file with optional token details.

    Args:
        linkIndex: Dictionary where key=refID, value=clusterID
        filepath: Full path to the output CSV file
        refDict: Optional dictionary where key=refID, value=list of tokens
    """
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if refDict:
            writer.writerow(['refID', 'clusterID', 'tokens'])
            for refID, clusterID in sorted(linkIndex.items()):
                tokens = ', '.join(refDict.get(refID, []))
                writer.writerow([refID, clusterID, tokens])
        else:
            writer.writerow(['refID', 'clusterID'])
            for refID, clusterID in sorted(linkIndex.items()):
                writer.writerow([refID, clusterID])
    print(f'  Data captured: {filepath}')


def save_token_freq_dict(tokenFreqDict, filepath):
    """
    Save tokenFreqDict to CSV file, sorted by frequency descending.

    Args:
        tokenFreqDict: Dictionary where key=token, value=frequency
        filepath: Full path to the output CSV file
    """
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['token', 'frequency'])
        # Sort by frequency descending, then by token alphabetically
        sorted_items = sorted(tokenFreqDict.items(), key=lambda x: (-x[1], x[0]))
        for token, freq in sorted_items:
            writer.writerow([token, freq])
    print(f'  Data captured: {filepath}')


def save_block_pair_list(blockPairList, filepath, refDict, truthDict=None):
    """
    Save blockPairList to CSV file with full token details and optional truth column.

    Args:
        blockPairList: List of strings in format "refID1|refID2"
        filepath: Full path to the output CSV file
        refDict: Dictionary where key=refID, value=list of tokens
        truthDict: Optional dictionary where key=refID, value=truthID
    """
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if truthDict:
            writer.writerow(['refID1', 'tokens1', 'refID2', 'tokens2', 'truth'])
        else:
            writer.writerow(['refID1', 'tokens1', 'refID2', 'tokens2'])
        for pair in blockPairList:
            parts = pair.split('|')
            if len(parts) == 2:
                refID1, refID2 = parts[0], parts[1]
                tokens1 = ', '.join(refDict.get(refID1, []))
                tokens2 = ', '.join(refDict.get(refID2, []))
                if truthDict:
                    # Determine truth: 1 if both refIDs have the same truthID, 0 otherwise
                    truth1 = truthDict.get(refID1)
                    truth2 = truthDict.get(refID2)
                    truth = 1 if (truth1 is not None and truth2 is not None and truth1 == truth2) else 0
                    writer.writerow([refID1, tokens1, refID2, tokens2, truth])
                else:
                    writer.writerow([refID1, tokens1, refID2, tokens2])
    print(f'  Data captured: {filepath}')


def save_linked_pair_list(linkedPairList, filepath, refDict, truthDict=None):
    """
    Save linkedPairList to CSV file with full token details and optional truth column.

    Args:
        linkedPairList: List of tuples (refID1, refID2)
        filepath: Full path to the output CSV file
        refDict: Dictionary where key=refID, value=list of tokens
        truthDict: Optional dictionary where key=refID, value=truthID
    """
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if truthDict:
            writer.writerow(['refID1', 'tokens1', 'refID2', 'tokens2', 'truth'])
        else:
            writer.writerow(['refID1', 'tokens1', 'refID2', 'tokens2'])
        for pair in linkedPairList:
            refID1, refID2 = pair[0], pair[1]
            tokens1 = ', '.join(refDict.get(refID1, []))
            tokens2 = ', '.join(refDict.get(refID2, []))
            if truthDict:
                # Determine truth: 1 if both refIDs have the same truthID, 0 otherwise
                truth1 = truthDict.get(refID1)
                truth2 = truthDict.get(refID2)
                truth = 1 if (truth1 is not None and truth2 is not None and truth1 == truth2) else 0
                writer.writerow([refID1, tokens1, refID2, tokens2, truth])
            else:
                writer.writerow([refID1, tokens1, refID2, tokens2])
    print(f'  Data captured: {filepath}')


def save_cluster_list(clusterList, filepath, refDict, truthDict=None):
    """
    Save clusterList to CSV file with full token details and optional truth column.

    Args:
        clusterList: List of tuples (clusterID, refID)
        filepath: Full path to the output CSV file
        refDict: Dictionary where key=refID, value=list of tokens
        truthDict: Optional dictionary where key=refID, value=truthID
    """
    # Pre-process: group refIDs by clusterID to determine cluster truth
    clusterGroups = {}
    for clusterID, refID in clusterList:
        if clusterID not in clusterGroups:
            clusterGroups[clusterID] = []
        clusterGroups[clusterID].append(refID)

    # Determine reference truth ID for each cluster
    clusterTruthIDs = {}
    if truthDict:
        for clusterID, refIDs in clusterGroups.items():
            # Try clusterID first (clusterID is also a refID)
            if clusterID in truthDict:
                clusterTruthIDs[clusterID] = truthDict[clusterID]
            else:
                # Find first refID with a truthID
                clusterTruthIDs[clusterID] = None
                for refID in refIDs:
                    if refID in truthDict:
                        clusterTruthIDs[clusterID] = truthDict[refID]
                        break

    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if truthDict:
            writer.writerow(['clusterID', 'refID', 'tokens', 'truth'])
        else:
            writer.writerow(['clusterID', 'refID', 'tokens'])

        # Sort by clusterID, then by refID
        sorted_clusters = sorted(clusterList, key=lambda x: (x[0], x[1]))
        for cluster in sorted_clusters:
            clusterID, refID = cluster[0], cluster[1]
            tokens = ', '.join(refDict.get(refID, []))

            if truthDict:
                # Get the cluster's reference truth ID
                clusterTruthID = clusterTruthIDs.get(clusterID)
                # Get this refID's truth ID
                refTruthID = truthDict.get(refID)
                # Determine truth: 1 if matches, 0 otherwise
                truth = 1 if (clusterTruthID is not None and
                             refTruthID is not None and
                             refTruthID == clusterTruthID) else 0
                writer.writerow([clusterID, refID, tokens, truth])
            else:
                writer.writerow([clusterID, refID, tokens])
    print(f'  Data captured: {filepath}')


_digit_re = re.compile(r'\d')


def _split_name_address(tokens):
    name_tokens = []
    address_tokens = []
    found_number = False
    for token in tokens:
        if not found_number and _digit_re.search(token):
            found_number = True
        if found_number:
            address_tokens.append(token)
        else:
            name_tokens.append(token)
    return name_tokens, address_tokens


def save_cluster_json(clusterList, filepath, refDict):
    """
    Save clusterList to JSON with name/address split and merged refIDs.

    Args:
        clusterList: List of tuples (clusterID, refID)
        filepath: Full path to output JSON file
        refDict: Dictionary where key=refID, value=list of tokens
    """
    clusters = {}
    for cluster_id, ref_id in clusterList:
        tokens = refDict.get(ref_id, [])
        name_tokens, address_tokens = _split_name_address(tokens)
        name = ' '.join(name_tokens).strip()
        address = ' '.join(address_tokens).strip()
        key = (name, address)
        if cluster_id not in clusters:
            clusters[cluster_id] = {}
        if key not in clusters[cluster_id]:
            clusters[cluster_id][key] = set()
        clusters[cluster_id][key].add(ref_id)
    output = []
    for cluster_id in sorted(clusters.keys()):
        records = []
        for (name, address), ref_ids in sorted(clusters[cluster_id].items(), key=lambda x: (x[0][0], x[0][1])):
            records.append({
                'refIDs': ', '.join(sorted(ref_ids)),
                'name': name,
                'address': address
            })
        output.append({
            'clusterID': cluster_id,
            'records': records
        })
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=True)
    print(f'  Data captured: {filepath}')
def filter_tokens_for_comparison(tokens, tokenFreqDict):
    """
    Apply the same token filtering logic used in DWM55.linkBlockPairs.removeStopWords(),
    but safely (no KeyError if token missing from tokenFreqDict).
    Returns ONLY the tokens that will actually be compared.
    """
    sigma = DWM10_Parms.sigma
    removeDuplicateTokens = DWM10_Parms.removeDuplicateTokens
    removeExcludedBlkTokens = DWM10_Parms.removeExcludedBlkTokens
    minBlkTokenLen = DWM10_Parms.minBlkTokenLen
    excludeNumericBlocks = DWM10_Parms.excludeNumericBlocks

    kept = []
    for token in tokens:
        tokenLen = len(token)
        includeToken = True

        freq = tokenFreqDict.get(token, 0)
        if freq >= sigma:
            includeToken = False

        if removeExcludedBlkTokens:
            if tokenLen < minBlkTokenLen:
                includeToken = False
            if excludeNumericBlocks and token.isdigit():
                includeToken = False

        if removeDuplicateTokens and token in kept:
            includeToken = False

        if includeToken:
            kept.append(token)

    return kept


def _get_comparator():
    """
    Same comparator selection pattern as DWM55.linkBlockPairs.
    Returns (comparator_name, comparator_object_or_module).
    """
    comparator = DWM10_Parms.comparator
    if comparator == 'MongeElkan':
        return comparator, MongeElkan()
    if comparator == 'Cosine':
        return comparator, Cosine()
    if comparator == 'ScoringMatrixStd':
        return comparator, DWM65_ScoringMatrixStd
    if comparator == 'ScoringMatrixKris':
        return comparator, DWM66_ScoringMatrixKris
    raise ValueError(f'Invalid Comparator Value in Parms File: {comparator}')


def save_pair_comparison_view(pairList, filepath_prefix, refDict, tokenFreqDict, truthDict=None, trace_min_sim=0.01):
    """
    Creates TWO CSVs:
      1) <prefix>_pair_summary.csv
         - shows ONLY the filtered tokens that were actually compared + similarity + optional truth
      2) <prefix>_token_matches.csv (only when comparator == ScoringMatrixKris)
         - shows the greedy token-to-token matches that contributed to the score

    pairList can be:
      - blockPairList: ["A|B", "C|D", ...]
      - linkedPairList: [(A,B), (C,D), ...]
    """
    comparator_name, Comp = _get_comparator()

    mu = getattr(DWM10_Parms, "mu", None)

    summary_path = f"{filepath_prefix}_pair_summary.csv"
    matches_path = f"{filepath_prefix}_token_matches.csv"

    # Normalize pairs into (refID1, refID2)
    pairs = []
    for p in pairList:
        if isinstance(p, str):
            parts = p.split("|")
            if len(parts) == 2:
                pairs.append((parts[0], parts[1]))
        else:
            pairs.append((p[0], p[1]))

    # Write summary
    with open(summary_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        header = ["refID1", "compared_tokens1", "refID2", "compared_tokens2", "similarity"]
        if mu is not None:
            header.append("mu")
        if truthDict:
            header.append("truth")
        writer.writerow(header)

        # Optional matches (only for Kris)
        write_matches = (comparator_name == "ScoringMatrixKris")
        match_rows = []

        for refID1, refID2 in sorted(pairs, key=lambda x: (x[0], x[1])):
            t1_raw = refDict.get(refID1, [])
            t2_raw = refDict.get(refID2, [])

            t1 = filter_tokens_for_comparison(t1_raw, tokenFreqDict)
            t2 = filter_tokens_for_comparison(t2_raw, tokenFreqDict)

            # similarity call patterns:
            if comparator_name in ("Cosine", "MongeElkan"):
                sim = Comp.normalized_similarity(t1[:], t2[:])
            elif comparator_name == "ScoringMatrixStd":
                sim = Comp.normalized_similarity(t1[:], t2[:])
            else:  # ScoringMatrixKris
                sim, trace = Comp.normalized_similarity(t1[:], t2[:], return_trace=True, trace_min_sim=trace_min_sim)

                # Save token match trace rows (ONLY matched tokens with sim >= trace_min_sim)
                for item in trace:
                    match_rows.append([
                        refID1,
                        refID2,
                        item["step"],
                        item["token1"],
                        item["token2"],
                        item["sim"],
                        item["weight"],
                        item["weighted_sim"],
                        item["row_index"],
                        item["col_index"],
                    ])

            row = [
                refID1,
                ", ".join(t1),
                refID2,
                ", ".join(t2),
                sim,
            ]
            if mu is not None:
                row.append(mu)

            if truthDict:
                truth1 = truthDict.get(refID1)
                truth2 = truthDict.get(refID2)
                truth = 1 if (truth1 is not None and truth2 is not None and truth1 == truth2) else 0
                row.append(truth)

            writer.writerow(row)

    print(f"  Data captured: {summary_path}")

    # Write token matches if comparator is Kris
    if comparator_name == "ScoringMatrixKris":
        with open(matches_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                "refID1", "refID2", "step",
                "token1", "token2",
                "sim", "weight", "weighted_sim",
                "row_index", "col_index"
            ])
            for r in match_rows:
                writer.writerow(r)
        print(f"  Data captured: {matches_path}")
