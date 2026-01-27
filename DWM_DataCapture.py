#!/usr/bin/env python
# coding: utf-8

"""
DWM_DataCapture.py - Utility module for capturing intermediate data after each DWM process step.

This module provides functions to save data structures to CSV files,
allowing users to understand how data transforms through the pipeline.
"""

import os
import csv


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
