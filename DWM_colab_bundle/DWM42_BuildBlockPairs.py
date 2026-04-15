#!/usr/bin/env python
# coding: utf-8

# In[1]:



import DWM10_Parms
import csv
import os
import re
import sys
import logging

import numpy as np

_DIGIT_RE = re.compile(r'\d')
_MODEL_CACHE = {}


def _split_name_address_tokens(tokenList):
    nameTokens = []
    addressTokens = []
    foundAddress = False
    for token in tokenList:
        if (not foundAddress) and _DIGIT_RE.search(token):
            foundAddress = True
        if foundAddress:
            addressTokens.append(token)
        else:
            nameTokens.append(token)
    return nameTokens, addressTokens


def _write_embedding_inputs(outputFolder, captureTag, rows):
    if outputFolder is None:
        return
    os.makedirs(outputFolder, exist_ok=True)
    namePath = os.path.join(outputFolder, captureTag+'_embedding_name_text.csv')
    addressPath = os.path.join(outputFolder, captureTag+'_embedding_address_text.csv')
    overlapPath = os.path.join(outputFolder, captureTag+'_embedding_overlap_text.csv')
    with open(namePath, 'w', newline='', encoding='utf-8') as nameFile:
        writer = csv.writer(nameFile)
        writer.writerow(['refID', 'nameText'])
        for row in rows:
            writer.writerow([row[0], row[1]])
    with open(addressPath, 'w', newline='', encoding='utf-8') as addressFile:
        writer = csv.writer(addressFile)
        writer.writerow(['refID', 'addressText'])
        for row in rows:
            writer.writerow([row[0], row[2]])
    with open(overlapPath, 'w', newline='', encoding='utf-8') as overlapFile:
        writer = csv.writer(overlapFile)
        writer.writerow(['refID', 'overlapText'])
        for row in rows:
            writer.writerow([row[0], row[3]])


def _write_embedding_clusters(outputFolder, captureTag, refIDs, labels):
    if outputFolder is None:
        return
    clusterPath = os.path.join(outputFolder, captureTag+'_embedding_cluster_labels.csv')
    with open(clusterPath, 'w', newline='', encoding='utf-8') as clusterFile:
        writer = csv.writer(clusterFile)
        writer.writerow(['refID', 'clusterLabel'])
        for j in range(len(refIDs)):
            writer.writerow([refIDs[j], labels[j]])


def _get_embedding_model(modelName, device):
    cacheKey = (modelName, device)
    model = _MODEL_CACHE.get(cacheKey)
    if model is not None:
        return model
    # Reduce TensorFlow console noise and disable oneDNN warning for deterministic behavior.
    os.environ.setdefault('TF_CPP_MIN_LOG_LEVEL', '2')
    os.environ.setdefault('TF_ENABLE_ONEDNN_OPTS', '0')
    os.environ.setdefault('TRANSFORMERS_NO_TF', '1')
    os.environ.setdefault('USE_TF', '0')
    logging.getLogger('tensorflow').setLevel(logging.ERROR)
    from sentence_transformers import SentenceTransformer
    if device.lower() == 'auto':
        model = SentenceTransformer(modelName)
    else:
        model = SentenceTransformer(modelName, device=device)
    _MODEL_CACHE[cacheKey] = model
    return model


def _progress_iter(iterable, total, desc, unit='it'):
    try:
        from tqdm.auto import tqdm
        return tqdm(iterable, total=total, desc=desc, unit=unit, leave=False, disable=not sys.stdout.isatty())
    except Exception:
        return iterable


def _build_token_block_pairs(refDict, linkIndex, tokenFreqDict):
    logFile = DWM10_Parms.logFile
    blockByPairs = DWM10_Parms.blockByPairs
    beta = DWM10_Parms.beta
    minBlkTokenLen = DWM10_Parms.minBlkTokenLen
    excludeNumericBlocks = DWM10_Parms.excludeNumericBlocks
    print('beta =', beta)
    print('beta =', beta, file=logFile)
    print('min blocking token length =', minBlkTokenLen)
    print('min blocking token length =', minBlkTokenLen, file=logFile)
    print('exclude numeric blocking tokens =', excludeNumericBlocks)
    print('exclude numeric blocking tokens =', excludeNumericBlocks, file=logFile)
    print('block by pairs of tokens =', blockByPairs)
    print('block by pairs of tokens =', blockByPairs, file=logFile)
    # blockList is a list of ordered pairs (blockingValue, RefID) where blockingValue
    # is a single blocking token when "blockByPairs is False"
    # or concatenated pairs of blocking tokens when "blockByPairs is True"
    blockList = []
    selectCnt = 0
    # First extract the blocking tokens from each reference into blockTokenList
    for key in linkIndex:
        if len(linkIndex[key]) > 0:
            continue
        selectCnt += 1
        tokenList = refDict[key]
        blockTokenList = []
        for token in tokenList:
            # Decide if token is going to be a Blocking Token
            isBlkToken = True
            if len(token) < minBlkTokenLen:
                isBlkToken = False
            if excludeNumericBlocks and token.isdigit():
                isBlkToken = False
            freq = tokenFreqDict[token]
            if freq < 2 or freq > beta:
                isBlkToken = False
            if isBlkToken:
                blockTokenList.append(token)
        tokenCnt = len(blockTokenList)
        # If there were no blocking tokens, then nothing to do
        if tokenCnt < 1:
            continue
        # When "blockByPairs==True" form all pairs from list
        if blockByPairs:
            if tokenCnt < 2:
                continue
            for j in range(0, tokenCnt - 1):
                for k in range(j + 1, tokenCnt):
                    tokenJ = blockTokenList[j]
                    tokenK = blockTokenList[k]
                    # Always concatenate pairs in ascending order
                    if tokenJ < tokenK:
                        blockList.append((tokenJ + tokenK, key))
                    else:
                        blockList.append((tokenK + tokenJ, key))
        else:
            for j in range(0, tokenCnt):
                tokenJ = blockTokenList[j]
                blockList.append((tokenJ, key))
    # End of iteration of refDict
    print('Total Records Selected for Reprocessing', selectCnt)
    print('Total Records Selected for Reprocessing', selectCnt, file=logFile)
    # Sort blockList
    blockList.sort()
    blockListLen = len(blockList)
    print('Total Blocking Records Created', blockListLen)
    print('Total Blocking Records Created', blockListLen, file=logFile)
    # Phase 2, generate blocks and pairs of refs in each block
    # start by appending a caboose to the end of the blockList
    blockList.append(('XXXXX', 'X'))
    block = []
    blockPairList = []
    blockCnt = 0
    for j in range(0, blockListLen):
        currPair = blockList[j]
        currBlockToken = currPair[0]
        block.append(currPair)
        nextPair = blockList[j + 1]
        nextBlockToken = nextPair[0]
        if currBlockToken != nextBlockToken:
            blockLen = len(block)
            if blockLen > 1:
                blockCnt += 1
                for m in range(0, blockLen - 1):
                    pairM = block[m]
                    refIDm = pairM[1]
                    for n in range(m + 1, blockLen):
                        pairN = block[n]
                        refIDn = pairN[1]
                        if refIDm < refIDn:
                            blockPairList.append(refIDm + '|' + refIDn)
                        else:
                            blockPairList.append(refIDn + '|' + refIDm)
            block.clear()
    # End of j loop
    print('Total Blocks Size>1 Created', blockCnt)
    print('Total Blocks Size>1 Created', blockCnt, file=logFile)
    # Deduplicate and sort pair list
    print('Total Pairs Generated by Blocks=', len(blockPairList))
    print('Total Pairs Generated by Blocks=', len(blockPairList), file=logFile)
    blockPairList = list(set(blockPairList))
    blockPairList.sort()
    print('Total Unduplicated Pairs =', len(blockPairList))
    print('Total Unduplicated Pairs =', len(blockPairList), file=logFile)
    return blockPairList


def _build_embedding_block_pairs(refDict, linkIndex, outputFolder, captureTag):
    logFile = DWM10_Parms.logFile
    modelName = DWM10_Parms.embeddingModelName
    blockMethod = DWM10_Parms.embeddingBlockMethod.upper()
    eps = DWM10_Parms.embeddingBlockEps
    minSamples = DWM10_Parms.embeddingBlockMinSamples
    topK = DWM10_Parms.embeddingTopK
    batchSize = DWM10_Parms.embeddingBatchSize
    nameWeight = DWM10_Parms.embeddingNameWeight
    addressWeight = 1.0 - nameWeight
    embeddingDevice = DWM10_Parms.embeddingDevice
    print('blocking mode = 1 (embedding)')
    print('blocking mode = 1 (embedding)', file=logFile)
    print('embedding model =', modelName)
    print('embedding model =', modelName, file=logFile)
    print('embedding method =', blockMethod)
    print('embedding method =', blockMethod, file=logFile)
    print('embedding device =', embeddingDevice)
    print('embedding device =', embeddingDevice, file=logFile)
    if blockMethod == 'DBSCAN':
        print('embedding eps =', eps)
        print('embedding eps =', eps, file=logFile)
        print('embedding min samples =', minSamples)
        print('embedding min samples =', minSamples, file=logFile)
    elif blockMethod == 'KNN':
        print('embedding topK =', topK)
        print('embedding topK =', topK, file=logFile)
    else:
        print('**Error: Invalid embeddingBlockMethod', blockMethod)
        print('**Error: Invalid embeddingBlockMethod', blockMethod, file=logFile)
        sys.exit()
    print('embedding name/address weight =', nameWeight, '/', addressWeight)
    print('embedding name/address weight =', nameWeight, '/', addressWeight, file=logFile)
    selectedRefIDs = []
    nameInputs = []
    addressInputs = []
    captureRows = []
    prepIter = _progress_iter(linkIndex, total=len(linkIndex), desc='DWM42 embed prep', unit='ref')
    for key in prepIter:
        if len(linkIndex[key]) > 0:
            continue
        tokenList = refDict[key]
        nameTokens, addressTokens = _split_name_address_tokens(tokenList)
        nameText = ' '.join(nameTokens).strip()
        addressText = ' '.join(addressTokens).strip()
        overlapText = (nameText + ' || ' + addressText).strip()
        if overlapText == '':
            continue
        selectedRefIDs.append(key)
        nameInputs.append(nameText if nameText != '' else '[EMPTY_NAME]')
        addressInputs.append(addressText if addressText != '' else '[EMPTY_ADDRESS]')
        captureRows.append((key, nameText, addressText, overlapText))
    selectCnt = len(selectedRefIDs)
    print('Total Records Selected for Reprocessing', selectCnt)
    print('Total Records Selected for Reprocessing', selectCnt, file=logFile)
    if selectCnt < 2:
        print('Total Unduplicated Pairs = 0')
        print('Total Unduplicated Pairs = 0', file=logFile)
        return []
    _write_embedding_inputs(outputFolder, captureTag, captureRows)
    model = _get_embedding_model(modelName, embeddingDevice)
    print('Embedding names...')
    print('Embedding names...', file=logFile)
    nameEmbeddings = model.encode(nameInputs, batch_size=batchSize, convert_to_numpy=True,
                                  show_progress_bar=True, normalize_embeddings=True)
    print('Embedding addresses...')
    print('Embedding addresses...', file=logFile)
    addressEmbeddings = model.encode(addressInputs, batch_size=batchSize, convert_to_numpy=True,
                                     show_progress_bar=True, normalize_embeddings=True)
    overlapEmbeddings = (nameWeight * nameEmbeddings) + (addressWeight * addressEmbeddings)
    norms = np.linalg.norm(overlapEmbeddings, axis=1, keepdims=True)
    norms[norms == 0.0] = 1.0
    overlapEmbeddings = overlapEmbeddings / norms
    if blockMethod == 'KNN':
        from sklearn.neighbors import NearestNeighbors
        n = len(selectedRefIDs)
        k = min(topK, n - 1)
        print('Running KNN neighbor search...')
        print('Running KNN neighbor search...', file=logFile)
        knn = NearestNeighbors(metric='cosine', algorithm='brute')
        knn.fit(overlapEmbeddings)
        distances, indices = knn.kneighbors(overlapEmbeddings, n_neighbors=k + 1, return_distance=True)
        if outputFolder is not None:
            knnPath = os.path.join(outputFolder, captureTag+'_embedding_knn_neighbors.csv')
            with open(knnPath, 'w', newline='', encoding='utf-8') as knnFile:
                writer = csv.writer(knnFile)
                writer.writerow(['anchorRefID', 'neighborRefID', 'cosineDistance'])
                rowIter = _progress_iter(range(n), total=n, desc='DWM42 KNN export', unit='ref')
                for i in rowIter:
                    anchor = selectedRefIDs[i]
                    for pos in range(1, k + 1):
                        j = int(indices[i][pos])
                        neighbor = selectedRefIDs[j]
                        writer.writerow([anchor, neighbor, float(distances[i][pos])])
        blockPairSet = set()
        pairIter = _progress_iter(range(n), total=n, desc='DWM42 KNN pair build', unit='ref')
        for i in pairIter:
            refIDi = selectedRefIDs[i]
            for pos in range(1, k + 1):
                j = int(indices[i][pos])
                if i == j:
                    continue
                refIDj = selectedRefIDs[j]
                if refIDi < refIDj:
                    blockPairSet.add(refIDi + '|' + refIDj)
                else:
                    blockPairSet.add(refIDj + '|' + refIDi)
        blockPairList = list(blockPairSet)
        blockPairList.sort()
        print('Total KNN Pairs Generated =', len(blockPairList))
        print('Total KNN Pairs Generated =', len(blockPairList), file=logFile)
        print('Total Unduplicated Pairs =', len(blockPairList))
        print('Total Unduplicated Pairs =', len(blockPairList), file=logFile)
        return blockPairList

    from sklearn.cluster import DBSCAN
    clusterer = DBSCAN(eps=eps, min_samples=minSamples, metric='cosine')
    labels = clusterer.fit_predict(overlapEmbeddings)
    _write_embedding_clusters(outputFolder, captureTag, selectedRefIDs, labels)
    clusterToRefIDs = {}
    labelIter = _progress_iter(range(len(selectedRefIDs)), total=len(selectedRefIDs),
                               desc='DWM42 label grouping', unit='ref')
    for j in labelIter:
        label = labels[j]
        if label < 0:
            continue
        if label not in clusterToRefIDs:
            clusterToRefIDs[label] = []
        clusterToRefIDs[label].append(selectedRefIDs[j])
    blockPairSet = set()
    clusterCnt = 0
    clusterKeys = list(clusterToRefIDs.keys())
    pairIter = _progress_iter(clusterKeys, total=len(clusterKeys), desc='DWM42 pair build', unit='cluster')
    for label in pairIter:
        clusterRefIDs = sorted(clusterToRefIDs[label])
        if len(clusterRefIDs) < 2:
            continue
        clusterCnt += 1
        for m in range(0, len(clusterRefIDs) - 1):
            refIDm = clusterRefIDs[m]
            for n in range(m + 1, len(clusterRefIDs)):
                refIDn = clusterRefIDs[n]
                blockPairSet.add(refIDm + '|' + refIDn)
    print('Total Embedding Clusters Size>1 Created', clusterCnt)
    print('Total Embedding Clusters Size>1 Created', clusterCnt, file=logFile)
    blockPairList = list(blockPairSet)
    blockPairList.sort()
    print('Total Unduplicated Pairs =', len(blockPairList))
    print('Total Unduplicated Pairs =', len(blockPairList), file=logFile)
    return blockPairList


def buildBlockPairs(refDict, linkIndex, tokenFreqDict, outputFolder=None, captureTag='05'):
    logFile = DWM10_Parms.logFile
    print('\n>>Starting DWM42')
    print('\n>>Starting DWM42', file=logFile)
    blockingMode = DWM10_Parms.blockingMode
    if blockingMode == 0:
        print('blocking mode = 0 (token)')
        print('blocking mode = 0 (token)', file=logFile)
        return _build_token_block_pairs(refDict, linkIndex, tokenFreqDict)
    return _build_embedding_block_pairs(refDict, linkIndex, outputFolder, captureTag)
