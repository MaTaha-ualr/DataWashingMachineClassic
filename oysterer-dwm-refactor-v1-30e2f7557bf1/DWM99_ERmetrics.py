#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sys
import time
import datetime
from csv import reader
import DWM10_Parms
import DWM100_ReportData

def generateMetrics(linkIndex):
    logFile = DWM10_Parms.logFile
    print('\n>>Starting DWM99')
    print('>>Starting DWM99', file=logFile)
    truthFileName = DWM10_Parms.truthFileName
    print('Truth File Name=', truthFileName)
    print('Truth File Name=', truthFileName, file=logFile)    
    def countPairs(dict):
        totalPairs = 0
        for cnt in dict.values():
            pairs = cnt*(cnt-1)/2
            totalPairs +=pairs
        return totalPairs
    erDict = {}
    for refID in linkIndex:
        clusterID = linkIndex[refID]
        erDict[refID] = (clusterID,'x')
    truthFile = open(truthFileName,'r')
    line = (truthFile.readline()).strip()
    line = (truthFile.readline()).strip()
    while line != '':
        part = line.split(',')
        recID = part[0].strip()
        truthID = part[1].strip()
        if recID in erDict:
            oldPair = erDict[recID]
            clusterID = oldPair[0]
            newPair = (clusterID, truthID)
            erDict[recID] = newPair
        line = (truthFile.readline()).strip()
    linkedPairs = {}
    equivPairs = {}
    truePos = {}
    clusterIndex = []
    for pair in erDict.values():
        clusterID = pair[0]
        truthID = pair[1]
        if pair in truePos:
            cnt = truePos[pair]
            aPair = [pair[0],truthID]
            clusterIndex.append(aPair)                        
            cnt +=1
            truePos[pair] = cnt            
        else:
            truePos[pair] = 1
        if clusterID in linkedPairs:
            cnt = linkedPairs[clusterID]
            cnt +=1
            linkedPairs[clusterID] = cnt
        else:
            linkedPairs[clusterID] = 1
        if truthID in equivPairs:
            cnt = equivPairs[truthID]
            cnt +=1
            equivPairs[truthID] = cnt
        else:
            equivPairs[truthID] = 1   
    # End of counts
    L = countPairs(linkedPairs)
    E = countPairs(equivPairs)
    TP = countPairs(truePos)
    FP = float(L-TP)
    FN = float(E-TP)
    if L > 0:
        precision = round(TP/float(L),4)
    else:
        precision = 1.00
    if E > 0:
        recall = round(TP/float(E),4)
    else:
        recall = 1.00
  
    fmeas = round((2*precision*recall)/(precision+recall),4)
      
    # for report process
    DWM10_Parms.precision = precision
    DWM10_Parms.recall = recall
    DWM10_Parms.fmeasure = fmeas
    DWM10_Parms.truePairs = TP
    DWM10_Parms.expectedPairs = E
    DWM10_Parms.linkedPairs = L
    
    print('True Pairs =',TP)
    print('True Pairs =',TP, file=logFile)
    print('Expected Pairs =',E)
    print('Expected Pairs =',E, file=logFile)
    print('Linked Pairs =',L)
    print('Linked Pairs =',L, file=logFile)
    print('Precision=',precision)
    print('Precision=',precision, file=logFile)
    print('Recall=', recall)
    print('Recall=', recall, file=logFile)
    print('F-measure=', fmeas)
    print('F-measure=', fmeas, file=logFile)

    return

def generateBlockingMetrics(blockPairList, iterationNum, refDict):
    """
    Calculate precision, recall, and F-measure for the blocking stage.

    For blocking:
    - Candidate Pairs (C) = number of pairs in blockPairList
    - Expected Pairs (E) = number of true pairs from ground truth (only for records in current dataset)
    - True Positives (TP) = pairs in blockPairList that are true matches

    Precision = TP / C  (what fraction of candidate pairs are true matches)
    Recall = TP / E     (what fraction of true pairs are captured by blocking)
    F-measure = 2 * (precision * recall) / (precision + recall)
    """
    logFile = DWM10_Parms.logFile
    truthFileName = DWM10_Parms.truthFileName

    print('\n>>Blocking Metrics (Iteration', iterationNum, ')')
    print('>>Blocking Metrics (Iteration', iterationNum, ')', file=logFile)
    print('Truth File Name=', truthFileName)
    print('Truth File Name=', truthFileName, file=logFile)

    # Get set of refIDs in current dataset
    datasetRefIDs = set(refDict.keys())

    # Build truthDict: refID -> truthID (only for records in current dataset)
    truthDict = {}
    truthFile = open(truthFileName, 'r')
    line = (truthFile.readline()).strip()  # Skip header
    line = (truthFile.readline()).strip()
    while line != '':
        part = line.split(',')
        recID = part[0].strip()
        truthID = part[1].strip()
        # Only include records that are in the current dataset
        if recID in datasetRefIDs:
            truthDict[recID] = truthID
        line = (truthFile.readline()).strip()
    truthFile.close()

    # Count expected pairs from truth (pairs within same truth cluster, only for current dataset)
    truthClusterCounts = {}
    for refID, truthID in truthDict.items():
        if truthID in truthClusterCounts:
            truthClusterCounts[truthID] += 1
        else:
            truthClusterCounts[truthID] = 1

    # Helper function to count pairs using combinations formula
    def countPairs(clusterDict):
        totalPairs = 0
        for cnt in clusterDict.values():
            pairs = cnt * (cnt - 1) / 2
            totalPairs += pairs
        return totalPairs

    E = countPairs(truthClusterCounts)  # Expected pairs from ground truth

    # Count candidate pairs and true positives
    C = len(blockPairList)  # Candidate pairs
    TP = 0

    for pair in blockPairList:
        parts = pair.split('|')
        refID1 = parts[0]
        refID2 = parts[1]

        # Check if both refs exist in truth and share same truthID
        if refID1 in truthDict and refID2 in truthDict:
            if truthDict[refID1] == truthDict[refID2]:
                TP += 1

    # Calculate metrics
    if C > 0:
        precision = round(TP / float(C), 4)
    else:
        precision = 1.00

    if E > 0:
        recall = round(TP / float(E), 4)
    else:
        recall = 1.00

    if (precision + recall) > 0:
        fmeas = round((2 * precision * recall) / (precision + recall), 4)
    else:
        fmeas = 0.00

    # Store in DWM10_Parms for reporting
    DWM10_Parms.blockPrecision = precision
    DWM10_Parms.blockRecall = recall
    DWM10_Parms.blockFMeasure = fmeas
    DWM10_Parms.blockTruePairs = TP
    DWM10_Parms.blockCandidatePairs = C
    DWM10_Parms.blockExpectedPairs = E

    # Log results (dual output pattern)
    print('Block Candidate Pairs =', C)
    print('Block Candidate Pairs =', C, file=logFile)
    print('Block Expected Pairs =', int(E))
    print('Block Expected Pairs =', int(E), file=logFile)
    print('Block True Pairs =', TP)
    print('Block True Pairs =', TP, file=logFile)
    print('Block Precision =', precision)
    print('Block Precision =', precision, file=logFile)
    print('Block Recall =', recall)
    print('Block Recall =', recall, file=logFile)
    print('Block F-measure =', fmeas)
    print('Block F-measure =', fmeas, file=logFile)

    return

