#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sys
import os
import time
import datetime
import logging
import DWM10_Parms
import DWM14_BuildRefDict
import DWM15_BuildLinkIndex
import DWM16_BuildTokenFreqDict
import DWM25_Global_Token_Replace
import DWM42_BuildBlockPairs
import DWM45_Block_Cleaning
import DWM55_LinkBlockPairs
import DWM80_TransitiveClosure
import DWM90_IterateClusters
import DWM96_WriteLinkIndex
import DWM97_ClusterProfile
import DWM99_ERmetrics
import DWM100_ReportData
import DWM_DataCapture
import xlsxwriter


# In[2]:


# Main Driver for Refactored Data Washing Machine
# Version 1.20 creates a log file with same information being written to console
# Version 1.30 creates cluster profile at end of program and evaluates ER statistics
# Version 1.40 FK - added module DWM25 to do global level token replacement
#              JRT - added DWM65_ScoringMatrix to allow ScoringMatrix as a comparitor type
# Version 1.50 Revised and corrected scoring matrix
#              Revised DWM25 Global Replacement to reuse Tokenizer Dictionary and use DWM_WordList.txt
# Version 1.60 Implemented 2 versions of Scoring Rule - Standard (Std) and Weighted (Kris)
#              Changed Parms to be a class imported by all modules
# Version 1.70 Added new parameter minBlkTokenLen to set a minimum length for blokcing tokens
#              Improved performance of global cleaning
# Version 1.80 Added new parameter excludeNumericBlocks when True does not block on numeric tokens
#              Added new parameter removeExcludedBlkTokens when True removes tokens excluded by
#                 minBlkTokenLen & exludeNumbericBlocks
#              Added timer and added Total Runtime to logging statistics
# Version 1.90 Extensive refactor of the processing logic separating stop word removal from blocking.
#              Also generate and deduplicate pairs from all blocks before comparing or correcting references
#              Added a new parameter blockByPairs requiring refs in a block to share 2 tokens
# Version 2.00 Added new parameter runIterationProfile to print profile & stats at end of each iteration
#              Also generated new token statistics for number of all-digit (numeric) tokens and ratio to total
#              Added token length profile and statistics
# Version 2.10 Added DWM100 for reporting to create a spreadsheet of statistics with parameter settings.
#              Added DWM45 with a new parameter blockCorrection to correct blocking tokens.
#              Added changes to DWM25 and DWM80 for faster processing of data.
# Version 2.20 Both global and block corrections output to log file instead of separate files
#              3 new parms, globalCorrectionDetail, blockCorrectionDetail, addRefsToLinkIndex
# Version 2.21 Corrected two bugs, one in Global Correction, and one in DWM90
# Version 2.30 Added new value 'CompressNbr' to parameter 'tokenizerType' 
# Version 2.31 Corrected a bug in DWM42 where last key group was not being processed
version = 2.31

# get start time for timer
startTime = time.time()
# date time is used to label the logfile
now = datetime.datetime.now()
tag = str(now.year)+(str(now.month)).zfill(2)+(str(now.day)).zfill(2)
tag = tag+'_'+(str(now.hour)).zfill(2)+'_'+(str(now.minute)).zfill(2)
while True:
    choice = input('Enter 1 to run single parms file, Enter 2 to run a list of parms files ->')
    if choice == '1':
        multi = False
        parmFileName = input('Enter Name of a Single Parameter File ->')
        break
    if choice == '2':
        multi = True
        fileName = input('Enter Name of a List of Parameter Files ->')
        file1 = open(fileName, 'r')
        break
    print('Try again')

# Get input filename to include in log and results filenames
if multi == True:
    # For multi mode, read first parms file to get input filename
    firstParmFileName = file1.readline().strip()
    file1.seek(0)  # Reset to beginning of file
    tempParmFile = open(firstParmFileName, 'r')
else:
    tempParmFile = open(parmFileName, 'r')

# Extract inputFileName from parms file
inputFileBaseName = 'unknown'
for line in tempParmFile:
    line = line.strip()
    if line.startswith('inputFileName'):
        parts = line.split('=')
        if len(parts) == 2:
            fullInputFileName = parts[1].strip()
            # Extract just the filename without path and extension
            if '\\' in fullInputFileName or '/' in fullInputFileName:
                fullInputFileName = fullInputFileName.replace('/', '\\')
                inputFileBaseName = fullInputFileName.split('\\')[-1]
            else:
                inputFileBaseName = fullInputFileName
            # Remove extension
            if '.' in inputFileBaseName:
                inputFileBaseName = inputFileBaseName.rsplit('.', 1)[0]
        break
tempParmFile.close()

# Create data capture folder for intermediate results
captureFolder = DWM_DataCapture.create_capture_folder(inputFileBaseName, tag)
print(f'Data capture folder created: {captureFolder}')

#data reporting init with input filename
logFile = open('DWM_Log_'+inputFileBaseName+'_'+tag+'.txt','w')
print("Data Washing Machine Refactor Version",version)
print("Data Washing Machine Refactor Version",version, file=logFile)
print("Date/Time",tag)
print("Data/Time",tag, file=logFile)
excelFileName = 'DWM_Results_'+inputFileBaseName+'_'+tag+'.xlsx'
DWM10_Parms.workbook = xlsxwriter.Workbook(excelFileName)
DWM10_Parms.worksheet = DWM10_Parms.workbook.add_worksheet()
DWM10_Parms.startRow = 0
while True:    
    now1 = datetime.datetime.now()
    if multi == True:
        parmFileName = file1.readline()
        print('\n\nRunning parms file',parmFileName)
        print('\nRunning parms file ',parmFileName, file=logFile)
        parmFileName = parmFileName.replace('\n','')
        if not parmFileName:
            print('\nEnd of the parmFileName Runs')
            break
    else:
        print('\n\nRunning parms file',parmFileName)
        print('\nRunning parms file ',parmFileName, file=logFile)   
    DWM10_Parms.getParms(parmFileName, logFile)
    # Must get mu start and save to muStart for single parms file
    # and epsilonStart for parmeter single parms file
    # DWM10_Parms.blockCorrection changes if DWM45 runs need original value for reporting
    DWM10_Parms.muStart=DWM10_Parms.mu
    DWM10_Parms.epsilonStart=DWM10_Parms.epsilon
    DWM10_Parms.blockCorrect =DWM10_Parms.blockCorrection
    # Load truth dictionary for data capture (if truth file is provided)
    truthDict = DWM_DataCapture.load_truth_dict(DWM10_Parms.truthFileName)
    # Create refDict, a dictionary where key=refID, value is list of reference tokens
    refDict = DWM14_BuildRefDict.tokenizeInput()
    DWM_DataCapture.save_ref_dict(refDict, os.path.join(captureFolder, '01_refDict.csv'))
    # Create linkIndx, a dictionary where key=refID, value is cluster ID`
    linkIndex = DWM15_BuildLinkIndex.buildLinkIndex(refDict)
    DWM_DataCapture.save_link_index(linkIndex, os.path.join(captureFolder, '02_linkIndex_initial.csv'), refDict)
    # Create tokenFeqDict, a dictionary where key=token, value is token frequency
    tokenFreqDict =DWM16_BuildTokenFreqDict.buildTokenFreqDict(refDict)
    DWM_DataCapture.save_token_freq_dict(tokenFreqDict, os.path.join(captureFolder, '03_tokenFreqDict.csv'))
    # create dictionary of corrections (stdTokenDict), leave empty if not running replacement
    #if global replacement configured, populate stdTokenDict of corrections in DWM25
    if DWM10_Parms.runGlobalCorrection:
        refDict = DWM25_Global_Token_Replace.globalReplace(refDict, tokenFreqDict)
        tokenFreqDict =DWM16_BuildTokenFreqDict.buildTokenFreqDict(refDict)
        DWM_DataCapture.save_ref_dict(refDict, os.path.join(captureFolder, '04_refDict_after_global_correction.csv'))
        DWM_DataCapture.save_token_freq_dict(tokenFreqDict, os.path.join(captureFolder, '04_tokenFreqDict_after_global_correction.csv'))
    moreToDo = True
    iterationNum = 0  # Track iteration number for data capture
    print('\n>>Starting Iterations')
    print('\n>>Starting Iterations', file=logFile)
    mu = DWM10_Parms.mu
    print('mu start value=', mu)
    print('mu start value=', mu, file=logFile)
    muIterate = DWM10_Parms.muIterate
    print('mu iterate value=', muIterate)
    print('mu iterate value=', muIterate, file=logFile)
    epsilon = DWM10_Parms.epsilon
    print('epsilon start value=', epsilon)
    print('epsilon start value=', epsilon, file=logFile)
    epsilonIterate = DWM10_Parms.epsilonIterate
    print('epsilon iterate value=', epsilonIterate)
    print('epsilon iterate value=', epsilonIterate, file=logFile)
    comparator = DWM10_Parms.comparator
    print('comparator =', comparator)
    print('comparator =', comparator, file=logFile)
    firstIteration = True
    while moreToDo:
        iterationNum += 1
        iterationFolder = DWM_DataCapture.create_iteration_folder(captureFolder, iterationNum)
        print('\n****New Iteration\nSize of refDict =', len(refDict))
        print('\n****New Iteration\nSize of refDict =', len(refDict), file=logFile)
        #blockList = DWM40_BuildBlocks.buildBlocks(logFile, refList, tokenFreqDict)
        blockPairList = DWM42_BuildBlockPairs.buildBlockPairs(refDict, linkIndex, tokenFreqDict)
        DWM_DataCapture.save_block_pair_list(blockPairList, os.path.join(iterationFolder, '05_blockPairList.csv'), refDict, truthDict)
        # Calculate blocking metrics if truth file is provided
        if DWM10_Parms.truthFileName != '':
            DWM99_ERmetrics.generateBlockingMetrics(blockPairList, iterationNum, refDict)
        if len(blockPairList)==0:
            print('--Ending because blockPairList is empty')
            print('--Ending because blockPairList is empty', file=logFile)
            break
        # If block correction requested, only run once on first iteration
        if DWM10_Parms.blockCorrection and firstIteration:
            changeCount = DWM45_Block_Cleaning.RunBlockCorrections(blockPairList, tokenFreqDict, refDict)
            # if there were block corrections, rebuild token dictionary and re-block
            if changeCount > 0:
                tokenFreqDict=DWM16_BuildTokenFreqDict.buildTokenFreqDict(refDict)
                blockPairList = DWM42_BuildBlockPairs.buildBlockPairs(refDict, linkIndex, tokenFreqDict)
                DWM_DataCapture.save_ref_dict(refDict, os.path.join(iterationFolder, '06_refDict_after_block_correction.csv'))
                DWM_DataCapture.save_block_pair_list(blockPairList, os.path.join(iterationFolder, '06_blockPairList_after_block_correction.csv'), refDict, truthDict)
                # Recalculate blocking metrics after correction
                if DWM10_Parms.truthFileName != '':
                    DWM99_ERmetrics.generateBlockingMetrics(blockPairList, iterationNum, refDict)
            firstIteration = False
        linkedPairList = DWM55_LinkBlockPairs.linkBlockPairs(blockPairList, refDict, tokenFreqDict)
        DWM_DataCapture.save_linked_pair_list(linkedPairList, os.path.join(iterationFolder, '07_linkedPairList.csv'), refDict, truthDict)
        if len(linkedPairList)==0:
            print('Ending because linkedPairList is empty')
            print('Ending because linkedPairList is empty', file=logFile)
            break
        clusterList = DWM80_TransitiveClosure.transitiveClosure(linkedPairList)
        DWM_DataCapture.save_cluster_list(clusterList, os.path.join(iterationFolder, '08_clusterList.csv'), refDict, truthDict)
        if len(clusterList)==0:
            print('--Ending because clusterList is empty')
            print('--Ending because clusterList is empty', file=logFile)
            break
        iterationLinkIndex = DWM90_IterateClusters.iterateClusters(clusterList, refDict, linkIndex)
        DWM_DataCapture.save_link_index(iterationLinkIndex, os.path.join(iterationFolder, '09_linkIndex.csv'), refDict)
        print("\n>>Itermediate Results from this Iteration")
        print("\n>>Itermediate Results from this Iteration", file=logFile)
        # Run iteration profile and statistics if requested
        if DWM10_Parms.runIterationProfile:
            DWM97_ClusterProfile.generateProfile(iterationLinkIndex)
            if DWM10_Parms.truthFileName != '':
                DWM99_ERmetrics.generateMetrics(iterationLinkIndex)
        print('\n>>End of Iteration, Resetting mu and epsilon')
        print('\n>>End of Iteration, Resetting mu and epsilon', file=logFile)
        mu += muIterate
        mu = round(mu, 2)
        DWM10_Parms.mu = mu
        print('>>>New Value of mu = ',mu)
        print('>>>New Value of mu = ',mu, file=logFile)
        epsilon += epsilonIterate
        epsilon = round(epsilon, 2)
        DWM10_Parms.epsilon = epsilon
        print('>>>New Value of epsilon = ',epsilon)
        print('>>>New Value of epsilon = ',epsilon, file=logFile)
        if mu > 1.0:
            moreToDo = False
            print('Ending because mu > 1.0')
            print('Ending because mu > 1.0', file=logFile)
    # End of iterations
    # Save final linkIndex to data capture folder
    DWM_DataCapture.save_link_index(linkIndex, os.path.join(captureFolder, 'final_linkIndex.csv'), refDict)
    # write Link Index to text file
    DWM96_WriteLinkIndex.writeLinkIndex(linkIndex, refDict)
    # Generate Cluster Profile
    DWM97_ClusterProfile.generateProfile(linkIndex)
    # Generat ER Metrics if truthFileName was given
    if DWM10_Parms.truthFileName != '':
        DWM99_ERmetrics.generateMetrics(linkIndex)
        DWM100_ReportData.reportData()
    now2 = datetime.datetime.now()
    print("\nTotal File Runtime =", now2-now1, file=logFile)
    print("\nEnd of File ",parmFileName)
    print('Time to run File ', now2-now1)
    print("End of File ",parmFileName, file=logFile)
    if multi==False:
        break
if multi==True:
    file1.close()
endTime = time.time()
totalTime = endTime - startTime
print("All Files Total Runtime =", totalTime/60, " minutes")
print("All Files Total Runtime =", totalTime/60, " minutes", file=logFile)
print("End of Program")
print("End of Program", file=logFile)
logFile.close()
DWM10_Parms.workbook.close()

