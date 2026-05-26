#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#!/usr/bin/env python
#coding: utf-8

import sys
import os
#####################################
# Parameters set by the User Script
#####################################
# Input Parameters
inputFileName = ''
delimiter = ','
hasHeader = False
tokenizerType = 'Splitter'
truthFileName = ''
runIterationProfile = False
addRefsToLinkIndex = False
# Global Correction Parameters
runGlobalCorrection = False
globalCorrectionDetail = False
learnTokenVariants = False
minFreqStdToken = 5
minLenStdToken = 3
maxFreqErrToken = 3
# Blocking Parameters
beta = 2
blockByPairs = True
minBlkTokenLen = 4
excludeNumericBlocks = True
blockingMode = 0
embeddingModelName = 'BAAI/bge-m3'
embeddingBlockMethod = 'DBSCAN'
embeddingBlockEps = 0.08
embeddingBlockMinSamples = 2
embeddingTopK = 10
embeddingBatchSize = 64
embeddingNameWeight = 0.76
embeddingDevice = 'auto'
# Block Correction Parameters
blockCorrection = False
blockCorrectionDetail = False
# Linking Parameters
epsilon = 0.50
epsilonIterate = 0.00
mu = 0.50
muIterate = 0.10
comparator = 'ScoringMatrixStd'
matrixNumTokenRule = False
matrixInitialRule = False
tahaRejectMuRatio = 0.25
tahaReviewUpperDelta = 0.10
tahaContextThreshold = 0.90
tahaMinNameSimilarity = 0.55
tahaMinLastNameSimilarity = 0.50
tahaEnableNameHardReject = False
tahaAliasFile = 'alias.dat'
tahaLlmDecisionFile = ''
tahaUseOpenAIReview = False
tahaOpenAIModel = 'gpt-4.1'
tahaOpenAIApiKeyEnv = 'OPENAI_API_KEY'
tahaOpenAIBaseURL = 'https://api.openai.com/v1'
tahaOpenAITimeoutSec = 45.0
tahaOpenAIMaxRetries = 2
tahaOpenAITemperature = 0.0
tahaOpenAIMaxOutputTokens = 120
tahaOpenAIReviewMaxPairs = 0
tahaUseAsyncOpenAIReview = True
tahaOpenAIAsyncWorkers = 8
tahaUseAnchorBatchReview = False
tahaAnchorBatchSize = 6
tahaUseDeterministicRules = True
tahaPoisonAddressMinSimilarity = 0.92
tahaPoisonFirstNameMax = 0.35
tahaPoisonLastNameMax = 0.70
tahaPoisonNameSimilarityMax = 0.68
tahaPoisonRequireAddressNumberMatch = True
tahaCoreConflictFirstNameMax = 0.25
tahaCoreConflictLastNameMax = 0.55
tahaCoreConflictNameSimilarityMax = 0.58
tahaStrongNameAcceptNameMin = 0.95
tahaStrongNameAcceptLastMin = 0.95
tahaStrongNameAcceptFirstMin = 0.88
tahaStrongNameAcceptPositionalMin = 0.93
tahaStrongNameAcceptMuDelta = 0.28
tahaLlmContextMaxExamples = 3
tahaPersistCachesAcrossIterations = True
tahaCollectDecisionTraceCount = False
tahaPairFeatureCacheMaxSize = 250000
tahaProgressInterval = 5000
tahaUseSoftRoleScoring = False
tahaUseProvisionalContext = False
tahaUseSchemaLightClusterQuality = False
tahaMustLinkThreshold = 0.87
tahaLikelyLinkThreshold = 0.74
tahaContextOnlyThreshold = 0.58
tahaCannotLinkThreshold = 0.28
tahaLocalSupportWeight = 0.18
tahaLocalConflictWeight = 0.20
tahaSchemaLightRareTokenMaxFreq = 8
tahaContextDisagreementDelta = 0.12
# Stop Word Parameters
sigma = 12
removeDuplicateTokens = False
removeExcludedBlkTokens = True
##################################################
# Internal Parameters set by the program
##################################################
inputPrefix = ''
logFile = ''
muStart = 0.00
epsilonStart = 0.00
fatalError = False
workbook = None
worksheet = None
startRow = 0
dataList = []
# Run Statistics
refCnt = 0
tokenCnt = 0
uniqueTokenRatio = 0.00
numTokenCnt = 0
numTokenRatio = 0.00
minFreq = 0
maxFreq = 0
avgFreq = 0.00
stdFreq = 0.00
minLen = 0
maxLen = 0
avgLen = 0.00
stdDevLen = 0.00
precision = 0.00
recall = 0.00
fMeasure = 0.00
truePairs = 0
linkedPairs = 0
expectedPairs = 0
# Blocking Metrics
blockPrecision = 0.00
blockRecall = 0.00
blockFMeasure = 0.00
blockTruePairs = 0
blockCandidatePairs = 0
blockExpectedPairs = 0
###########################################
# Helper Functions
###########################################
def convertToBoolean(lineNbr, value):
    if value=='True':
        return True
    if value=='False':
        return False
    print('**Error: Invalid Boolean value in Parameter File, line:',lineNbr,'->',value)
    global fatalError
    fatalError = True
def convertToFloat(lineNbr, value):
    try:
        floatValue = float(value)
    except ValueError:
        print('**Error: Invalid floating point value in Parameter File, line:',lineNbr,'->',value)
        global fatalError
        fatalError = True
    else:
        return floatValue
def convertToInteger(lineNbr, value):
    if value.isdigit():
        return int(value)
    else:
        print('**Error: Invalid integer value in Parameter File, line:',lineNbr,'->',value)
        global fatalError
        fatalError = True
###############################################
# Main Program
###############################################
def getParms(parmFileName, logName):
    global logFile
    logFile = logName
    global fatalError
   
    validParmNames = ['inputFileName','delimiter', 'hasHeader', 'tokenizerType', 'removeDuplicateTokens',
                      'minFreqStdToken', 'minLenStdToken', 'maxFreqErrToken', 'addRefsToLinkIndex',
                      'mu', 'muIterate', 'beta', 'minBlkTokenLen', 'sigma', 'epsilon', 'epsilonIterate',
                      'excludeNumericBlocks', 'removeExcludedBlkTokens', 'runClusterMetrics', 'createFinalJoin',
                      'blockByPairs', 'comparator', 'truthFileName', 'matrixNumTokenRule', 'matrixInitialRule',
                      'runGlobalCorrection', 'runIterationProfile', 'blockCorrection', 'blockCorrectionDetail',
                      'globalCorrectionDetail', 'learnTokenVariants', 'blockingMode', 'embeddingModelName',
                      'embeddingBlockMethod', 'embeddingBlockEps', 'embeddingBlockMinSamples', 'embeddingTopK',
                      'embeddingBatchSize', 'embeddingNameWeight', 'embeddingDevice',
                      'tahaRejectMuRatio', 'tahaReviewUpperDelta', 'tahaContextThreshold',
                      'tahaMinNameSimilarity', 'tahaMinLastNameSimilarity',
                      'tahaEnableNameHardReject', 'tahaAliasFile', 'tahaLlmDecisionFile',
                      'tahaUseOpenAIReview', 'tahaOpenAIModel', 'tahaOpenAIApiKeyEnv',
                      'tahaOpenAIBaseURL', 'tahaOpenAITimeoutSec', 'tahaOpenAIMaxRetries',
                      'tahaOpenAITemperature', 'tahaOpenAIMaxOutputTokens',
                      'tahaOpenAIReviewMaxPairs', 'tahaUseAsyncOpenAIReview',
                      'tahaOpenAIAsyncWorkers', 'tahaUseAnchorBatchReview',
                      'tahaAnchorBatchSize', 'tahaUseDeterministicRules',
                      'tahaPoisonAddressMinSimilarity', 'tahaPoisonFirstNameMax',
                      'tahaPoisonLastNameMax', 'tahaPoisonNameSimilarityMax',
                      'tahaPoisonRequireAddressNumberMatch',
                      'tahaCoreConflictFirstNameMax', 'tahaCoreConflictLastNameMax',
                      'tahaCoreConflictNameSimilarityMax',
                      'tahaStrongNameAcceptNameMin', 'tahaStrongNameAcceptLastMin',
                      'tahaStrongNameAcceptFirstMin', 'tahaStrongNameAcceptPositionalMin',
                      'tahaStrongNameAcceptMuDelta', 'tahaLlmContextMaxExamples',
                      'tahaPersistCachesAcrossIterations', 'tahaCollectDecisionTraceCount',
                      'tahaPairFeatureCacheMaxSize', 'tahaProgressInterval',
                      'tahaUseSoftRoleScoring', 'tahaUseProvisionalContext',
                      'tahaUseSchemaLightClusterQuality', 'tahaMustLinkThreshold',
                      'tahaLikelyLinkThreshold', 'tahaContextOnlyThreshold',
                      'tahaCannotLinkThreshold', 'tahaLocalSupportWeight',
                      'tahaLocalConflictWeight', 'tahaSchemaLightRareTokenMaxFreq',
                      'tahaContextDisagreementDelta']
    parmFile = open(parmFileName,'r')
    parms = {}
    lineNbr = 0
    while True:
        line = (parmFile.readline()).strip()
        lineNbr +=1
        if line=='':
            break
        # Skip comment lines in parameter file
        if  line.startswith('#'):
            continue
        if line.find('=') < 0:
            print('**Error: Parameter line does not have equal sign, line:',lineNbr,'->',line)
            fatalError = True
            continue
        part = line.split('=')
        parmName = part[0].strip()
        if parmName not in validParmNames:
            print('**Error: Invalid Parameter Name in Parameter File, line:',lineNbr,'->',parmName)
            fatalError = True
        parmValue = part[1].strip()
        if parmName=='inputFileName':
            global inputFileName
            inputFileName = parmValue
            global inputPrefix
            inputPrefix = os.path.splitext(os.path.basename(inputFileName))[0]
            continue
        if parmName=='delimiter':
            global delimiter
            if ',;:|\t'.find(parmValue)>=0:
                delimiter = parmValue
                continue
            else:
                print('**Error: Invalid delimiter in Parameter File, line:',lineNbr,'->',parmName)
                sys.exit()                             
        if parmName=='hasHeader':
            global hasHeader
            hasHeader = convertToBoolean(lineNbr, parmValue)
            continue    
        if parmName=='tokenizerType':
            global tokenizerType
            tokenizerType = parmValue
            continue
        if parmName=='removeDuplicateTokens':
            global removeDuplicateTokens
            removeDuplicateTokens = convertToBoolean(lineNbr, parmValue)
            continue
        if parmName=='runGlobalCorrection':
            global runGlobalCorrection
            runGlobalCorrection = convertToBoolean(lineNbr, parmValue)
            continue
        if parmName=='globalCorrectionDetail':
            global globalCorrectionDetail
            globalCorrectionDetail = convertToBoolean(lineNbr, parmValue)
            continue        
        if parmName=='learnTokenVariants':
            global learnTokenVariants
            learnTokenVariants = convertToBoolean(lineNbr, parmValue)
            continue
        if parmName=='runIterationProfile':
            global runIterationProfile
            runIterationProfile = convertToBoolean(lineNbr, parmValue)
            continue
        if parmName=='addRefsToLinkIndex':
            global addRefsToLinkIndex
            addRefsToLinkIndex = convertToBoolean(lineNbr, parmValue)
            continue            
        if parmName=='minFreqStdToken':
            global minFreqStdToken
            minFreqStdToken = convertToInteger(lineNbr, parmValue)
            continue
        if parmName=='minLenStdToken':
            global minLenStdToken
            minLenStdToken = convertToInteger(lineNbr, parmValue)
            continue
        if parmName=='maxFreqErrToken':
            global maxFreqErrToken
            maxFreqErrToken = convertToInteger(lineNbr, parmValue)
            continue            
        if parmName=='matrixNumTokenRule':
            global matrixNumTokenRule
            matrixNumTokenRule = convertToBoolean(lineNbr, parmValue)
            continue
        if parmName=='matrixInitialRule':
            global matrixInitialRule
            matrixInitialRule = convertToBoolean(lineNbr, parmValue)
            continue
        if parmName=='tahaRejectMuRatio':
            global tahaRejectMuRatio
            tahaRejectMuRatio = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='tahaReviewUpperDelta':
            global tahaReviewUpperDelta
            tahaReviewUpperDelta = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='tahaContextThreshold':
            global tahaContextThreshold
            tahaContextThreshold = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='tahaMinNameSimilarity':
            global tahaMinNameSimilarity
            tahaMinNameSimilarity = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='tahaMinLastNameSimilarity':
            global tahaMinLastNameSimilarity
            tahaMinLastNameSimilarity = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='tahaEnableNameHardReject':
            global tahaEnableNameHardReject
            tahaEnableNameHardReject = convertToBoolean(lineNbr, parmValue)
            continue
        if parmName=='tahaAliasFile':
            global tahaAliasFile
            tahaAliasFile = parmValue.strip()
            continue
        if parmName=='tahaLlmDecisionFile':
            global tahaLlmDecisionFile
            tahaLlmDecisionFile = parmValue.strip()
            continue
        if parmName=='tahaUseOpenAIReview':
            global tahaUseOpenAIReview
            tahaUseOpenAIReview = convertToBoolean(lineNbr, parmValue)
            continue
        if parmName=='tahaOpenAIModel':
            global tahaOpenAIModel
            tahaOpenAIModel = parmValue.strip()
            continue
        if parmName=='tahaOpenAIApiKeyEnv':
            global tahaOpenAIApiKeyEnv
            tahaOpenAIApiKeyEnv = parmValue.strip()
            continue
        if parmName=='tahaOpenAIBaseURL':
            global tahaOpenAIBaseURL
            tahaOpenAIBaseURL = parmValue.strip()
            continue
        if parmName=='tahaOpenAITimeoutSec':
            global tahaOpenAITimeoutSec
            tahaOpenAITimeoutSec = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='tahaOpenAIMaxRetries':
            global tahaOpenAIMaxRetries
            tahaOpenAIMaxRetries = convertToInteger(lineNbr, parmValue)
            continue
        if parmName=='tahaOpenAITemperature':
            global tahaOpenAITemperature
            tahaOpenAITemperature = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='tahaOpenAIMaxOutputTokens':
            global tahaOpenAIMaxOutputTokens
            tahaOpenAIMaxOutputTokens = convertToInteger(lineNbr, parmValue)
            continue
        if parmName=='tahaOpenAIReviewMaxPairs':
            global tahaOpenAIReviewMaxPairs
            tahaOpenAIReviewMaxPairs = convertToInteger(lineNbr, parmValue)
            continue
        if parmName=='tahaUseAsyncOpenAIReview':
            global tahaUseAsyncOpenAIReview
            tahaUseAsyncOpenAIReview = convertToBoolean(lineNbr, parmValue)
            continue
        if parmName=='tahaOpenAIAsyncWorkers':
            global tahaOpenAIAsyncWorkers
            tahaOpenAIAsyncWorkers = convertToInteger(lineNbr, parmValue)
            continue
        if parmName=='tahaUseAnchorBatchReview':
            global tahaUseAnchorBatchReview
            tahaUseAnchorBatchReview = convertToBoolean(lineNbr, parmValue)
            continue
        if parmName=='tahaAnchorBatchSize':
            global tahaAnchorBatchSize
            tahaAnchorBatchSize = convertToInteger(lineNbr, parmValue)
            continue
        if parmName=='tahaUseDeterministicRules':
            global tahaUseDeterministicRules
            tahaUseDeterministicRules = convertToBoolean(lineNbr, parmValue)
            continue
        if parmName=='tahaPoisonAddressMinSimilarity':
            global tahaPoisonAddressMinSimilarity
            tahaPoisonAddressMinSimilarity = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='tahaPoisonFirstNameMax':
            global tahaPoisonFirstNameMax
            tahaPoisonFirstNameMax = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='tahaPoisonLastNameMax':
            global tahaPoisonLastNameMax
            tahaPoisonLastNameMax = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='tahaPoisonNameSimilarityMax':
            global tahaPoisonNameSimilarityMax
            tahaPoisonNameSimilarityMax = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='tahaPoisonRequireAddressNumberMatch':
            global tahaPoisonRequireAddressNumberMatch
            tahaPoisonRequireAddressNumberMatch = convertToBoolean(lineNbr, parmValue)
            continue
        if parmName=='tahaCoreConflictFirstNameMax':
            global tahaCoreConflictFirstNameMax
            tahaCoreConflictFirstNameMax = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='tahaCoreConflictLastNameMax':
            global tahaCoreConflictLastNameMax
            tahaCoreConflictLastNameMax = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='tahaCoreConflictNameSimilarityMax':
            global tahaCoreConflictNameSimilarityMax
            tahaCoreConflictNameSimilarityMax = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='tahaStrongNameAcceptNameMin':
            global tahaStrongNameAcceptNameMin
            tahaStrongNameAcceptNameMin = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='tahaStrongNameAcceptLastMin':
            global tahaStrongNameAcceptLastMin
            tahaStrongNameAcceptLastMin = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='tahaStrongNameAcceptFirstMin':
            global tahaStrongNameAcceptFirstMin
            tahaStrongNameAcceptFirstMin = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='tahaStrongNameAcceptPositionalMin':
            global tahaStrongNameAcceptPositionalMin
            tahaStrongNameAcceptPositionalMin = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='tahaStrongNameAcceptMuDelta':
            global tahaStrongNameAcceptMuDelta
            tahaStrongNameAcceptMuDelta = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='tahaLlmContextMaxExamples':
            global tahaLlmContextMaxExamples
            tahaLlmContextMaxExamples = convertToInteger(lineNbr, parmValue)
            continue
        if parmName=='tahaPersistCachesAcrossIterations':
            global tahaPersistCachesAcrossIterations
            tahaPersistCachesAcrossIterations = convertToBoolean(lineNbr, parmValue)
            continue
        if parmName=='tahaCollectDecisionTraceCount':
            global tahaCollectDecisionTraceCount
            tahaCollectDecisionTraceCount = convertToBoolean(lineNbr, parmValue)
            continue
        if parmName=='tahaPairFeatureCacheMaxSize':
            global tahaPairFeatureCacheMaxSize
            tahaPairFeatureCacheMaxSize = convertToInteger(lineNbr, parmValue)
            continue
        if parmName=='tahaProgressInterval':
            global tahaProgressInterval
            tahaProgressInterval = convertToInteger(lineNbr, parmValue)
            continue
        if parmName=='tahaUseSoftRoleScoring':
            global tahaUseSoftRoleScoring
            tahaUseSoftRoleScoring = convertToBoolean(lineNbr, parmValue)
            continue
        if parmName=='tahaUseProvisionalContext':
            global tahaUseProvisionalContext
            tahaUseProvisionalContext = convertToBoolean(lineNbr, parmValue)
            continue
        if parmName=='tahaUseSchemaLightClusterQuality':
            global tahaUseSchemaLightClusterQuality
            tahaUseSchemaLightClusterQuality = convertToBoolean(lineNbr, parmValue)
            continue
        if parmName=='tahaMustLinkThreshold':
            global tahaMustLinkThreshold
            tahaMustLinkThreshold = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='tahaLikelyLinkThreshold':
            global tahaLikelyLinkThreshold
            tahaLikelyLinkThreshold = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='tahaContextOnlyThreshold':
            global tahaContextOnlyThreshold
            tahaContextOnlyThreshold = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='tahaCannotLinkThreshold':
            global tahaCannotLinkThreshold
            tahaCannotLinkThreshold = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='tahaLocalSupportWeight':
            global tahaLocalSupportWeight
            tahaLocalSupportWeight = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='tahaLocalConflictWeight':
            global tahaLocalConflictWeight
            tahaLocalConflictWeight = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='tahaSchemaLightRareTokenMaxFreq':
            global tahaSchemaLightRareTokenMaxFreq
            tahaSchemaLightRareTokenMaxFreq = convertToInteger(lineNbr, parmValue)
            continue
        if parmName=='tahaContextDisagreementDelta':
            global tahaContextDisagreementDelta
            tahaContextDisagreementDelta = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='mu':
            global mu
            mu = convertToFloat(lineNbr, parmValue)
            muStart = mu
            continue            
        if parmName=='muIterate':
            global muIterate
            muIterate = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='epsilon':
            global epsilon
            epsilon = convertToFloat(lineNbr, parmValue)
            epsilonStart = epsilon
            continue
        if parmName=='epsilonIterate':
            global epsilonIterate
            epsilonIterate = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='comparator':
            global comparator
            comparator = parmValue
            continue  
        if parmName=='beta':
            global beta
            beta = convertToInteger(lineNbr, parmValue)
            continue
        if parmName=='minBlkTokenLen':
            global minBlkTokenLen
            minBlkTokenLen = convertToInteger(lineNbr, parmValue)
            continue            
        if parmName=='excludeNumericBlocks':
            global excludeNumericBlocks
            excludeNumericBlocks = convertToBoolean(lineNbr, parmValue)
            continue            
        if parmName=='blockByPairs':
            global blockByPairs
            blockByPairs = convertToBoolean(lineNbr, parmValue)
            continue
        if parmName=='blockingMode':
            global blockingMode
            blockingMode = convertToInteger(lineNbr, parmValue)
            continue
        if parmName=='embeddingModelName':
            global embeddingModelName
            embeddingModelName = parmValue
            continue
        if parmName=='embeddingBlockMethod':
            global embeddingBlockMethod
            embeddingBlockMethod = parmValue.upper().strip()
            continue
        if parmName=='embeddingBlockEps':
            global embeddingBlockEps
            embeddingBlockEps = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='embeddingBlockMinSamples':
            global embeddingBlockMinSamples
            embeddingBlockMinSamples = convertToInteger(lineNbr, parmValue)
            continue
        if parmName=='embeddingTopK':
            global embeddingTopK
            embeddingTopK = convertToInteger(lineNbr, parmValue)
            continue
        if parmName=='embeddingBatchSize':
            global embeddingBatchSize
            embeddingBatchSize = convertToInteger(lineNbr, parmValue)
            continue
        if parmName=='embeddingNameWeight':
            global embeddingNameWeight
            embeddingNameWeight = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='embeddingDevice':
            global embeddingDevice
            embeddingDevice = parmValue.strip()
            continue
        if parmName=='removeExcludedBlkTokens':
            global removeExcludedBlkTokens
            removeExcludedBlkTokens = convertToBoolean(lineNbr, parmValue)
            continue            
        if parmName=='sigma':
            global sigma
            sigma = convertToInteger(lineNbr, parmValue)
            continue
        if parmName=='truthFileName':
            global truthFileName
            truthFileName = parmValue
            continue
        if parmName=='blockCorrection':
            global blockCorrection
            blockCorrection = convertToBoolean(lineNbr,parmValue)
            continue
        if parmName=='blockCorrectionDetail':
            global blockCorrectionDetail
            blockCorrectionDetail = convertToBoolean(lineNbr,parmValue)
            continue       
        if parmName=='workbook':
            global workbook
            workbook = parmValue
            continue
        if parmName=='worksheet':
            global worksheet
            worksheet = parmValue
            continue              
        if parmName=='startRow':
            global startRow
            startRow =  convertToInteger(lineNbr, parmValue)
            continue
        if parmName=='dataList':
            global dataList
            dataList = parmValue
            continue         
        if parmName=='refCnt':
            global refCnt
            refCnt =  convertToInteger(lineNbr, parmValue)
            continue
        if parmName=='tokenCnt':
            global tokenCnt
            tokenCnt =  convertToInteger(lineNbr, parmValue)
            continue
        if parmName=='uniqueTokenRatio':
            global uniqueTokenRatio
            uniqueTokenRatio = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='numTokenCnt':
            global numTokenCnt
            numTokenCnt =  convertToInteger(lineNbr, parmValue)
            continue
        if parmName=='numTokenRatio':
            global numTokenRatio
            numTokenRatio = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='minFreq':
            global minFreq
            minFreq =  convertToInteger(lineNbr, parmValue)
            continue
        if parmName=='maxFreq':
            global maxFreq
            maxFreq =  convertToInteger(lineNbr, parmValue)
            continue
        if parmName=='avgFreq':
            global avgFreq
            avgFreq = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='stdFreq':
            global stdFreq
            stdFreq = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='avgLen':
            global avgLen
            avgLen = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='stdDevLen':
            global stdDevLen
            stdDevLen = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='minLen':
            global minLen
            minLen =  convertToInteger(lineNbr, parmValue)
            continue
        if parmName=='maxLen':
            global maxLen
            maxLen =  convertToInteger(lineNbr, parmValue)
            continue
        if parmName=='precision':
            global precision
            precision = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='recall':
            global recall
            recall = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='fMeasure':
            global fMeasure
            fMeasure = convertToFloat(lineNbr, parmValue)
            continue
        if parmName=='truePairs':
            global truePairs
            truePairs =  convertToInteger(lineNbr, parmValue)
            continue
        if parmName=='linkedPairs':
            global linkedPairs
            linkedPairs =  convertToInteger(lineNbr, parmValue)
            continue
        if parmName=='expectedPairs':
            global expectedPairs
            expectedPairs =  convertToInteger(lineNbr, parmValue)
            continue
    ###############################################
    # End of Script cross checks
    if beta<2:
        print('**Error: beta value ', beta,' must be larger than 2')
        fatalError = True
    if sigma <= beta:
        print('**Error: sigma value ', sigma,' must be larger than beta value ', beta)
        fatalError = True
    if blockingMode not in [0,1]:
        print('**Error: blockingMode value ', blockingMode,' must be 0 or 1')
        fatalError = True
    # Validate embedding parameters only when embedding blocking is enabled.
    if blockingMode == 1:
        if embeddingModelName == '':
            print('**Error: embeddingModelName cannot be empty')
            fatalError = True
        if embeddingBlockMethod not in ['DBSCAN', 'KNN']:
            print('**Error: embeddingBlockMethod value ', embeddingBlockMethod,' must be DBSCAN or KNN')
            fatalError = True
        if embeddingBatchSize < 1:
            print('**Error: embeddingBatchSize value ', embeddingBatchSize,' must be >= 1')
            fatalError = True
        if embeddingNameWeight < 0.0 or embeddingNameWeight > 1.0:
            print('**Error: embeddingNameWeight value ', embeddingNameWeight,' must be in interval [0.00,1.00]')
            fatalError = True
        if embeddingDevice == '':
            print('**Error: embeddingDevice cannot be empty')
            fatalError = True
        if embeddingBlockMethod == 'DBSCAN':
            if embeddingBlockEps <= 0.0 or embeddingBlockEps > 2.0:
                print('**Error: embeddingBlockEps value ', embeddingBlockEps,' must be in interval (0.00,2.00]')
                fatalError = True
            if embeddingBlockMinSamples < 2:
                print('**Error: embeddingBlockMinSamples value ', embeddingBlockMinSamples,' must be >= 2')
                fatalError = True
        if embeddingBlockMethod == 'KNN':
            if embeddingTopK < 1:
                print('**Error: embeddingTopK value ', embeddingTopK,' must be >= 1')
                fatalError = True
    if mu <= 0.0 or mu > 1.00:
        print('**Error: mu value ', mu,' must be in interval (0.00,1.00]')
        fatalError = True
    if muIterate < 0.0 or muIterate > 1.00:
        print('**Error: muIterate value ', muIterate,' must be in interval (0.00,1.00]')
        fatalError = True
    if epsilon <= 0.0 or epsilon > 1.00:
        print('**Error: epsilon value ', epsilon,' must be in interval (0.00,1.00]')
        fatalError = True
    if epsilonIterate < 0.0 or epsilonIterate > 1.00:
        print('**Error: epsilonIterate value ', epsilonIterate,' must be in interval (0.00,1.00]')
        fatalError = True
    if minFreqStdToken <= maxFreqErrToken:
        print('**Error: minFreqStdToken ', minFreqStdToken,' must be greater than maxFreqErrToken', maxFreqErrToken)
        fatalError = True
    if tahaRejectMuRatio < 0.0 or tahaRejectMuRatio > 1.0:
        print('**Error: tahaRejectMuRatio value ', tahaRejectMuRatio,' must be in interval [0.00,1.00]')
        fatalError = True
    if tahaReviewUpperDelta < 0.0 or tahaReviewUpperDelta > 1.0:
        print('**Error: tahaReviewUpperDelta value ', tahaReviewUpperDelta,' must be in interval [0.00,1.00]')
        fatalError = True
    if tahaContextThreshold < 0.0 or tahaContextThreshold > 1.0:
        print('**Error: tahaContextThreshold value ', tahaContextThreshold,' must be in interval [0.00,1.00]')
        fatalError = True
    if tahaMinNameSimilarity < 0.0 or tahaMinNameSimilarity > 1.0:
        print('**Error: tahaMinNameSimilarity value ', tahaMinNameSimilarity,' must be in interval [0.00,1.00]')
        fatalError = True
    if tahaMinLastNameSimilarity < 0.0 or tahaMinLastNameSimilarity > 1.0:
        print('**Error: tahaMinLastNameSimilarity value ', tahaMinLastNameSimilarity,' must be in interval [0.00,1.00]')
        fatalError = True
    if tahaAliasFile == '':
        print('**Error: tahaAliasFile cannot be empty')
        fatalError = True
    if tahaOpenAIModel == '':
        print('**Error: tahaOpenAIModel cannot be empty')
        fatalError = True
    if tahaOpenAIApiKeyEnv == '':
        print('**Error: tahaOpenAIApiKeyEnv cannot be empty')
        fatalError = True
    if tahaOpenAIBaseURL == '':
        print('**Error: tahaOpenAIBaseURL cannot be empty')
        fatalError = True
    if tahaOpenAITimeoutSec <= 0.0 or tahaOpenAITimeoutSec > 600.0:
        print('**Error: tahaOpenAITimeoutSec value ', tahaOpenAITimeoutSec,' must be in interval (0.00,600.00]')
        fatalError = True
    if tahaOpenAIMaxRetries < 0 or tahaOpenAIMaxRetries > 10:
        print('**Error: tahaOpenAIMaxRetries value ', tahaOpenAIMaxRetries,' must be in interval [0,10]')
        fatalError = True
    if tahaOpenAITemperature < 0.0 or tahaOpenAITemperature > 2.0:
        print('**Error: tahaOpenAITemperature value ', tahaOpenAITemperature,' must be in interval [0.00,2.00]')
        fatalError = True
    if tahaOpenAIMaxOutputTokens < 16 or tahaOpenAIMaxOutputTokens > 4096:
        print('**Error: tahaOpenAIMaxOutputTokens value ', tahaOpenAIMaxOutputTokens,' must be in interval [16,4096]')
        fatalError = True
    if tahaOpenAIReviewMaxPairs < 0:
        print('**Error: tahaOpenAIReviewMaxPairs value ', tahaOpenAIReviewMaxPairs,' must be >= 0')
        fatalError = True
    if tahaOpenAIAsyncWorkers < 1 or tahaOpenAIAsyncWorkers > 128:
        print('**Error: tahaOpenAIAsyncWorkers value ', tahaOpenAIAsyncWorkers,' must be in interval [1,128]')
        fatalError = True
    if tahaAnchorBatchSize < 1 or tahaAnchorBatchSize > 50:
        print('**Error: tahaAnchorBatchSize value ', tahaAnchorBatchSize,' must be in interval [1,50]')
        fatalError = True
    if tahaPoisonAddressMinSimilarity < 0.0 or tahaPoisonAddressMinSimilarity > 1.0:
        print('**Error: tahaPoisonAddressMinSimilarity value ', tahaPoisonAddressMinSimilarity,' must be in interval [0.00,1.00]')
        fatalError = True
    if tahaPoisonFirstNameMax < 0.0 or tahaPoisonFirstNameMax > 1.0:
        print('**Error: tahaPoisonFirstNameMax value ', tahaPoisonFirstNameMax,' must be in interval [0.00,1.00]')
        fatalError = True
    if tahaPoisonLastNameMax < 0.0 or tahaPoisonLastNameMax > 1.0:
        print('**Error: tahaPoisonLastNameMax value ', tahaPoisonLastNameMax,' must be in interval [0.00,1.00]')
        fatalError = True
    if tahaPoisonNameSimilarityMax < 0.0 or tahaPoisonNameSimilarityMax > 1.0:
        print('**Error: tahaPoisonNameSimilarityMax value ', tahaPoisonNameSimilarityMax,' must be in interval [0.00,1.00]')
        fatalError = True
    if tahaCoreConflictFirstNameMax < 0.0 or tahaCoreConflictFirstNameMax > 1.0:
        print('**Error: tahaCoreConflictFirstNameMax value ', tahaCoreConflictFirstNameMax,' must be in interval [0.00,1.00]')
        fatalError = True
    if tahaCoreConflictLastNameMax < 0.0 or tahaCoreConflictLastNameMax > 1.0:
        print('**Error: tahaCoreConflictLastNameMax value ', tahaCoreConflictLastNameMax,' must be in interval [0.00,1.00]')
        fatalError = True
    if tahaCoreConflictNameSimilarityMax < 0.0 or tahaCoreConflictNameSimilarityMax > 1.0:
        print('**Error: tahaCoreConflictNameSimilarityMax value ', tahaCoreConflictNameSimilarityMax,' must be in interval [0.00,1.00]')
        fatalError = True
    if tahaStrongNameAcceptNameMin < 0.0 or tahaStrongNameAcceptNameMin > 1.0:
        print('**Error: tahaStrongNameAcceptNameMin value ', tahaStrongNameAcceptNameMin,' must be in interval [0.00,1.00]')
        fatalError = True
    if tahaStrongNameAcceptLastMin < 0.0 or tahaStrongNameAcceptLastMin > 1.0:
        print('**Error: tahaStrongNameAcceptLastMin value ', tahaStrongNameAcceptLastMin,' must be in interval [0.00,1.00]')
        fatalError = True
    if tahaStrongNameAcceptFirstMin < 0.0 or tahaStrongNameAcceptFirstMin > 1.0:
        print('**Error: tahaStrongNameAcceptFirstMin value ', tahaStrongNameAcceptFirstMin,' must be in interval [0.00,1.00]')
        fatalError = True
    if tahaStrongNameAcceptPositionalMin < 0.0 or tahaStrongNameAcceptPositionalMin > 1.0:
        print('**Error: tahaStrongNameAcceptPositionalMin value ', tahaStrongNameAcceptPositionalMin,' must be in interval [0.00,1.00]')
        fatalError = True
    if tahaStrongNameAcceptMuDelta < 0.0 or tahaStrongNameAcceptMuDelta > 1.0:
        print('**Error: tahaStrongNameAcceptMuDelta value ', tahaStrongNameAcceptMuDelta,' must be in interval [0.00,1.00]')
        fatalError = True
    if tahaLlmContextMaxExamples < 0 or tahaLlmContextMaxExamples > 50:
        print('**Error: tahaLlmContextMaxExamples value ', tahaLlmContextMaxExamples,' must be in interval [0,50]')
        fatalError = True
    if tahaPairFeatureCacheMaxSize < 0:
        print('**Error: tahaPairFeatureCacheMaxSize value ', tahaPairFeatureCacheMaxSize,' must be >= 0')
        fatalError = True
    if tahaProgressInterval < 0:
        print('**Error: tahaProgressInterval value ', tahaProgressInterval,' must be >= 0')
        fatalError = True
    if tahaMustLinkThreshold < 0.0 or tahaMustLinkThreshold > 1.0:
        print('**Error: tahaMustLinkThreshold value ', tahaMustLinkThreshold,' must be in interval [0.00,1.00]')
        fatalError = True
    if tahaLikelyLinkThreshold < 0.0 or tahaLikelyLinkThreshold > 1.0:
        print('**Error: tahaLikelyLinkThreshold value ', tahaLikelyLinkThreshold,' must be in interval [0.00,1.00]')
        fatalError = True
    if tahaContextOnlyThreshold < 0.0 or tahaContextOnlyThreshold > 1.0:
        print('**Error: tahaContextOnlyThreshold value ', tahaContextOnlyThreshold,' must be in interval [0.00,1.00]')
        fatalError = True
    if tahaCannotLinkThreshold < 0.0 or tahaCannotLinkThreshold > 1.0:
        print('**Error: tahaCannotLinkThreshold value ', tahaCannotLinkThreshold,' must be in interval [0.00,1.00]')
        fatalError = True
    if tahaLocalSupportWeight < 0.0 or tahaLocalSupportWeight > 1.0:
        print('**Error: tahaLocalSupportWeight value ', tahaLocalSupportWeight,' must be in interval [0.00,1.00]')
        fatalError = True
    if tahaLocalConflictWeight < 0.0 or tahaLocalConflictWeight > 1.0:
        print('**Error: tahaLocalConflictWeight value ', tahaLocalConflictWeight,' must be in interval [0.00,1.00]')
        fatalError = True
    if tahaSchemaLightRareTokenMaxFreq < 1:
        print('**Error: tahaSchemaLightRareTokenMaxFreq value ', tahaSchemaLightRareTokenMaxFreq,' must be >= 1')
        fatalError = True
    if tahaContextDisagreementDelta < 0.0 or tahaContextDisagreementDelta > 1.0:
        print('**Error: tahaContextDisagreementDelta value ', tahaContextDisagreementDelta,' must be in interval [0.00,1.00]')
        fatalError = True
    if tahaCannotLinkThreshold > tahaContextOnlyThreshold:
        print('**Error: tahaCannotLinkThreshold must be <= tahaContextOnlyThreshold')
        fatalError = True
    if tahaContextOnlyThreshold > tahaLikelyLinkThreshold:
        print('**Error: tahaContextOnlyThreshold must be <= tahaLikelyLinkThreshold')
        fatalError = True
    if tahaLikelyLinkThreshold > tahaMustLinkThreshold:
        print('**Error: tahaLikelyLinkThreshold must be <= tahaMustLinkThreshold')
        fatalError = True
    if fatalError:
        sys.exit()  
    return

