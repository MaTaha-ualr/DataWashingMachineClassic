#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sys
import time
from textdistance import Cosine
from textdistance import MongeElkan
import DWM10_Parms
import DWM65_ScoringMatrixStd
import DWM66_ScoringMatrixKris
import DWM67_Tahacomparator
def linkBlockPairs(blockPairList, refDict, tokenFreqDict): 
    logFile = DWM10_Parms.logFile
    sigma = DWM10_Parms.sigma
    removeDuplicateTokens = DWM10_Parms.removeDuplicateTokens
    removeExcludedBlkTokens = DWM10_Parms.removeExcludedBlkTokens
    minBlkTokenLen = DWM10_Parms.minBlkTokenLen
    excludeNumericBlocks = DWM10_Parms.excludeNumericBlocks
    print('\n>>Starting DWM55')
    print('\n>>Starting DWM55', file=logFile)
    print('Sigma =', sigma)
    print('Sigma =', sigma, file=logFile)    
    print('Remove Duplicate Tokens =', removeDuplicateTokens)
    print('Remove Duplicate Tokens =', removeDuplicateTokens, file=logFile)
    print('Remove Excluded Block Tokens =', removeExcludedBlkTokens)
    print('Remove Excluded Block Tokens =', removeExcludedBlkTokens, file=logFile)
    # Define nested function for removing stop words
    def removeStopWords(tokenList):
        newList = []
        #print('-- tokenList', tokenList)
        for token in tokenList:
            tokenLen = len(token)
            includeToken = True
            freq = tokenFreqDict[token]
            if freq>=sigma:
                includeToken = False
                #print('-- sigma rule', token, freq)
            if removeExcludedBlkTokens:
                if tokenLen < minBlkTokenLen:
                    includeToken = False
                    #print('-- min len rule', token, tokenLen)
                if token.isdigit() and excludeNumericBlocks:
                    includeToken = False
                    #print('-- number rule', token)
            if removeDuplicateTokens and (token in newList):
                includeToken = False
                #print('-- duplicate token rule', token)
            if includeToken:
                newList.append(token)
       # print('-- newList', newList)
        return newList
    # end of nexted fucntion
    # Check for valid comparator
    validComparator = False
    comparator = DWM10_Parms.comparator
    if comparator == 'MongeElkan':
        Class = MongeElkan()
        validComparator = True
    if comparator == 'Cosine':
        Class = Cosine()
        validComparator = True
    if comparator == 'ScoringMatrixStd':
        Class = DWM65_ScoringMatrixStd
        validComparator = True
    if comparator == 'ScoringMatrixKris':
        Class = DWM66_ScoringMatrixKris
        validComparator = True        
    if comparator == 'TahaComparator' or comparator == 'ScoringMatrixTaha':
        Class = DWM67_Tahacomparator
        validComparator = True
        Class.start_run(tokenFreqDict)
    if not validComparator:
        print('**Error: Invalid Comparator Value in Parms File', comparator)
        sys.exit()
    mu = DWM10_Parms.mu
    progressInterval = int(getattr(DWM10_Parms, 'tahaProgressInterval', 5000))
    if progressInterval < 0:
        progressInterval = 0
    useAsyncOpenAI = False
    if comparator == 'TahaComparator' or comparator == 'ScoringMatrixTaha':
        useAsyncOpenAI = bool(getattr(DWM10_Parms, 'tahaUseAsyncOpenAIReview', True))
        if useAsyncOpenAI:
            asyncWorkers = int(getattr(DWM10_Parms, 'tahaOpenAIAsyncWorkers', 8))
            print('TahaComparator Async OpenAI Review = True, workers =', asyncWorkers)
            print('TahaComparator Async OpenAI Review = True, workers =', asyncWorkers, file=logFile)
            useAnchorBatch = bool(getattr(DWM10_Parms, 'tahaUseAnchorBatchReview', False))
            anchorBatchSize = int(getattr(DWM10_Parms, 'tahaAnchorBatchSize', 6))
            print('TahaComparator Anchor Batch Review =', useAnchorBatch, ', batch_size =', anchorBatchSize)
            print('TahaComparator Anchor Batch Review =', useAnchorBatch, ', batch_size =', anchorBatchSize, file=logFile)
        else:
            print('TahaComparator Async OpenAI Review = False')
            print('TahaComparator Async OpenAI Review = False', file=logFile)
    linkedPairList = []
    tahaDecisionList = []
    blockPairListLen = len(blockPairList)
    startTime = time.time()

    def _progress_line(done, total, linked, elapsed_sec):
        if total <= 0:
            pct = 100.0
            bar = '[' + ('#' * 24) + ']'
        else:
            pct = (100.0 * done) / total
            bar_len = 24
            filled = int((done * bar_len) / total)
            bar = '[' + ('#' * filled) + ('-' * (bar_len - filled)) + ']'
        return (
            f'DWM55 progress {bar} {done}/{total} '
            f'({pct:5.1f}%) linked={linked} elapsed_sec={round(elapsed_sec, 1)}'
        )

    for j in range(0, blockPairListLen):
        pair = blockPairList[j]
        refIDs = pair.split('|')
        refID1 = refIDs[0]
        refID2 = refIDs[1]
        tokenList1 = removeStopWords(refDict[refID1])
        tokenList2 = removeStopWords(refDict[refID2])
        if comparator == 'TahaComparator' or comparator == 'ScoringMatrixTaha':
            decision = Class.compare_pair(
                refID1,
                refID2,
                tokenList1[:],
                tokenList2[:],
                mu=mu,
                fullTokenList1=refDict[refID1],
                fullTokenList2=refDict[refID2],
                defer_openai=True
            )
            tahaDecisionList.append(decision)
        else:
            result = Class.normalized_similarity(tokenList1[:],tokenList2[:])
            if result >= mu:
                linkedPairList.append((refID1,refID2))
        if progressInterval > 0 and (j + 1) % progressInterval == 0:
            elapsed = time.time() - startTime
            if comparator == 'TahaComparator' or comparator == 'ScoringMatrixTaha':
                currentLinked = sum(1 for d in tahaDecisionList if d.get('link', False))
            else:
                currentLinked = len(linkedPairList)
            progress_message = _progress_line(j + 1, blockPairListLen, currentLinked, elapsed)
            print(progress_message)
            print(progress_message, file=logFile)

    if blockPairListLen > 0 and (progressInterval <= 0 or (blockPairListLen % progressInterval) != 0):
        elapsed = time.time() - startTime
        if comparator == 'TahaComparator' or comparator == 'ScoringMatrixTaha':
            currentLinked = sum(1 for d in tahaDecisionList if d.get('link', False))
        else:
            currentLinked = len(linkedPairList)
        progress_message = _progress_line(blockPairListLen, blockPairListLen, currentLinked, elapsed)
        print(progress_message)
        print(progress_message, file=logFile)

    if comparator == 'TahaComparator' or comparator == 'ScoringMatrixTaha':
        print('DWM55: applying provisional cluster context pass...')
        print('DWM55: applying provisional cluster context pass...', file=logFile)
        Class.apply_provisional_context_pass(tahaDecisionList, refDict)

        pending_reviews = sum(1 for d in tahaDecisionList if d.get('final_decision', '') == 'pending_llm')
        if pending_reviews > 0:
            if useAsyncOpenAI:
                print('DWM55: resolving OpenAI reviews asynchronously...')
                print('DWM55: resolving OpenAI reviews asynchronously...', file=logFile)
                Class.resolve_pending_reviews_async(tahaDecisionList, refDict, logFile=logFile)
            else:
                print('DWM55: resolving OpenAI reviews with single worker...')
                print('DWM55: resolving OpenAI reviews with single worker...', file=logFile)
                original_workers = getattr(DWM10_Parms, 'tahaOpenAIAsyncWorkers', 8)
                original_anchor_batch = getattr(DWM10_Parms, 'tahaUseAnchorBatchReview', False)
                DWM10_Parms.tahaOpenAIAsyncWorkers = 1
                DWM10_Parms.tahaUseAnchorBatchReview = False
                try:
                    Class.resolve_pending_reviews_async(tahaDecisionList, refDict, logFile=logFile)
                finally:
                    DWM10_Parms.tahaOpenAIAsyncWorkers = original_workers
                    DWM10_Parms.tahaUseAnchorBatchReview = original_anchor_batch
        for decision in tahaDecisionList:
            Class.record_decision(decision)
            if decision['link']:
                linkedPairList.append((decision['refID1'], decision['refID2']))
    print('Number of Pairs Linked =', len(linkedPairList), 'at mu=', mu)
    print('Number of Pairs Linked =', len(linkedPairList), 'at mu=', mu, file=logFile)
    if comparator == 'TahaComparator' or comparator == 'ScoringMatrixTaha':
        stats = Class.get_run_stats()
        print('TahaComparator Auto-Reject Pairs =', stats.get('auto_reject_pairs', 0))
        print('TahaComparator Auto-Reject Pairs =', stats.get('auto_reject_pairs', 0), file=logFile)
        print('TahaComparator LLM-Review Pairs =', stats.get('llm_review_pairs', 0))
        print('TahaComparator LLM-Review Pairs =', stats.get('llm_review_pairs', 0), file=logFile)
        print('TahaComparator Rule-Reject Pairs =', stats.get('rule_reject_pairs', 0))
        print('TahaComparator Rule-Reject Pairs =', stats.get('rule_reject_pairs', 0), file=logFile)
        print('TahaComparator Rule-Accept Pairs =', stats.get('rule_accept_pairs', 0))
        print('TahaComparator Rule-Accept Pairs =', stats.get('rule_accept_pairs', 0), file=logFile)
        print('TahaComparator Rule Poison Reject Pairs =', stats.get('rule_poison_reject_pairs', 0))
        print('TahaComparator Rule Poison Reject Pairs =', stats.get('rule_poison_reject_pairs', 0), file=logFile)
        print('TahaComparator Rule Name Conflict Reject Pairs =', stats.get('rule_name_conflict_reject_pairs', 0))
        print('TahaComparator Rule Name Conflict Reject Pairs =', stats.get('rule_name_conflict_reject_pairs', 0), file=logFile)
        print('TahaComparator Rule Strong Name Accept Pairs =', stats.get('rule_strong_name_accept_pairs', 0))
        print('TahaComparator Rule Strong Name Accept Pairs =', stats.get('rule_strong_name_accept_pairs', 0), file=logFile)
        print('TahaComparator Pending-LLM Pairs =', stats.get('pending_llm_pairs', 0))
        print('TahaComparator Pending-LLM Pairs =', stats.get('pending_llm_pairs', 0), file=logFile)
        print('TahaComparator Context Pairs (>= threshold) =', stats.get('context_pairs', 0))
        print('TahaComparator Context Pairs (>= threshold) =', stats.get('context_pairs', 0), file=logFile)
        print('TahaComparator OpenAI Review Requests =', stats.get('openai_review_requests', 0))
        print('TahaComparator OpenAI Review Requests =', stats.get('openai_review_requests', 0), file=logFile)
        print('TahaComparator OpenAI Review Accept =', stats.get('openai_review_accept', 0))
        print('TahaComparator OpenAI Review Accept =', stats.get('openai_review_accept', 0), file=logFile)
        print('TahaComparator OpenAI Review Reject =', stats.get('openai_review_reject', 0))
        print('TahaComparator OpenAI Review Reject =', stats.get('openai_review_reject', 0), file=logFile)
        print('TahaComparator OpenAI Review Errors =', stats.get('openai_review_errors', 0))
        print('TahaComparator OpenAI Review Errors =', stats.get('openai_review_errors', 0), file=logFile)
        print('TahaComparator OpenAI Chat Fallback Used =', stats.get('openai_chat_fallback_used', 0))
        print('TahaComparator OpenAI Chat Fallback Used =', stats.get('openai_chat_fallback_used', 0), file=logFile)
        print('TahaComparator File Review Accept =', stats.get('file_review_accept', 0))
        print('TahaComparator File Review Accept =', stats.get('file_review_accept', 0), file=logFile)
        print('TahaComparator File Review Reject =', stats.get('file_review_reject', 0))
        print('TahaComparator File Review Reject =', stats.get('file_review_reject', 0), file=logFile)
        print('TahaComparator LLM Reject Overrides =', stats.get('llm_reject_overrides', 0))
        print('TahaComparator LLM Reject Overrides =', stats.get('llm_reject_overrides', 0), file=logFile)
        print('TahaComparator Context Pass Revisions =', stats.get('context_pass_revisions', 0))
        print('TahaComparator Context Pass Revisions =', stats.get('context_pass_revisions', 0), file=logFile)
        print('TahaComparator Context Disagreement Reviews =', stats.get('context_disagreement_reviews', 0))
        print('TahaComparator Context Disagreement Reviews =', stats.get('context_disagreement_reviews', 0), file=logFile)
    return linkedPairList
