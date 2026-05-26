#!/usr/bin/env python
# coding: utf-8

# In[1]:


from textdistance import DamerauLevenshtein
import Levenshtein as lev
#from fastDamerauLevenshtein import damerauLevenshtein 
import re
import DWM10_Parms

def normalized_similarity(inRef1, inRef2, return_trace=False, trace_min_sim=0.0):
    Class = DamerauLevenshtein()

    m = len(inRef1)
    n = len(inRef2)
    score = 0.0

    if m == 0 or n == 0:
        return (score, []) if return_trace else score

    # Make ref1 the shorter list
    if m <= n:
        ref1 = inRef1
        ref2 = inRef2
    else:
        ref1 = inRef2
        ref2 = inRef1

    m = len(ref1)
    n = len(ref2)

    base = float(m * (m + 1) / 2)

    # Build m x n matrix of token similarities
    matrix = [[0.0 for _ in range(n)] for _ in range(m)]

    for j in range(m):
        token1 = ref1[j]
        for k in range(n):
            token2 = ref2[k]

            # Rule 1: Numeric token rule
            if DWM10_Parms.matrixNumTokenRule:
                if token1.isdigit() and token2.isdigit():
                    matrix[j][k] = 1.0 if token1 == token2 else 0.0
                    continue

            # Rule 2: Initial rule (length 1)
            if DWM10_Parms.matrixInitialRule:
                if len(token1) == 1 or len(token2) == 1:
                    matrix[j][k] = 1.0 if token1 == token2 else 0.0
                    continue

            # Rule 3: Damerau-Levenshtein normalized similarity
            matrix[j][k] = Class.normalized_similarity(token1, token2)

    trace = []
    step = 0

    # Greedy selection loop: pick max cell, consume its row+col
    while True:
        maxVal = -1.0
        saveJ = -1
        saveK = -1

        for j in range(m):
            for k in range(n):
                if matrix[j][k] > maxVal:
                    maxVal = matrix[j][k]
                    saveJ = j
                    saveK = k

        if maxVal < 0:
            break

        numerator = m - saveJ
        weight = float(numerator) / base
        wgtSim = maxVal * weight
        score += wgtSim

        token1 = ref1[saveJ]
        token2 = ref2[saveK]

        if return_trace and maxVal >= trace_min_sim:
            trace.append({
                "step": step,
                "token1": token1,
                "token2": token2,
                "sim": float(maxVal),
                "weight": float(weight),
                "weighted_sim": float(wgtSim),
                "row_index": int(saveJ),
                "col_index": int(saveK),
            })

        step += 1

        # consume column saveK
        for j in range(m):
            matrix[j][saveK] = -1.0
        # consume row saveJ
        for k in range(n):
            matrix[saveJ][k] = -1.0

    return (score, trace) if return_trace else score
