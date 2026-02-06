#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import math
import re
import DWM10_Parms


_digit_re = re.compile(r'\d')
_NAME_WEIGHT = 0.76


def _split_name_address_tokens(tokens):
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


def _entropy_quality(cluster):
    cluster_size = len(cluster)
    if cluster_size == 0:
        return 1.0, 0
    token_count = 0
    for j in range(0, cluster_size):
        token_count = token_count + len(cluster[j])
    if token_count == 0:
        return 1.0, 0
    base_prob = 1 / float(cluster_size)
    base = -token_count * base_prob * math.log(base_prob, 2)
    if base == 0:
        return 1.0, token_count
    entropy = 0.0
    # work on copies to avoid mutating input lists
    working = [token_list.copy() for token_list in cluster]
    for j in range(0, cluster_size - 1):
        j_list = working[j]
        for token in j_list:
            cnt = 1
            for k in range(j + 1, cluster_size):
                if token in working[k]:
                    cnt += 1
                    working[k].remove(token)
            token_prob = cnt / cluster_size
            term = -token_prob * math.log(token_prob, 2)
            entropy += term
            cnt = 0
    for token in working[cluster_size - 1]:
        token_prob = 1.0 / cluster_size
        term = -token_prob * math.log(token_prob, 2)
        entropy += term
    quality = 1.0 - entropy / base
    return quality, token_count
def calculateEntropy(cluster):
    name_cluster = []
    address_cluster = []
    name_count = 0
    address_count = 0
    for token_list in cluster:
        name_tokens, address_tokens = _split_name_address_tokens(token_list)
        name_cluster.append(name_tokens)
        address_cluster.append(address_tokens)
        name_count += len(name_tokens)
        address_count += len(address_tokens)
    name_quality, _ = _entropy_quality(name_cluster)
    address_quality, _ = _entropy_quality(address_cluster)
    if name_count > 0 and address_count > 0:
        quality = (_NAME_WEIGHT * name_quality) + ((1.0 - _NAME_WEIGHT) * address_quality)
    elif name_count > 0:
        quality = name_quality
    else:
        quality = address_quality
    return quality
