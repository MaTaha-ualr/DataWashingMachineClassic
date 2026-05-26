#!/usr/bin/env python
# coding: utf-8

import csv
from collections import defaultdict
import concurrent.futures
import json
import os
import re
import threading
import time
import urllib.error
import urllib.request
from textdistance import DamerauLevenshtein
import DWM10_Parms

_digit_re = re.compile(r'\d')
_distance = DamerauLevenshtein()

_run_decisions = []
_run_stats = {}
_llm_decision_map = {}
_llm_decision_file_loaded = None
_openai_cache = {}
_openai_cache_lock = threading.Lock()
_context_by_ref = {}
_openai_key_missing_logged = False
_pair_feature_cache = {}
_run_token_freq_dict = {}
_dataset_stats = {}
_name_alias_equiv = None
_alias_file_loaded = None
_alias_load_warning_logged = False

_name_suffix_tokens = {
    'JR', 'SR', 'II', 'III', 'IV', 'V',
    'MD', 'M', 'D', 'PHD', 'DDS', 'DVM'
}


def _parm(name, default):
    return getattr(DWM10_Parms, name, default)


def _clamp(value, low=0.0, high=1.0):
    try:
        value = float(value)
    except Exception:
        value = low
    if value < low:
        return float(low)
    if value > high:
        return float(high)
    return float(value)


def _as_bool(value, default=False):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if value is None:
        return default
    v = str(value).strip().lower()
    if v in ('1', 'true', 'yes', 'y', 'on'):
        return True
    if v in ('0', 'false', 'no', 'n', 'off', ''):
        return False
    return default


def _pair_key(refID1, refID2):
    if refID1 <= refID2:
        return f'{refID1}|{refID2}'
    return f'{refID2}|{refID1}'


def _feature_cache_key(refID1, refID2, tokenList1, tokenList2, fullTokenList1, fullTokenList2):
    return (
        _pair_key(refID1, refID2),
        tuple(tokenList1),
        tuple(tokenList2),
        tuple(fullTokenList1) if fullTokenList1 is not None else tuple(tokenList1),
        tuple(fullTokenList2) if fullTokenList2 is not None else tuple(tokenList2),
    )


def _normalize_llm_decision(value):
    if value is None:
        return ''
    v = str(value).strip().lower()
    if v in ('1', 'true', 'yes', 'y', 'accept', 'accepted', 'match', 'same', 'same_person', 'link', 'linked'):
        return 'accept'
    if v in ('0', 'false', 'no', 'n', 'reject', 'rejected', 'nonmatch', 'different', 'not_same', 'fp'):
        return 'reject'
    return ''


def _load_llm_decisions_if_needed():
    global _llm_decision_file_loaded
    global _llm_decision_map

    llm_file = str(_parm('tahaLlmDecisionFile', '')).strip()
    if llm_file == _llm_decision_file_loaded:
        return

    _llm_decision_file_loaded = llm_file
    _llm_decision_map = {}

    if llm_file == '':
        return

    if not os.path.exists(llm_file):
        print(f'Warning: tahaLlmDecisionFile not found: {llm_file}')
        return

    loaded = 0
    with open(llm_file, 'r', newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        if reader.fieldnames:
            col_map = {c.lower().strip(): c for c in reader.fieldnames}
            ref1_col = None
            ref2_col = None
            decision_col = None

            for candidate in ('refid1', 'id1', 'left_refid', 'record1', 'recordid1'):
                if candidate in col_map:
                    ref1_col = col_map[candidate]
                    break
            for candidate in ('refid2', 'id2', 'right_refid', 'record2', 'recordid2'):
                if candidate in col_map:
                    ref2_col = col_map[candidate]
                    break
            for candidate in ('decision', 'llm_decision', 'label', 'match', 'same_person'):
                if candidate in col_map:
                    decision_col = col_map[candidate]
                    break

            if ref1_col and ref2_col and decision_col:
                for row in reader:
                    refID1 = str(row.get(ref1_col, '')).strip()
                    refID2 = str(row.get(ref2_col, '')).strip()
                    decision = _normalize_llm_decision(row.get(decision_col, ''))
                    if refID1 and refID2 and decision:
                        _llm_decision_map[_pair_key(refID1, refID2)] = decision
                        loaded += 1
                print(f'Loaded {loaded} LLM decisions from {llm_file}')
                return

    with open(llm_file, 'r', newline='', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 3:
                continue
            refID1 = str(row[0]).strip()
            refID2 = str(row[1]).strip()
            decision = _normalize_llm_decision(row[2])
            if refID1 and refID2 and decision:
                _llm_decision_map[_pair_key(refID1, refID2)] = decision
                loaded += 1
    print(f'Loaded {loaded} LLM decisions from {llm_file}')


def _load_name_alias_equiv():
    global _name_alias_equiv
    global _alias_file_loaded
    global _alias_load_warning_logged

    alias_file = str(_parm('tahaAliasFile', 'alias.dat')).strip()
    if alias_file == '':
        alias_file = 'alias.dat'

    if _name_alias_equiv is not None and _alias_file_loaded == alias_file:
        return _name_alias_equiv

    _alias_file_loaded = alias_file
    _name_alias_equiv = {}

    if not os.path.exists(alias_file):
        if not _alias_load_warning_logged:
            print(f'Warning: alias file not found for Taha name matching: {alias_file}')
            _alias_load_warning_logged = True
        return _name_alias_equiv

    with open(alias_file, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            line = line.strip()
            if line == '' or line.startswith('!!'):
                continue
            parts = [p.strip().upper() for p in line.split('\t') if p.strip() != '']
            if len(parts) < 2:
                continue
            canonical = parts[0]
            alias = parts[1]
            if canonical == '' or alias == '':
                continue

            if canonical not in _name_alias_equiv:
                _name_alias_equiv[canonical] = set([canonical])
            if alias not in _name_alias_equiv:
                _name_alias_equiv[alias] = set([alias])

            _name_alias_equiv[canonical].add(alias)
            _name_alias_equiv[alias].add(canonical)
            _name_alias_equiv[canonical].update(_name_alias_equiv[alias])
            _name_alias_equiv[alias].update(_name_alias_equiv[canonical])

    return _name_alias_equiv


def _nickname_equivalent(token1, token2):
    t1 = str(token1).strip().upper()
    t2 = str(token2).strip().upper()
    if t1 == '' or t2 == '':
        return False
    if t1 == t2:
        return True
    alias_map = _load_name_alias_equiv()
    s1 = alias_map.get(t1, set([t1]))
    s2 = alias_map.get(t2, set([t2]))
    return (t1 in s2) or (t2 in s1) or (len(s1.intersection(s2)) > 0)


def _name_token_similarity(token1, token2):
    t1 = str(token1).strip().upper()
    t2 = str(token2).strip().upper()
    if t1 == '' or t2 == '':
        return 0.0
    if t1 == t2:
        return 1.0
    if _nickname_equivalent(t1, t2):
        return 1.0
    if len(t1) == 1 and len(t2) > 1:
        return 0.92 if t2.startswith(t1) else 0.0
    if len(t2) == 1 and len(t1) > 1:
        return 0.92 if t1.startswith(t2) else 0.0
    return float(_distance.normalized_similarity(t1, t2))


def _openai_api_key():
    env_name = str(_parm('tahaOpenAIApiKeyEnv', 'OPENAI_API_KEY')).strip()
    if env_name == '':
        env_name = 'OPENAI_API_KEY'
    return os.environ.get(env_name, '').strip()


def _openai_enabled():
    return _as_bool(_parm('tahaUseOpenAIReview', False), default=False)


def _openai_review_limit_reached():
    max_reviews = int(_parm('tahaOpenAIReviewMaxPairs', 0))
    if max_reviews <= 0:
        return False
    return _run_stats.get('openai_review_requests', 0) >= max_reviews


def _http_post_json(url, headers, payload, timeout_sec):
    body = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(url, data=body, headers=headers, method='POST')
    with urllib.request.urlopen(req, timeout=timeout_sec) as response:
        text = response.read().decode('utf-8')
        return response.getcode(), text


def _extract_text_from_responses_api(data):
    output_text = data.get('output_text', '')
    if isinstance(output_text, str) and output_text.strip() != '':
        return output_text

    texts = []
    for item in data.get('output', []):
        for content in item.get('content', []):
            ctype = content.get('type', '')
            if ctype in ('output_text', 'text'):
                txt = content.get('text', '')
                if isinstance(txt, str) and txt.strip() != '':
                    texts.append(txt)
    return '\n'.join(texts).strip()


def _extract_text_from_chat_api(data):
    choices = data.get('choices', [])
    if not choices:
        return ''
    msg = choices[0].get('message', {})
    content = msg.get('content', '')
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        texts = []
        for item in content:
            txt = item.get('text', '')
            if isinstance(txt, str) and txt.strip() != '':
                texts.append(txt)
        return '\n'.join(texts).strip()
    return ''


def _extract_json_block(text):
    if not isinstance(text, str):
        return None
    stripped = text.strip()
    if stripped == '':
        return None
    try:
        return json.loads(stripped)
    except Exception:
        pass

    start = stripped.find('{')
    end = stripped.rfind('}')
    if start >= 0 and end > start:
        candidate = stripped[start:end + 1]
        try:
            return json.loads(candidate)
        except Exception:
            return None
    return None


def _decision_from_structured(parsed, raw_text):
    if isinstance(parsed, dict):
        decision = _normalize_llm_decision(parsed.get('decision', ''))
        reason = str(parsed.get('reason', '')).strip()
        confidence = parsed.get('confidence', '')
        try:
            confidence = float(confidence)
        except Exception:
            confidence = ''
        if decision in ('accept', 'reject'):
            return decision, reason, confidence

    plain_decision = _normalize_llm_decision(raw_text)
    if plain_decision in ('accept', 'reject'):
        return plain_decision, '', ''
    return '', '', ''


def _try_openai_responses_api(base_url, headers, model, temperature, max_output_tokens, system_text, user_text, timeout_sec):
    payload = {
        'model': model,
        'temperature': temperature,
        'max_output_tokens': max_output_tokens,
        'input': [
            {'role': 'system', 'content': [{'type': 'text', 'text': system_text}]},
            {'role': 'user', 'content': [{'type': 'text', 'text': user_text}]},
        ],
    }
    _, body = _http_post_json(f'{base_url}/responses', headers, payload, timeout_sec)
    data = json.loads(body)
    response_text = _extract_text_from_responses_api(data)
    parsed = _extract_json_block(response_text)
    decision, reason, confidence = _decision_from_structured(parsed, response_text)
    return decision, response_text, reason, confidence


def _try_openai_chat_api(base_url, headers, model, temperature, max_output_tokens, system_text, user_text, timeout_sec):
    payload = {
        'model': model,
        'temperature': temperature,
        'max_tokens': max_output_tokens,
        'response_format': {'type': 'json_object'},
        'messages': [
            {'role': 'system', 'content': system_text},
            {'role': 'user', 'content': user_text},
        ],
    }
    _, body = _http_post_json(f'{base_url}/chat/completions', headers, payload, timeout_sec)
    data = json.loads(body)
    response_text = _extract_text_from_chat_api(data)
    parsed = _extract_json_block(response_text)
    decision, reason, confidence = _decision_from_structured(parsed, response_text)
    return decision, response_text, reason, confidence


def _format_context_for_prompt(refID):
    max_examples = int(_parm('tahaLlmContextMaxExamples', 3))
    if max_examples < 0:
        max_examples = 0
    records = _context_by_ref.get(refID, [])
    if not records:
        return 'None'

    selected = records[-max_examples:] if max_examples > 0 else []
    if not selected:
        return 'None'

    lines = []
    for item in selected:
        lines.append(
            (
                f"- Similarity={item.get('similarity', 0.0):.3f} "
                f"Ref={item.get('other_ref_id', '')} "
                f"Name='{item.get('other_name', '')}' "
                f"Address='{item.get('other_address', '')}'"
            )
        )
    return '\n'.join(lines)


def _record_context(refID1, refID2, fullTokenList1, fullTokenList2, similarity):
    max_keep = int(_parm('tahaLlmContextMaxExamples', 3))
    if max_keep < 1:
        return

    name1_tokens, addr1_tokens = split_name_address(fullTokenList1)
    name2_tokens, addr2_tokens = split_name_address(fullTokenList2)

    name1 = ' '.join(name1_tokens).strip()
    name2 = ' '.join(name2_tokens).strip()
    addr1 = ' '.join(addr1_tokens).strip()
    addr2 = ' '.join(addr2_tokens).strip()

    if refID1 not in _context_by_ref:
        _context_by_ref[refID1] = []
    if refID2 not in _context_by_ref:
        _context_by_ref[refID2] = []

    _context_by_ref[refID1].append({
        'other_ref_id': refID2,
        'other_name': name2,
        'other_address': addr2,
        'similarity': float(similarity),
    })
    _context_by_ref[refID2].append({
        'other_ref_id': refID1,
        'other_name': name1,
        'other_address': addr1,
        'similarity': float(similarity),
    })

    if len(_context_by_ref[refID1]) > max_keep:
        _context_by_ref[refID1] = _context_by_ref[refID1][-max_keep:]
    if len(_context_by_ref[refID2]) > max_keep:
        _context_by_ref[refID2] = _context_by_ref[refID2][-max_keep:]


def _openai_prompt_payload(refID1, refID2, fullTokenList1, fullTokenList2, similarity, name_details, mu, local_context_text=''):
    name1_tokens, addr1_tokens = split_name_address(fullTokenList1)
    name2_tokens, addr2_tokens = split_name_address(fullTokenList2)

    name1 = ' '.join(name1_tokens).strip()
    name2 = ' '.join(name2_tokens).strip()
    addr1 = ' '.join(addr1_tokens).strip()
    addr2 = ' '.join(addr2_tokens).strip()

    context1 = _format_context_for_prompt(refID1)
    context2 = _format_context_for_prompt(refID2)

    system_text = (
        "You are a careful entity-resolution clerical reviewer.\n"
        "Goal: decide whether two records refer to the same person.\n"
        "Decision policy:\n"
        "1) Core name agreement (last name + first name/initial/nickname) is strong evidence.\n"
        "2) A person can have multiple addresses over time; address mismatch alone is weak evidence.\n"
        "3) High-confidence context records are supportive only; lack of context overlap is not a standalone rejection reason.\n"
        "4) Middle names/initials may be missing or extra.\n"
        "5) Reject when there is clear contradiction in core identity signals.\n"
        "6) If evidence is mixed but core name signals are strong, prefer ACCEPT with lower confidence.\n"
        "Return JSON only: {\"decision\":\"ACCEPT|REJECT\",\"reason\":\"short text\",\"confidence\":0.0}."
    )

    user_text = (
        f"Threshold mu={mu:.3f}. Current similarity={similarity:.3f}.\n"
        f"Name metrics: name={name_details.get('name_similarity', 0.0):.3f}, "
        f"first={name_details.get('first_name_similarity', 0.0):.3f}, "
        f"middle={name_details.get('middle_name_similarity', 0.0):.3f}, "
        f"last={name_details.get('last_name_similarity', 0.0):.3f}, "
        f"positional={name_details.get('name_positional_similarity', 0.0):.3f}.\n"
        "Reminder: address overlap is optional and should not be required for acceptance.\n\n"
        f"Record A ({refID1})\n"
        f"Name: {name1}\n"
        f"Address: {addr1}\n"
        f"All tokens: {', '.join(fullTokenList1)}\n\n"
        f"Record B ({refID2})\n"
        f"Name: {name2}\n"
        f"Address: {addr2}\n"
        f"All tokens: {', '.join(fullTokenList2)}\n\n"
        f"High-confidence context for {refID1}:\n{context1}\n\n"
        f"High-confidence context for {refID2}:\n{context2}\n\n"
    )
    if str(local_context_text).strip() != '':
        user_text += f"Local provisional cluster context:\n{local_context_text}\n\n"
    user_text += "Output JSON only."
    return system_text, user_text


def _call_openai_for_decision(refID1, refID2, fullTokenList1, fullTokenList2, similarity, name_details, mu, local_context_text=''):
    pair_key = _pair_key(refID1, refID2)
    base_url = str(_parm('tahaOpenAIBaseURL', 'https://api.openai.com/v1')).strip().rstrip('/')
    model = str(_parm('tahaOpenAIModel', 'gpt-4.1')).strip()
    openai_cache_key = (pair_key, model, base_url)
    with _openai_cache_lock:
        if openai_cache_key in _openai_cache:
            return _openai_cache[openai_cache_key]

    api_key = _openai_api_key()
    if api_key == '':
        result = ('', 'none', 'OPENAI_API_KEY missing', '', '')
        with _openai_cache_lock:
            _openai_cache[openai_cache_key] = result
        return result

    timeout_sec = float(_parm('tahaOpenAITimeoutSec', 45))
    max_retries = int(_parm('tahaOpenAIMaxRetries', 2))
    max_output_tokens = int(_parm('tahaOpenAIMaxOutputTokens', 120))
    temperature = float(_parm('tahaOpenAITemperature', 0.0))

    system_text, user_text = _openai_prompt_payload(
        refID1, refID2, fullTokenList1, fullTokenList2, similarity, name_details, mu, local_context_text=local_context_text
    )

    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json',
    }

    last_error = ''
    response_text = ''
    llm_reason = ''
    llm_confidence = ''

    for attempt in range(max_retries + 1):
        primary_error = ''
        fallback_error = ''
        try:
            decision, response_text, llm_reason, llm_confidence = _try_openai_responses_api(
                base_url,
                headers,
                model,
                temperature,
                max_output_tokens,
                system_text,
                user_text,
                timeout_sec
            )
            if decision in ('accept', 'reject'):
                result = (decision, 'openai_live', response_text, llm_reason, llm_confidence)
                with _openai_cache_lock:
                    _openai_cache[openai_cache_key] = result
                return result
            primary_error = f'Unparseable /responses output: {response_text[:240]}'
        except urllib.error.HTTPError as e:
            err_body = ''
            try:
                err_body = e.read().decode('utf-8')
            except Exception:
                err_body = str(e)
            primary_error = f'HTTP {e.code} /responses: {err_body[:240]}'
        except Exception as e:
            primary_error = f'/responses error: {str(e)}'

        try:
            decision, response_text, llm_reason, llm_confidence = _try_openai_chat_api(
                base_url,
                headers,
                model,
                temperature,
                max_output_tokens,
                system_text,
                user_text,
                timeout_sec
            )
            if decision in ('accept', 'reject'):
                result = (decision, 'openai_live_chat_fallback', response_text, llm_reason, llm_confidence)
                with _openai_cache_lock:
                    _openai_cache[openai_cache_key] = result
                return result
            fallback_error = f'Unparseable /chat/completions output: {response_text[:240]}'
        except urllib.error.HTTPError as e:
            err_body = ''
            try:
                err_body = e.read().decode('utf-8')
            except Exception:
                err_body = str(e)
            fallback_error = f'HTTP {e.code} /chat/completions: {err_body[:240]}'
        except Exception as e:
            fallback_error = f'/chat/completions error: {str(e)}'

        last_error = ' | '.join([x for x in [primary_error, fallback_error] if x])

        if attempt < max_retries:
            time.sleep(0.8 * (attempt + 1))

    result = ('', 'openai_error', last_error, '', '')
    with _openai_cache_lock:
        _openai_cache[openai_cache_key] = result
    return result


def _openai_batch_prompt_payload(anchor_refID, anchor_tokens, candidate_rows):
    anchor_name_tokens, anchor_addr_tokens = split_name_address(anchor_tokens)
    anchor_name = ' '.join(anchor_name_tokens).strip()
    anchor_addr = ' '.join(anchor_addr_tokens).strip()
    anchor_context = _format_context_for_prompt(anchor_refID)

    candidate_blocks = []
    for pos, item in enumerate(candidate_rows, start=1):
        ref_id = str(item.get('refID2', '')).strip()
        tokens = item.get('full2', [])
        if not isinstance(tokens, list):
            tokens = []
        name_tokens, addr_tokens = split_name_address(tokens)
        name_text = ' '.join(name_tokens).strip()
        addr_text = ' '.join(addr_tokens).strip()
        metrics = item.get('name_details', {})
        candidate_context = _format_context_for_prompt(ref_id)
        local_context = str(item.get('local_context', '')).strip()
        candidate_blocks.append(
            (
                f"Candidate {pos} RefID={ref_id}\n"
                f"Similarity={float(item.get('similarity', 0.0)):.3f}, "
                f"mu={float(item.get('mu', _parm('mu', 0.5))):.3f}\n"
                f"Name metrics: name={float(metrics.get('name_similarity', 0.0)):.3f}, "
                f"first={float(metrics.get('first_name_similarity', 0.0)):.3f}, "
                f"middle={float(metrics.get('middle_name_similarity', 0.0)):.3f}, "
                f"last={float(metrics.get('last_name_similarity', 0.0)):.3f}, "
                f"positional={float(metrics.get('name_positional_similarity', 0.0)):.3f}\n"
                f"Name: {name_text}\n"
                f"Address: {addr_text}\n"
                f"All tokens: {', '.join([str(t) for t in tokens])}\n"
                f"High-confidence context for {ref_id}:\n{candidate_context}\n"
                + (f"Local provisional cluster context:\n{local_context}" if local_context != '' else '')
            )
        )

    expected_refs = [str(item.get('refID2', '')).strip() for item in candidate_rows]
    expected_refs = [r for r in expected_refs if r != '']

    system_text = (
        "You are a careful entity-resolution clerical reviewer.\n"
        "Goal: compare one anchor record against multiple candidate records and decide if each candidate is the same person as anchor.\n"
        "Decision policy:\n"
        "1) Core name agreement (last name + first name/initial/nickname) is strong evidence.\n"
        "2) A person can have multiple addresses over time; address mismatch alone is weak evidence.\n"
        "3) High-confidence context records are supportive only; lack of context overlap is not a standalone rejection reason.\n"
        "4) Middle names/initials may be missing or extra.\n"
        "5) Reject when there is clear contradiction in core identity signals.\n"
        "6) If evidence is mixed but core name signals are strong, prefer ACCEPT with lower confidence.\n"
        "Return JSON only in this format:\n"
        "{\"decisions\":[{\"ref_id\":\"<candidate ref id>\",\"decision\":\"ACCEPT|REJECT\",\"reason\":\"short text\",\"confidence\":0.0}]}"
    )

    user_text = (
        f"Anchor RefID={anchor_refID}\n"
        f"Anchor name: {anchor_name}\n"
        f"Anchor address: {anchor_addr}\n"
        f"Anchor all tokens: {', '.join([str(t) for t in anchor_tokens])}\n"
        f"High-confidence context for {anchor_refID}:\n{anchor_context}\n\n"
        "Candidates to evaluate (each independently vs anchor):\n\n"
        + "\n\n".join(candidate_blocks)
        + "\n\n"
        + "Required candidates: "
        + ", ".join(expected_refs)
        + "\nOutput JSON only."
    )
    return system_text, user_text, expected_refs


def _batch_decisions_from_structured(parsed, raw_text, expected_ref_ids):
    expected = set()
    for rid in expected_ref_ids:
        txt = str(rid).strip()
        if txt != '':
            expected.add(txt)

    decisions = {}

    def _add(ref_id, decision, reason='', confidence=''):
        ref_txt = str(ref_id).strip()
        if ref_txt == '' or ref_txt not in expected:
            return
        norm_decision = _normalize_llm_decision(decision)
        if norm_decision not in ('accept', 'reject'):
            return
        try:
            conf = float(confidence)
        except Exception:
            conf = ''
        decisions[ref_txt] = (norm_decision, str(reason).strip(), conf)

    def _from_item(item):
        if not isinstance(item, dict):
            return
        ref_id = (
            item.get('ref_id', '')
            or item.get('refid', '')
            or item.get('id', '')
            or item.get('candidate_ref_id', '')
            or item.get('candidate', '')
        )
        _add(ref_id, item.get('decision', ''), item.get('reason', ''), item.get('confidence', ''))

    if isinstance(parsed, dict):
        if isinstance(parsed.get('decisions', None), list):
            for item in parsed.get('decisions', []):
                _from_item(item)
        elif isinstance(parsed.get('results', None), list):
            for item in parsed.get('results', []):
                _from_item(item)
        else:
            for key, value in parsed.items():
                if key in ('decision', 'reason', 'confidence', 'decisions', 'results'):
                    continue
                if isinstance(value, dict):
                    _add(key, value.get('decision', ''), value.get('reason', ''), value.get('confidence', ''))
                else:
                    _add(key, value, '', '')
    elif isinstance(parsed, list):
        for item in parsed:
            _from_item(item)

    if decisions:
        return decisions

    if isinstance(raw_text, str):
        for line in raw_text.splitlines():
            line = line.strip()
            if line == '':
                continue
            for ref_id in expected:
                if not line.startswith(ref_id):
                    continue
                trailing = line[len(ref_id):].strip(" \t:,-")
                token = trailing.split()[0] if trailing else ''
                _add(ref_id, token, '', '')
                break

    return decisions


def _try_openai_responses_api_batch(
    base_url,
    headers,
    model,
    temperature,
    max_output_tokens,
    system_text,
    user_text,
    timeout_sec,
    expected_ref_ids
):
    payload = {
        'model': model,
        'temperature': temperature,
        'max_output_tokens': max_output_tokens,
        'input': [
            {'role': 'system', 'content': [{'type': 'text', 'text': system_text}]},
            {'role': 'user', 'content': [{'type': 'text', 'text': user_text}]},
        ],
    }
    _, body = _http_post_json(f'{base_url}/responses', headers, payload, timeout_sec)
    data = json.loads(body)
    response_text = _extract_text_from_responses_api(data)
    parsed = _extract_json_block(response_text)
    decisions = _batch_decisions_from_structured(parsed, response_text, expected_ref_ids)
    return decisions, response_text


def _try_openai_chat_api_batch(
    base_url,
    headers,
    model,
    temperature,
    max_output_tokens,
    system_text,
    user_text,
    timeout_sec,
    expected_ref_ids
):
    payload = {
        'model': model,
        'temperature': temperature,
        'max_tokens': max_output_tokens,
        'response_format': {'type': 'json_object'},
        'messages': [
            {'role': 'system', 'content': system_text},
            {'role': 'user', 'content': user_text},
        ],
    }
    _, body = _http_post_json(f'{base_url}/chat/completions', headers, payload, timeout_sec)
    data = json.loads(body)
    response_text = _extract_text_from_chat_api(data)
    parsed = _extract_json_block(response_text)
    decisions = _batch_decisions_from_structured(parsed, response_text, expected_ref_ids)
    return decisions, response_text


def _call_openai_for_anchor_batch(anchor_refID, anchor_tokens, candidate_rows):
    base_url = str(_parm('tahaOpenAIBaseURL', 'https://api.openai.com/v1')).strip().rstrip('/')
    model = str(_parm('tahaOpenAIModel', 'gpt-4.1')).strip()

    results_by_idx = {}
    pending = []

    for item in candidate_rows:
        ref_id = str(item.get('refID2', '')).strip()
        if ref_id == '':
            results_by_idx[item.get('idx', -1)] = ('', 'openai_error', 'Missing candidate refID2 in anchor batch', '', '')
            continue
        pair_key = _pair_key(anchor_refID, ref_id)
        cache_key = (pair_key, model, base_url)
        with _openai_cache_lock:
            cached = _openai_cache.get(cache_key, None)
        if cached is not None:
            results_by_idx[item.get('idx', -1)] = cached
        else:
            row_copy = dict(item)
            row_copy['pair_key'] = pair_key
            row_copy['cache_key'] = cache_key
            pending.append(row_copy)

    if not pending:
        return results_by_idx

    api_key = _openai_api_key()
    if api_key == '':
        missing_key_result = ('', 'none', 'OPENAI_API_KEY missing', '', '')
        with _openai_cache_lock:
            for item in pending:
                _openai_cache[item['cache_key']] = missing_key_result
        for item in pending:
            results_by_idx[item['idx']] = missing_key_result
        return results_by_idx

    timeout_sec = float(_parm('tahaOpenAITimeoutSec', 45))
    max_retries = int(_parm('tahaOpenAIMaxRetries', 2))
    configured_output = int(_parm('tahaOpenAIMaxOutputTokens', 120))
    max_output_tokens = max(configured_output, min(4096, 64 + (len(pending) * 56)))
    temperature = float(_parm('tahaOpenAITemperature', 0.0))

    system_text, user_text, expected_ref_ids = _openai_batch_prompt_payload(
        anchor_refID, anchor_tokens, pending
    )

    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json',
    }

    decision_map = {}
    source = ''
    response_text = ''
    last_error = ''

    for attempt in range(max_retries + 1):
        primary_error = ''
        fallback_error = ''
        try:
            decisions, response_text = _try_openai_responses_api_batch(
                base_url,
                headers,
                model,
                temperature,
                max_output_tokens,
                system_text,
                user_text,
                timeout_sec,
                expected_ref_ids
            )
            if len(decisions) > 0:
                decision_map = decisions
                source = 'openai_live_batch'
                break
            primary_error = f'Unparseable /responses output: {response_text[:240]}'
        except urllib.error.HTTPError as e:
            err_body = ''
            try:
                err_body = e.read().decode('utf-8')
            except Exception:
                err_body = str(e)
            primary_error = f'HTTP {e.code} /responses: {err_body[:240]}'
        except Exception as e:
            primary_error = f'/responses error: {str(e)}'

        try:
            decisions, response_text = _try_openai_chat_api_batch(
                base_url,
                headers,
                model,
                temperature,
                max_output_tokens,
                system_text,
                user_text,
                timeout_sec,
                expected_ref_ids
            )
            if len(decisions) > 0:
                decision_map = decisions
                source = 'openai_live_chat_batch_fallback'
                break
            fallback_error = f'Unparseable /chat/completions output: {response_text[:240]}'
        except urllib.error.HTTPError as e:
            err_body = ''
            try:
                err_body = e.read().decode('utf-8')
            except Exception:
                err_body = str(e)
            fallback_error = f'HTTP {e.code} /chat/completions: {err_body[:240]}'
        except Exception as e:
            fallback_error = f'/chat/completions error: {str(e)}'

        last_error = ' | '.join([x for x in [primary_error, fallback_error] if x])
        if attempt < max_retries:
            time.sleep(0.8 * (attempt + 1))

    with _openai_cache_lock:
        for item in pending:
            ref_id = str(item.get('refID2', '')).strip()
            idx = item['idx']
            cache_key = item['cache_key']
            if ref_id in decision_map:
                decision, reason, confidence = decision_map[ref_id]
                result = (decision, source, response_text, reason, confidence)
            else:
                result = ('', 'openai_error', last_error if last_error != '' else 'Missing decision in batch response', '', '')
            _openai_cache[cache_key] = result
            results_by_idx[idx] = result

    return results_by_idx


def _token_similarity(token1, token2):
    token1 = str(token1).strip()
    token2 = str(token2).strip()

    if token1 == '' or token2 == '':
        return 0.0

    if DWM10_Parms.matrixNumTokenRule:
        if token1.isdigit() and token2.isdigit():
            return 1.0 if token1 == token2 else 0.0

    if DWM10_Parms.matrixInitialRule:
        if len(token1) == 1 or len(token2) == 1:
            return 1.0 if token1 == token2 else 0.0

    if token1 == token2:
        return 1.0

    if len(token1) == 1 and len(token2) > 1:
        return 0.85 if token2.startswith(token1) else 0.0
    if len(token2) == 1 and len(token1) > 1:
        return 0.85 if token1.startswith(token2) else 0.0

    return float(_distance.normalized_similarity(token1, token2))


def normalized_similarity(inRef1, inRef2, return_trace=False, trace_min_sim=0.0):
    m = len(inRef1)
    n = len(inRef2)
    score = 0.0

    if m == 0 or n == 0:
        return (score, []) if return_trace else score

    if m <= n:
        ref1 = inRef1
        ref2 = inRef2
    else:
        ref1 = inRef2
        ref2 = inRef1

    m = len(ref1)
    n = len(ref2)
    base = float(m * (m + 1) / 2)
    matrix = [[0.0 for _ in range(n)] for _ in range(m)]

    for j in range(m):
        token1 = ref1[j]
        for k in range(n):
            token2 = ref2[k]
            matrix[j][k] = _token_similarity(token1, token2)

    trace = []
    step = 0
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

        weight = float(m - saveJ) / base
        wgtSim = maxVal * weight
        score += wgtSim

        if return_trace and maxVal >= trace_min_sim:
            trace.append({
                'step': step,
                'token1': ref1[saveJ],
                'token2': ref2[saveK],
                'sim': float(maxVal),
                'weight': float(weight),
                'weighted_sim': float(wgtSim),
                'row_index': int(saveJ),
                'col_index': int(saveK),
            })

        step += 1

        for jj in range(m):
            matrix[jj][saveK] = -1.0
        for kk in range(n):
            matrix[saveJ][kk] = -1.0

    return (score, trace) if return_trace else score


def split_name_address(tokens):
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


def _clean_name_tokens(name_tokens):
    cleaned = []
    for token in name_tokens:
        t = str(token).strip().upper()
        if t == '':
            continue
        if t in _name_suffix_tokens:
            continue
        cleaned.append(t)
    return cleaned


def _clean_address_tokens(address_tokens):
    cleaned = []
    for token in address_tokens:
        t = str(token).strip().upper()
        if t == '':
            continue
        cleaned.append(t)
    return cleaned


def _extract_address_number(address_tokens):
    for token in address_tokens:
        tok = str(token).strip().upper()
        if tok == '':
            continue
        digits = ''.join([c for c in tok if c.isdigit()])
        if digits != '':
            return digits
    return ''


def _positional_token_similarity(tokens1, tokens2, similarity_func=None):
    if not tokens1 and not tokens2:
        return 1.0
    if not tokens1 or not tokens2:
        return 0.0

    if similarity_func is None:
        similarity_func = _token_similarity

    if len(tokens1) <= len(tokens2):
        shorter = tokens1
        longer = tokens2
    else:
        shorter = tokens2
        longer = tokens1

    used = set()
    total = 0.0

    for i, token_s in enumerate(shorter):
        best = 0.0
        best_j = -1
        for j, token_l in enumerate(longer):
            if j in used:
                continue
            token_sim = similarity_func(token_s, token_l)
            pos_weight = 1.0 / (1.0 + abs(i - j))
            sim = token_sim * pos_weight
            if sim > best:
                best = sim
                best_j = j
        if best_j >= 0:
            used.add(best_j)
        total += best

    return total / float(max(len(tokens1), len(tokens2)))


def name_similarity_details(tokens1, tokens2):
    name1_raw, _ = split_name_address(tokens1)
    name2_raw, _ = split_name_address(tokens2)
    name1 = _clean_name_tokens(name1_raw)
    name2 = _clean_name_tokens(name2_raw)

    first1 = name1[0] if len(name1) > 0 else ''
    first2 = name2[0] if len(name2) > 0 else ''

    if len(name1) == 0:
        last1 = ''
        middle1 = []
    elif len(name1) == 1:
        last1 = name1[0]
        middle1 = []
    else:
        last1 = name1[-1]
        middle1 = name1[1:-1]

    if len(name2) == 0:
        last2 = ''
        middle2 = []
    elif len(name2) == 1:
        last2 = name2[0]
        middle2 = []
    else:
        last2 = name2[-1]
        middle2 = name2[1:-1]

    first_sim = _name_token_similarity(first1, first2) if first1 and first2 else 0.0
    last_sim = _name_token_similarity(last1, last2) if last1 and last2 else 0.0

    if not middle1 and not middle2:
        middle_sim = 1.0
    elif middle1 and middle2:
        middle_sim = _positional_token_similarity(middle1, middle2, similarity_func=_name_token_similarity)
    else:
        middle_sim = 0.75

    full_name_sim = _positional_token_similarity(name1, name2, similarity_func=_name_token_similarity)
    name_sim = (0.45 * last_sim) + (0.35 * first_sim) + (0.20 * middle_sim)

    return {
        'name_tokens1': name1,
        'name_tokens2': name2,
        'first_name_similarity': float(first_sim),
        'middle_name_similarity': float(middle_sim),
        'last_name_similarity': float(last_sim),
        'name_positional_similarity': float(full_name_sim),
        'name_similarity': float(name_sim),
    }


def address_similarity_details(tokens1, tokens2):
    _, addr1_raw = split_name_address(tokens1)
    _, addr2_raw = split_name_address(tokens2)
    addr1 = _clean_address_tokens(addr1_raw)
    addr2 = _clean_address_tokens(addr2_raw)

    if not addr1 and not addr2:
        addr_sim = 0.0
        number_sim = 0.0
    elif not addr1 or not addr2:
        addr_sim = 0.0
        number_sim = 0.0
    else:
        addr_sim = _positional_token_similarity(addr1, addr2, similarity_func=_token_similarity)
        number_tokens1 = [t for t in addr1 if _digit_re.search(t)]
        number_tokens2 = [t for t in addr2 if _digit_re.search(t)]
        if number_tokens1 and number_tokens2:
            number_sim = _positional_token_similarity(number_tokens1, number_tokens2, similarity_func=_token_similarity)
        else:
            number_sim = 0.0

    number1 = _extract_address_number(addr1)
    number2 = _extract_address_number(addr2)
    number_match = (number1 != '' and number2 != '' and number1 == number2)

    return {
        'address_tokens1': addr1,
        'address_tokens2': addr2,
        'address_similarity': float(addr_sim),
        'address_number_similarity': float(number_sim),
        'address_number_match': bool(number_match),
    }


def _first_digit_index(tokens):
    for idx, token in enumerate(tokens):
        if _digit_re.search(str(token)):
            return idx
    return len(tokens)


def _normalize_tokens(tokens):
    return [str(token).strip().upper() for token in tokens if str(token).strip() != '']


def _infer_soft_token_roles(tokens):
    import math
    normalized = _normalize_tokens(tokens)
    first_digit_idx = _first_digit_index(normalized)
    stats = _dataset_stats
    log_max = max(1.0, stats.get('log_max_freq', 1.0))
    p50 = max(1, stats.get('freq_p50', 1))
    p90 = max(1, stats.get('freq_p90', 10))
    role_rows = []

    for idx, token in enumerate(normalized):
        freq = int(_run_token_freq_dict.get(token, 1)) if _run_token_freq_dict else 1
        has_digit = bool(_digit_re.search(token))
        alpha = token.isalpha()
        mixed = has_digit and any(c.isalpha() for c in token)
        short = len(token) <= 2
        before_digit = idx < first_digit_idx

        # --- frequency-derived rarity signal (0 = most common, 1 = unique) ---
        rarity = 1.0 - (math.log1p(freq) / log_max)

        # --- position signal (0.0 at end, 1.0 at start) ---
        token_count = max(1, len(normalized))
        position_signal = 1.0 - (float(idx) / token_count)

        # --- role accumulators derived from data signals ---
        identity = 0.0
        context = 0.0
        numeric = 0.0
        volatile = 0.0

        # positional: before first digit = identity region, after = context
        if before_digit:
            identity += 0.5 * position_signal
        else:
            context += 0.5 * (1.0 - position_signal)

        # character composition
        if has_digit:
            numeric += 0.65
            context += 0.15
        elif alpha:
            if before_digit:
                identity += 0.35
            else:
                context += 0.25

        # frequency-derived: rare tokens are identity-like, common are context/noise
        if alpha:
            identity += 0.35 * rarity
            context += 0.20 * (1.0 - rarity)
            if freq >= p90:
                volatile += 0.25
                identity -= 0.10
        elif not has_digit:
            volatile += 0.20

        # short tokens are volatile (initials, state codes, suffixes)
        if short:
            volatile += 0.15
            if before_digit and alpha:
                identity += 0.05

        # mixed alphanumeric (apartment numbers, unit codes)
        if mixed:
            volatile += 0.15
            context += 0.10

        # long rare alphabetic tokens are strong identity signals
        if len(token) >= 6 and alpha and freq <= p50:
            identity += 0.15 * rarity

        # name suffixes (JR, SR, MD, etc.)
        if token in _name_suffix_tokens:
            volatile += 0.20
            identity -= 0.10

        # normalize to probabilities
        identity = max(0.0, identity)
        context = max(0.0, context)
        numeric = max(0.0, numeric)
        volatile = max(0.0, volatile)
        total = identity + context + numeric + volatile
        if total <= 0.0:
            identity = 0.25
            context = 0.25
            numeric = 0.25
            volatile = 0.25
            total = 1.0

        role_rows.append({
            'token': token,
            'identity_like': identity / total,
            'location_like': context / total,
            'numeric_like': numeric / total,
            'volatile_or_noise': volatile / total,
            'freq': freq,
            'before_digit': before_digit,
        })

    return role_rows


def _role_weight_map(tokens, role_key):
    role_rows = _infer_soft_token_roles(tokens)
    token_map = {}
    for row in role_rows:
        token = row['token']
        value = float(row.get(role_key, 0.0))
        if token not in token_map or value > token_map[token]:
            token_map[token] = value
    return token_map


def _weighted_overlap_from_maps(map1, map2):
    if not map1 and not map2:
        return 0.0
    keys = set(map1.keys()).union(set(map2.keys()))
    numerator = 0.0
    denominator = 0.0
    for key in keys:
        v1 = float(map1.get(key, 0.0))
        v2 = float(map2.get(key, 0.0))
        numerator += min(v1, v2)
        denominator += max(v1, v2)
    if denominator <= 0.0:
        return 0.0
    return float(numerator / denominator)


def _soft_role_evidence(tokens1, tokens2, name_details, address_details, similarity):
    import math

    if not _as_bool(_parm('tahaUseSoftRoleScoring', False), default=False):
        identity_evidence = _clamp(
            (0.72 * float(name_details.get('name_similarity', 0.0))) +
            (0.18 * float(name_details.get('last_name_similarity', 0.0))) +
            (0.10 * float(name_details.get('name_positional_similarity', 0.0)))
        )
        context_evidence = _clamp(
            (0.75 * float(address_details.get('address_similarity', 0.0))) +
            (0.25 * float(address_details.get('address_number_similarity', 0.0)))
        )
        contradiction_score = _clamp(
            max(
                0.0,
                (0.60 * max(0.0, 0.55 - float(name_details.get('last_name_similarity', 0.0)))) +
                (0.40 * max(0.0, 0.35 - float(name_details.get('first_name_similarity', 0.0))))
            )
        )
        return {
            'identity_evidence_score': float(identity_evidence),
            'context_evidence_score': float(context_evidence),
            'contradiction_score': float(contradiction_score),
            'base_edge_score': float(_clamp(similarity)),
            'identity_overlap': 0.0,
            'context_overlap': 0.0,
            'numeric_overlap': 0.0,
            'rare_identity_bonus': 0.0,
        }

    # --- soft-role overlap from data-derived token roles ---
    identity_map_1 = _role_weight_map(tokens1, 'identity_like')
    identity_map_2 = _role_weight_map(tokens2, 'identity_like')
    context_map_1 = _role_weight_map(tokens1, 'location_like')
    context_map_2 = _role_weight_map(tokens2, 'location_like')
    numeric_map_1 = _role_weight_map(tokens1, 'numeric_like')
    numeric_map_2 = _role_weight_map(tokens2, 'numeric_like')

    identity_overlap = _weighted_overlap_from_maps(identity_map_1, identity_map_2)
    context_overlap = _weighted_overlap_from_maps(context_map_1, context_map_2)
    numeric_overlap = _weighted_overlap_from_maps(numeric_map_1, numeric_map_2)

    # rare identity bonus: shared tokens that are rare in the dataset
    stats = _dataset_stats
    p50 = max(1, stats.get('freq_p50', 1))
    log_max = max(1.0, stats.get('log_max_freq', 1.0))
    shared_identity_tokens = set(identity_map_1.keys()).intersection(set(identity_map_2.keys()))
    rare_identity_sum = 0.0
    for token in shared_identity_tokens:
        freq = int(_run_token_freq_dict.get(token, 1)) if _run_token_freq_dict else 1
        if freq <= p50:
            rare_identity_sum += 1.0 - (math.log1p(freq) / log_max)
    rare_identity_bonus = min(1.0, rare_identity_sum / 2.0)

    # --- evidence channels: blend structured similarity with soft-role overlap ---
    name_sim = float(name_details.get('name_similarity', 0.0))
    last_sim = float(name_details.get('last_name_similarity', 0.0))
    first_sim = float(name_details.get('first_name_similarity', 0.0))
    positional_sim = float(name_details.get('name_positional_similarity', 0.0))
    addr_sim = float(address_details.get('address_similarity', 0.0))
    addr_num_sim = float(address_details.get('address_number_similarity', 0.0))

    # identity evidence: average of structured name signals and soft-role overlap
    structured_identity = (name_sim + last_sim + positional_sim) / 3.0
    identity_evidence = _clamp(
        (structured_identity + identity_overlap + rare_identity_bonus) / 3.0
        if identity_overlap > 0.0 or rare_identity_bonus > 0.0
        else structured_identity
    )

    # context evidence: average of structured address signals and soft-role overlap
    structured_context = (addr_sim + addr_num_sim) / 2.0
    context_evidence = _clamp(
        (structured_context + context_overlap + numeric_overlap) / 3.0
        if context_overlap > 0.0 or numeric_overlap > 0.0
        else structured_context
    )

    # contradiction: measures how much name evidence disagrees
    # uses complement of name similarities — high when names are dissimilar
    last_gap = max(0.0, 1.0 - last_sim)
    first_gap = max(0.0, 1.0 - first_sim)
    name_conflict = (last_gap + first_gap) / 2.0
    # contradiction is amplified when context is strong but identity is weak
    context_identity_gap = max(0.0, context_evidence - identity_evidence)
    contradiction_score = _clamp(
        name_conflict * (0.5 + 0.5 * context_identity_gap)
    )

    # --- base edge score: self-weighted combination ---
    # each channel's weight is proportional to how much evidence it carries
    id_strength = identity_evidence
    ctx_strength = context_evidence
    num_strength = numeric_overlap
    sim_strength = similarity
    total_strength = id_strength + ctx_strength + num_strength + sim_strength
    if total_strength > 0.0:
        base_edge_score = _clamp(
            ((id_strength * identity_evidence) +
             (ctx_strength * context_evidence) +
             (num_strength * numeric_overlap) +
             (sim_strength * similarity)) / total_strength
            - 0.5 * contradiction_score * context_identity_gap
        )
    else:
        base_edge_score = _clamp(similarity)

    return {
        'identity_evidence_score': float(identity_evidence),
        'context_evidence_score': float(context_evidence),
        'contradiction_score': float(contradiction_score),
        'base_edge_score': float(base_edge_score),
        'identity_overlap': float(identity_overlap),
        'context_overlap': float(context_overlap),
        'numeric_overlap': float(numeric_overlap),
        'rare_identity_bonus': float(rare_identity_bonus),
    }


def _soft_identity_bridge(name_details, evidence):
    """Identity bridge: rescue a pair that fell below mu but has strong identity.

    The principle: if identity evidence clearly dominates context evidence,
    the pair deserves review rather than outright rejection.
    """
    if not _as_bool(_parm('tahaUseSoftRoleScoring', False), default=False):
        return False

    identity_score = float(evidence.get('identity_evidence_score', 0.0))
    context_score = float(evidence.get('context_evidence_score', 0.0))
    contradiction = float(evidence.get('contradiction_score', 0.0))

    # identity must clearly dominate context, with low contradiction
    return identity_score > context_score and contradiction < identity_score


def _context_only_overlap_flag(name_details, evidence):
    """Flag pairs where overlap is driven entirely by context, not identity.

    The principle: context evidence dominates and identity evidence is weak.
    """
    if not _as_bool(_parm('tahaUseSoftRoleScoring', False), default=False):
        return False

    context_score = float(evidence.get('context_evidence_score', 0.0))
    identity_score = float(evidence.get('identity_evidence_score', 0.0))
    contradiction_score = float(evidence.get('contradiction_score', 0.0))

    # context must dominate identity, with meaningful contradiction
    return (
        context_score > identity_score and
        contradiction_score > identity_score
    )


def _edge_type_from_scores(score, identity_score, context_score, contradiction_score):
    must_link = float(_parm('tahaMustLinkThreshold', 0.87))
    likely_link = float(_parm('tahaLikelyLinkThreshold', 0.74))
    context_only = float(_parm('tahaContextOnlyThreshold', 0.58))
    cannot_link = float(_parm('tahaCannotLinkThreshold', 0.28))

    if contradiction_score >= 0.65 and identity_score < max(0.52, context_score):
        return 'cannot_link'
    if score >= must_link and contradiction_score < 0.35:
        return 'must_link'
    if score >= likely_link and identity_score >= max(0.50, context_score - 0.05):
        return 'likely_link'
    if context_score >= max(context_only, identity_score + 0.08):
        return 'context_only_overlap'
    if score <= cannot_link or contradiction_score >= 0.55:
        return 'cannot_link'
    return 'likely_nonmatch'


def _thresholds(mu):
    low_ratio = float(_parm('tahaRejectMuRatio', 0.25))
    review_delta = float(_parm('tahaReviewUpperDelta', 0.10))
    context_threshold = float(_parm('tahaContextThreshold', 0.90))

    low_ratio = max(0.0, min(1.0, low_ratio))
    review_delta = max(0.0, min(1.0, review_delta))
    context_threshold = max(0.0, min(1.0, context_threshold))

    low_cutoff = max(0.0, min(1.0, mu * low_ratio))
    review_upper = max(low_cutoff, min(1.0, mu + review_delta))
    return low_cutoff, review_upper, context_threshold


def band_label(similarity, mu=None):
    if mu is None:
        mu = float(DWM10_Parms.mu)
    low_cutoff, review_upper, _ = _thresholds(mu)
    if similarity < low_cutoff:
        return 'reject'
    if similarity <= review_upper:
        return 'llm_review'
    return 'accept'


def _name_rule_reject(name_details):
    if not _as_bool(_parm('tahaEnableNameHardReject', False), default=False):
        return False, ''

    min_name_similarity = float(_parm('tahaMinNameSimilarity', 0.55))
    min_last_name_similarity = float(_parm('tahaMinLastNameSimilarity', 0.50))

    name_token_cnt_1 = len(name_details['name_tokens1'])
    name_token_cnt_2 = len(name_details['name_tokens2'])

    first_sim = name_details['first_name_similarity']
    middle_sim = name_details['middle_name_similarity']
    last_sim = name_details['last_name_similarity']
    name_sim = name_details['name_similarity']

    if name_token_cnt_1 == 0 or name_token_cnt_2 == 0:
        return False, ''

    if (last_sim < max(0.15, min_last_name_similarity)) and (name_sim < max(0.30, min_name_similarity)) and (first_sim < 0.30):
        return True, 'name_similarity_below_threshold'

    return False, ''


def _deterministic_rule_decision(similarity, name_details, address_details, mu):
    if not _as_bool(_parm('tahaUseDeterministicRules', True), default=True):
        return '', ''

    name_sim = float(name_details.get('name_similarity', 0.0))
    first_sim = float(name_details.get('first_name_similarity', 0.0))
    last_sim = float(name_details.get('last_name_similarity', 0.0))
    positional_sim = float(name_details.get('name_positional_similarity', 0.0))

    addr_sim = float(address_details.get('address_similarity', 0.0))
    addr_num_match = bool(address_details.get('address_number_match', False))

    poison_addr_min = float(_parm('tahaPoisonAddressMinSimilarity', 0.92))
    poison_first_max = float(_parm('tahaPoisonFirstNameMax', 0.35))
    poison_last_max = float(_parm('tahaPoisonLastNameMax', 0.70))
    poison_name_max = float(_parm('tahaPoisonNameSimilarityMax', 0.68))
    poison_require_number = _as_bool(_parm('tahaPoisonRequireAddressNumberMatch', True), default=True)

    if (
        addr_sim >= poison_addr_min and
        first_sim <= poison_first_max and
        last_sim <= poison_last_max and
        name_sim <= poison_name_max and
        ((not poison_require_number) or addr_num_match)
    ):
        return 'reject', 'rule_address_match_name_conflict_poison'

    conflict_first_max = float(_parm('tahaCoreConflictFirstNameMax', 0.25))
    conflict_last_max = float(_parm('tahaCoreConflictLastNameMax', 0.55))
    conflict_name_max = float(_parm('tahaCoreConflictNameSimilarityMax', 0.58))
    if (
        first_sim <= conflict_first_max and
        last_sim <= conflict_last_max and
        name_sim <= conflict_name_max
    ):
        return 'reject', 'rule_core_name_conflict'

    strong_name_min = float(_parm('tahaStrongNameAcceptNameMin', 0.95))
    strong_last_min = float(_parm('tahaStrongNameAcceptLastMin', 0.95))
    strong_first_min = float(_parm('tahaStrongNameAcceptFirstMin', 0.88))
    strong_positional_min = float(_parm('tahaStrongNameAcceptPositionalMin', 0.93))
    strong_mu_delta = float(_parm('tahaStrongNameAcceptMuDelta', 0.28))
    min_similarity = max(0.40, min(1.0, float(mu) - strong_mu_delta))

    if (
        name_sim >= strong_name_min and
        last_sim >= strong_last_min and
        (first_sim >= strong_first_min or positional_sim >= strong_positional_min) and
        float(similarity) >= min_similarity
    ):
        return 'accept', 'rule_strong_name_accept'

    return '', ''


def _is_openai_source(llm_source):
    return isinstance(llm_source, str) and llm_source.startswith('openai_')


def _should_override_llm_reject(similarity, name_details, mu):
    name_sim = float(name_details.get('name_similarity', 0.0))
    first_sim = float(name_details.get('first_name_similarity', 0.0))
    last_sim = float(name_details.get('last_name_similarity', 0.0))
    positional_sim = float(name_details.get('name_positional_similarity', 0.0))

    strong_name_min = float(_parm('tahaStrongNameAcceptNameMin', 0.95))
    strong_last_min = float(_parm('tahaStrongNameAcceptLastMin', 0.95))
    strong_first_min = float(_parm('tahaStrongNameAcceptFirstMin', 0.88))
    strong_positional_min = float(_parm('tahaStrongNameAcceptPositionalMin', 0.93))
    strong_mu_delta = float(_parm('tahaStrongNameAcceptMuDelta', 0.28))
    min_similarity = max(0.40, min(1.0, float(mu) - strong_mu_delta))
    strong_name = (
        name_sim >= strong_name_min and
        last_sim >= strong_last_min and
        (first_sim >= strong_first_min or positional_sim >= strong_positional_min)
    )

    if strong_name and float(similarity) >= min_similarity:
        return True, 'llm_reject_overridden_strong_name'
    return False, ''


def _review_priority(decision):
    sim = float(decision.get('final_edge_score', decision.get('similarity', 0.0)))
    likely_link = float(_parm('tahaLikelyLinkThreshold', 0.74))
    closeness = 1.0 - min(1.0, abs(sim - likely_link))
    return (
        closeness,
        float(decision.get('cluster_impact', 0.0)),
        -float(decision.get('local_conflict_score', 0.0)),
        float(decision.get('name_similarity', 0.0)),
        float(decision.get('last_name_similarity', 0.0)),
        float(decision.get('first_name_similarity', 0.0)),
        sim,
    )


def _record_role_maps(tokens):
    return {
        'identity': _role_weight_map(tokens, 'identity_like'),
        'context': _role_weight_map(tokens, 'location_like'),
        'numeric': _role_weight_map(tokens, 'numeric_like'),
    }


def _build_profile_from_token_lists(token_lists):
    profile = {
        'size': len(token_lists),
        'stable_identity': {},
        'context': {},
        'numeric': {},
    }
    if not token_lists:
        return profile

    identity_sum = defaultdict(float)
    context_sum = defaultdict(float)
    numeric_sum = defaultdict(float)
    seen_count = defaultdict(int)
    cluster_size = len(token_lists)

    for token_list in token_lists:
        role_rows = _infer_soft_token_roles(token_list)
        seen_tokens = set()
        for row in role_rows:
            token = row['token']
            identity_sum[token] += float(row['identity_like'])
            context_sum[token] += float(row['location_like'])
            numeric_sum[token] += float(row['numeric_like'])
            if token not in seen_tokens:
                seen_count[token] += 1
                seen_tokens.add(token)

    for token, count in seen_count.items():
        rate = float(count) / float(cluster_size)
        identity_avg = float(identity_sum[token]) / float(cluster_size)
        context_avg = float(context_sum[token]) / float(cluster_size)
        numeric_avg = float(numeric_sum[token]) / float(cluster_size)
        stable_identity = _clamp((0.55 * rate) + (0.45 * identity_avg))
        stable_context = _clamp((0.50 * rate) + (0.50 * context_avg))
        stable_numeric = _clamp((0.50 * rate) + (0.50 * numeric_avg))
        if stable_identity >= 0.22 and rate >= 0.34:
            profile['stable_identity'][token] = stable_identity
        if stable_context >= 0.24 and rate >= 0.25:
            profile['context'][token] = stable_context
        if stable_numeric >= 0.28 and rate >= 0.25:
            profile['numeric'][token] = stable_numeric

    return profile


def _build_profile_from_members(members, refDict):
    token_lists = [refDict.get(ref_id, []) for ref_id in members]
    return _build_profile_from_token_lists(token_lists)


def _profile_alignment(tokens, profile):
    if not profile or profile.get('size', 0) <= 1:
        return 0.0, 0.0, 0.0, 0.0

    record_maps = _record_role_maps(tokens)
    identity_support = _weighted_overlap_from_maps(record_maps['identity'], profile.get('stable_identity', {}))
    context_support = _weighted_overlap_from_maps(record_maps['context'], profile.get('context', {}))
    numeric_support = _weighted_overlap_from_maps(record_maps['numeric'], profile.get('numeric', {}))
    support = _clamp((0.65 * identity_support) + (0.20 * context_support) + (0.15 * numeric_support))
    conflict = _clamp(max(0.0, 0.55 - identity_support) * 1.15)
    return support, conflict, identity_support, context_support


def _build_provisional_components(decision_rows, refDict):
    must_link = float(_parm('tahaMustLinkThreshold', 0.87))
    likely_link = float(_parm('tahaLikelyLinkThreshold', 0.74))
    parent = {}
    adjacency = defaultdict(set)
    node_set = set()

    def _find(node):
        parent.setdefault(node, node)
        while parent[node] != node:
            parent[node] = parent[parent[node]]
            node = parent[node]
        return node

    def _union(node_a, node_b):
        root_a = _find(node_a)
        root_b = _find(node_b)
        if root_a != root_b:
            parent[root_b] = root_a

    for decision in decision_rows:
        ref_id_1 = str(decision.get('refID1', '')).strip()
        ref_id_2 = str(decision.get('refID2', '')).strip()
        if ref_id_1 == '' or ref_id_2 == '':
            continue
        node_set.add(ref_id_1)
        node_set.add(ref_id_2)
        score = float(decision.get('base_edge_score', decision.get('similarity', 0.0)))
        identity = float(decision.get('identity_evidence_score', 0.0))
        context = float(decision.get('context_evidence_score', 0.0))
        contradiction = float(decision.get('contradiction_score', 0.0))
        edge_type = str(decision.get('edge_type', '')).strip()
        strong_edge = (
            edge_type == 'must_link' or
            (
                edge_type == 'likely_link' and
                score >= min(1.0, likely_link + 0.05) and
                identity >= max(0.55, context - 0.05) and
                contradiction <= 0.30
            ) or
            (
                score >= must_link and
                contradiction <= 0.25
            )
        )
        if strong_edge:
            _union(ref_id_1, ref_id_2)
            adjacency[ref_id_1].add(ref_id_2)
            adjacency[ref_id_2].add(ref_id_1)

    components = defaultdict(list)
    for node in node_set:
        components[_find(node)].append(node)

    profiles = {}
    for root, members in components.items():
        if len(members) <= 1:
            continue
        profiles[root] = _build_profile_from_members(members, refDict)

    node_to_root = {node: _find(node) for node in node_set}
    return node_to_root, profiles, adjacency


def _component_context_summary(decision, node_to_root, profiles, adjacency):
    ref_id_1 = decision.get('refID1', '')
    ref_id_2 = decision.get('refID2', '')
    full1 = decision.get('full_tokens1', decision.get('compared_tokens1', []))
    full2 = decision.get('full_tokens2', decision.get('compared_tokens2', []))
    root_1 = node_to_root.get(ref_id_1)
    root_2 = node_to_root.get(ref_id_2)
    profile_1 = profiles.get(root_1)
    profile_2 = profiles.get(root_2)

    support_1 = 0.0
    support_2 = 0.0
    conflict_1 = 0.0
    conflict_2 = 0.0
    identity_align_1 = 0.0
    identity_align_2 = 0.0
    if profile_2 is not None:
        support_1, conflict_1, identity_align_1, _ = _profile_alignment(full1, profile_2)
    if profile_1 is not None:
        support_2, conflict_2, identity_align_2, _ = _profile_alignment(full2, profile_1)

    neigh_1 = adjacency.get(ref_id_1, set())
    neigh_2 = adjacency.get(ref_id_2, set())
    union_cnt = len(neigh_1.union(neigh_2))
    if union_cnt > 0:
        shared_ratio = float(len(neigh_1.intersection(neigh_2))) / float(union_cnt)
    else:
        shared_ratio = 0.0

    support_values = [v for v in (support_1, support_2) if v > 0.0]
    local_support = float(sum(support_values) / len(support_values)) if support_values else 0.0
    # blend profile alignment with shared-neighbor ratio (equal weight)
    local_support = _clamp((local_support + shared_ratio) / 2.0)

    conflict_values = [v for v in (conflict_1, conflict_2) if v > 0.0]
    local_conflict = float(sum(conflict_values) / len(conflict_values)) if conflict_values else 0.0
    # amplify conflict when context dominates identity and support is absent
    ctx_ev = float(decision.get('context_evidence_score', 0.0))
    id_ev = float(decision.get('identity_evidence_score', 0.0))
    if ctx_ev > id_ev and local_support == 0.0:
        local_conflict = _clamp(max(local_conflict, ctx_ev))

    cluster_impact = _clamp(local_support - local_conflict, low=-1.0, high=1.0)
    context_lines = []
    if profile_1 is not None:
        context_lines.append(
            f"{ref_id_1} local cluster size={profile_1.get('size', 1)} align={support_2:.3f} conflict={conflict_2:.3f}"
        )
    if profile_2 is not None:
        context_lines.append(
            f"{ref_id_2} local cluster size={profile_2.get('size', 1)} align={support_1:.3f} conflict={conflict_1:.3f}"
        )
    if shared_ratio > 0.0:
        context_lines.append(f"shared provisional neighbors ratio={shared_ratio:.3f}")

    return {
        'local_support_score': float(local_support),
        'local_conflict_score': float(local_conflict),
        'cluster_impact': float(cluster_impact),
        'shared_neighbor_ratio': float(shared_ratio),
        'identity_alignment_1': float(identity_align_1),
        'identity_alignment_2': float(identity_align_2),
        'local_context_prompt': '; '.join(context_lines),
    }


def _finalize_context_decision(decision):
    """Revised context decision: context adjusts the score, mu makes the decision.

    Instead of a 140-line threshold cascade, the provisional context pass
    simply revises the base_edge_score by adding support and subtracting
    conflict, then compares to mu.  Rule-based decisions and LLM file
    decisions are still respected.
    """
    mu = float(DWM10_Parms.mu)

    base_score = float(decision.get('base_edge_score', decision.get('similarity', 0.0)))
    local_support = float(decision.get('local_support_score', 0.0))
    local_conflict = float(decision.get('local_conflict_score', 0.0))
    cluster_impact = float(decision.get('cluster_impact', 0.0))
    base_initial = str(
        decision.get('base_initial_decision', decision.get('initial_decision', ''))
    ).strip()
    existing_llm = str(decision.get('llm_decision', '')).strip()
    identity_score = float(decision.get('identity_evidence_score', 0.0))
    context_score = float(decision.get('context_evidence_score', 0.0))
    contradiction_score = float(decision.get('contradiction_score', 0.0))

    # --- pass-through: decisions already made by rules or LLM stay untouched ---
    if base_initial != 'llm_review' or existing_llm in ('accept', 'reject'):
        preserved_final = str(decision.get('final_decision', '')).strip()
        if preserved_final not in ('accept', 'reject'):
            preserved_final = 'accept' if base_initial == 'accept' else 'reject'
        decision['final_edge_score'] = float(base_score)
        decision['edge_type'] = _edge_type_from_scores(
            base_score, identity_score, context_score, contradiction_score
        )
        decision['initial_decision'] = base_initial
        decision['final_decision'] = preserved_final
        decision['link'] = (preserved_final == 'accept')
        return

    # --- context-revised score: simple additive adjustment ---
    # support lifts the score, conflict lowers it, weighted by cluster_impact
    context_adjustment = (local_support - local_conflict) * abs(cluster_impact)
    revised_score = _clamp(base_score + context_adjustment)
    decision['final_edge_score'] = float(revised_score)
    decision['edge_type'] = _edge_type_from_scores(
        revised_score, identity_score, context_score, contradiction_score
    )

    # --- check for LLM file decisions before making our own ---
    pair_key = _pair_key(decision.get('refID1', ''), decision.get('refID2', ''))
    file_review = _llm_decision_map.get(pair_key, '')
    if file_review == 'accept':
        decision['initial_decision'] = 'llm_review'
        decision['llm_decision'] = 'accept'
        decision['llm_source'] = 'file'
        decision['final_decision'] = 'accept'
        decision['reason'] = 'llm_accept'
        decision['link'] = True
        return
    if file_review == 'reject':
        decision['initial_decision'] = 'llm_review'
        decision['llm_decision'] = 'reject'
        decision['llm_source'] = 'file'
        decision['final_decision'] = 'reject'
        decision['reason'] = 'llm_reject'
        decision['link'] = False
        return

    # --- the decision: revised score vs mu ---
    # rule decisions are respected unless context strongly disagrees
    rule_decision = decision.get('rule_decision', '')
    if rule_decision == 'reject' and revised_score < mu:
        decision['initial_decision'] = 'reject'
        decision['final_decision'] = 'reject'
        decision['reason'] = decision.get('rule_reason', 'context_rule_reject')
    elif rule_decision == 'accept' and revised_score >= mu:
        decision['initial_decision'] = 'accept'
        decision['final_decision'] = 'accept'
        decision['reason'] = decision.get('rule_reason', 'context_rule_accept')
    elif revised_score >= mu and identity_score > context_score:
        # accept: score above mu and identity drives the match
        decision['initial_decision'] = 'accept'
        decision['final_decision'] = 'accept'
        decision['reason'] = 'context_accept'
    elif revised_score >= mu and identity_score <= context_score:
        # score above mu but context-driven — send to LLM if available
        if _openai_enabled():
            decision['initial_decision'] = 'llm_review'
            decision['final_decision'] = 'pending_llm'
            decision['reason'] = 'context_only_review'
            decision['link'] = False
            return
        else:
            decision['initial_decision'] = 'reject'
            decision['final_decision'] = 'reject'
            decision['reason'] = 'context_only_no_llm_reject'
    else:
        # below mu after context adjustment
        decision['initial_decision'] = 'reject'
        decision['final_decision'] = 'reject'
        decision['reason'] = 'context_reject'

    decision['link'] = (decision.get('final_decision', '') == 'accept')


def apply_provisional_context_pass(decision_rows, refDict):
    if not _as_bool(_parm('tahaUseProvisionalContext', False), default=False):
        for decision in decision_rows:
            decision.setdefault('local_support_score', 0.0)
            decision.setdefault('local_conflict_score', 0.0)
            decision.setdefault('cluster_impact', 0.0)
            decision.setdefault('shared_neighbor_ratio', 0.0)
            decision.setdefault('local_context_prompt', '')
            decision.setdefault('final_edge_score', float(decision.get('base_edge_score', decision.get('similarity', 0.0))))
            decision.setdefault(
                'edge_type',
                _edge_type_from_scores(
                    float(decision.get('final_edge_score', 0.0)),
                    float(decision.get('identity_evidence_score', 0.0)),
                    float(decision.get('context_evidence_score', 0.0)),
                    float(decision.get('contradiction_score', 0.0))
                )
            )
        return

    node_to_root, profiles, adjacency = _build_provisional_components(decision_rows, refDict)
    for decision in decision_rows:
        summary = _component_context_summary(decision, node_to_root, profiles, adjacency)
        decision.update(summary)
        _finalize_context_decision(decision)


def schema_light_cluster_quality(cluster):
    if not cluster:
        return 1.0
    profile = _build_profile_from_token_lists(cluster)
    if profile.get('size', 0) <= 1:
        return 1.0

    identity_alignment = 0.0
    context_alignment = 0.0
    numeric_alignment = 0.0
    conflict_total = 0.0

    for token_list in cluster:
        support, conflict, identity_support, context_support = _profile_alignment(token_list, profile)
        role_maps = _record_role_maps(token_list)
        numeric_support = _weighted_overlap_from_maps(role_maps['numeric'], profile.get('numeric', {}))
        identity_alignment += identity_support
        context_alignment += context_support
        numeric_alignment += numeric_support
        conflict_total += conflict

    cluster_size = float(len(cluster))
    raw_quality = _clamp(
        (0.62 * (identity_alignment / cluster_size)) +
        (0.18 * (context_alignment / cluster_size)) +
        (0.10 * (numeric_alignment / cluster_size)) +
        (0.10 * min(1.0, len(profile.get('stable_identity', {})) / 6.0)) -
        (0.30 * (conflict_total / cluster_size))
    )
    return _clamp(0.25 + (0.75 * (raw_quality ** 0.40)))


def _compute_dataset_stats(tokenFreqDict):
    """Compute dataset-level frequency statistics once per run.

    Returns a dict with percentile thresholds derived from the actual
    frequency distribution so that token-role inference needs no
    hard-coded constants.
    """
    if not tokenFreqDict:
        return {
            'freq_p25': 1, 'freq_p50': 1, 'freq_p75': 5,
            'freq_p90': 10, 'freq_p95': 20, 'freq_max': 1,
            'log_max_freq': 1.0, 'total_tokens': 0,
            'unique_tokens': 0, 'avg_freq': 1.0,
        }
    freqs = sorted(tokenFreqDict.values())
    n = len(freqs)
    import math

    def _percentile(sorted_vals, p):
        idx = int(p / 100.0 * (len(sorted_vals) - 1))
        return sorted_vals[min(idx, len(sorted_vals) - 1)]

    total = sum(freqs)
    max_freq = freqs[-1] if freqs else 1
    return {
        'freq_p25': _percentile(freqs, 25),
        'freq_p50': _percentile(freqs, 50),
        'freq_p75': _percentile(freqs, 75),
        'freq_p90': _percentile(freqs, 90),
        'freq_p95': _percentile(freqs, 95),
        'freq_max': max_freq,
        'log_max_freq': math.log1p(max_freq),
        'total_tokens': total,
        'unique_tokens': n,
        'avg_freq': float(total) / max(1, n),
    }


def start_run(tokenFreqDict=None):
    global _run_decisions
    global _run_stats
    global _openai_cache
    global _context_by_ref
    global _openai_key_missing_logged
    global _pair_feature_cache
    global _run_token_freq_dict
    global _dataset_stats
    _run_decisions = []
    persist_caches = _as_bool(_parm('tahaPersistCachesAcrossIterations', True), default=True)
    if not persist_caches:
        _openai_cache = {}
        _context_by_ref = {}
        _pair_feature_cache = {}
    _openai_key_missing_logged = False
    _run_token_freq_dict = dict(tokenFreqDict) if isinstance(tokenFreqDict, dict) else {}
    _dataset_stats = _compute_dataset_stats(_run_token_freq_dict)
    _run_stats = {
        'total_pairs': 0,
        'auto_reject_pairs': 0,
        'auto_accept_pairs': 0,
        'rule_reject_pairs': 0,
        'rule_accept_pairs': 0,
        'rule_poison_reject_pairs': 0,
        'rule_name_conflict_reject_pairs': 0,
        'rule_strong_name_accept_pairs': 0,
        'llm_review_pairs': 0,
        'llm_accept_pairs': 0,
        'llm_reject_pairs': 0,
        'pending_llm_pairs': 0,
        'context_pairs': 0,
        'linked_pairs': 0,
        'openai_review_requests': 0,
        'openai_review_accept': 0,
        'openai_review_reject': 0,
        'openai_review_errors': 0,
        'openai_review_skipped_no_key': 0,
        'openai_chat_fallback_used': 0,
        'file_review_accept': 0,
        'file_review_reject': 0,
        'review_limit_skips': 0,
        'llm_reject_overrides': 0,
        'context_pass_revisions': 0,
        'context_disagreement_reviews': 0,
    }
    _load_llm_decisions_if_needed()


def compare_pair(
    refID1,
    refID2,
    tokenList1,
    tokenList2,
    mu=None,
    trace_min_sim=0.01,
    fullTokenList1=None,
    fullTokenList2=None,
    defer_openai=False
):
    global _openai_key_missing_logged
    global _pair_feature_cache
    if mu is None:
        mu = float(DWM10_Parms.mu)
    name_source_1 = fullTokenList1 if fullTokenList1 is not None else tokenList1
    name_source_2 = fullTokenList2 if fullTokenList2 is not None else tokenList2

    cache_key = _feature_cache_key(refID1, refID2, tokenList1, tokenList2, name_source_1, name_source_2)
    cached = _pair_feature_cache.get(cache_key)

    if cached is not None:
        similarity = float(cached['similarity'])
        name_details = dict(cached['name_details'])
        address_details = dict(cached.get('address_details', {}))
        evidence = dict(cached.get('evidence', {}))
        if not address_details:
            address_details = address_similarity_details(name_source_1, name_source_2)
        if not evidence:
            evidence = _soft_role_evidence(name_source_1, name_source_2, name_details, address_details, similarity)
        trace_count = int(cached.get('trace_count', 0))
    else:
        collect_trace = _as_bool(_parm('tahaCollectDecisionTraceCount', False), default=False)
        if collect_trace:
            similarity, trace = normalized_similarity(
                tokenList1,
                tokenList2,
                return_trace=True,
                trace_min_sim=trace_min_sim
            )
            trace_count = len(trace)
        else:
            similarity = normalized_similarity(
                tokenList1,
                tokenList2,
                return_trace=False
            )
            trace_count = 0

        name_details = name_similarity_details(name_source_1, name_source_2)
        address_details = address_similarity_details(name_source_1, name_source_2)
        evidence = _soft_role_evidence(name_source_1, name_source_2, name_details, address_details, similarity)

        cache_max = int(_parm('tahaPairFeatureCacheMaxSize', 250000))
        if cache_max < 0:
            cache_max = 0
        if cache_max == 0 or len(_pair_feature_cache) < cache_max:
            _pair_feature_cache[cache_key] = {
                'similarity': float(similarity),
                'name_details': dict(name_details),
                'address_details': dict(address_details),
                'evidence': dict(evidence),
                'trace_count': int(trace_count),
            }

    low_cutoff, review_upper, context_threshold = _thresholds(mu)
    rule_decision, rule_reason = _deterministic_rule_decision(
        similarity,
        name_details,
        address_details,
        mu
    )

    reject_name, name_reason = _name_rule_reject(name_details)
    if rule_decision == 'reject':
        initial_decision = 'reject'
        reason = rule_reason
    elif reject_name:
        initial_decision = 'reject'
        reason = name_reason
    elif rule_decision == 'accept':
        initial_decision = 'accept'
        reason = rule_reason
    else:
        if similarity < low_cutoff:
            if _soft_identity_bridge(name_details, evidence):
                initial_decision = 'llm_review'
                reason = 'soft_identity_bridge_review'
            else:
                initial_decision = 'reject'
                reason = 'similarity_below_reject_cutoff'
        elif similarity <= review_upper:
            initial_decision = 'llm_review'
            reason = 'similarity_in_llm_review_band'
        else:
            if _context_only_overlap_flag(name_details, evidence):
                initial_decision = 'llm_review'
                reason = 'context_only_overlap_review'
            else:
                initial_decision = 'accept'
                reason = 'similarity_above_auto_accept_cutoff'

    base_initial_decision = initial_decision
    base_edge_score = float(evidence.get('base_edge_score', similarity))
    identity_evidence_score = float(evidence.get('identity_evidence_score', 0.0))
    context_evidence_score = float(evidence.get('context_evidence_score', 0.0))
    contradiction_score = float(evidence.get('contradiction_score', 0.0))
    edge_type = _edge_type_from_scores(
        base_edge_score,
        identity_evidence_score,
        context_evidence_score,
        contradiction_score
    )

    llm_decision = ''
    llm_source = 'none'
    llm_raw = ''
    llm_reason = ''
    llm_confidence = ''
    final_decision = initial_decision
    if initial_decision == 'llm_review':
        pair_key = _pair_key(refID1, refID2)
        llm_decision = _llm_decision_map.get(pair_key, '')

        if llm_decision in ('accept', 'reject'):
            llm_source = 'file'
            if llm_decision == 'accept':
                _run_stats['file_review_accept'] += 1
            else:
                _run_stats['file_review_reject'] += 1
        elif _openai_enabled():
            if defer_openai:
                final_decision = 'pending_llm'
                reason = 'llm_pending_async_queue'
            else:
                if _openai_review_limit_reached():
                    _run_stats['review_limit_skips'] += 1
                    final_decision = 'pending_llm'
                    reason = 'llm_review_limit_reached'
                else:
                    _run_stats['openai_review_requests'] += 1
                    llm_decision, llm_source, llm_raw, llm_reason, llm_confidence = _call_openai_for_decision(
                        refID1,
                        refID2,
                        name_source_1,
                        name_source_2,
                        similarity,
                        name_details,
                        mu,
                        local_context_text=''
                    )
                    if llm_decision == 'accept':
                        _run_stats['openai_review_accept'] += 1
                    elif llm_decision == 'reject':
                        _run_stats['openai_review_reject'] += 1
                    else:
                        if llm_source == 'none':
                            _run_stats['openai_review_skipped_no_key'] += 1
                            if not _openai_key_missing_logged:
                                print('Warning: OpenAI review enabled but API key is missing; review-band pairs stay pending.')
                                _openai_key_missing_logged = True
                        else:
                            _run_stats['openai_review_errors'] += 1
                    if llm_source == 'openai_live_chat_fallback':
                        _run_stats['openai_chat_fallback_used'] += 1

        if llm_decision == 'accept':
            final_decision = 'accept'
            reason = 'llm_accept'
        elif llm_decision == 'reject':
            final_decision = 'reject'
            reason = 'llm_reject'
            if _is_openai_source(llm_source):
                override_accept, override_reason = _should_override_llm_reject(
                    similarity,
                    name_details,
                    mu
                )
                if override_accept:
                    final_decision = 'accept'
                    reason = override_reason
                    _run_stats['llm_reject_overrides'] += 1
        elif final_decision != 'pending_llm':
            final_decision = 'pending_llm'
            if llm_source == 'none' and _openai_enabled():
                reason = 'llm_pending_no_api_key'
            elif llm_source == 'openai_error':
                reason = 'llm_pending_openai_error'
            else:
                reason = 'llm_pending_manual_review'

    final_edge_score = base_edge_score
    link = (final_decision == 'accept')
    context_candidate = similarity >= context_threshold
    if (
        link and
        context_candidate and
        fullTokenList1 is not None and
        fullTokenList2 is not None
    ):
        _record_context(refID1, refID2, fullTokenList1, fullTokenList2, similarity)

    return {
        'refID1': refID1,
        'refID2': refID2,
        'full_tokens1': name_source_1[:],
        'full_tokens2': name_source_2[:],
        'compared_tokens1': tokenList1[:],
        'compared_tokens2': tokenList2[:],
        'similarity': float(similarity),
        'mu': float(mu),
        'reject_cutoff': float(low_cutoff),
        'review_upper': float(review_upper),
        'context_threshold': float(context_threshold),
        'name_similarity': float(name_details['name_similarity']),
        'first_name_similarity': float(name_details['first_name_similarity']),
        'middle_name_similarity': float(name_details['middle_name_similarity']),
        'last_name_similarity': float(name_details['last_name_similarity']),
        'name_positional_similarity': float(name_details['name_positional_similarity']),
        'address_similarity': float(address_details.get('address_similarity', 0.0)),
        'address_number_similarity': float(address_details.get('address_number_similarity', 0.0)),
        'address_number_match': bool(address_details.get('address_number_match', False)),
        'identity_evidence_score': float(identity_evidence_score),
        'context_evidence_score': float(context_evidence_score),
        'contradiction_score': float(contradiction_score),
        'base_edge_score': float(base_edge_score),
        'final_edge_score': float(final_edge_score),
        'local_support_score': 0.0,
        'local_conflict_score': 0.0,
        'cluster_impact': 0.0,
        'shared_neighbor_ratio': 0.0,
        'edge_type': edge_type,
        'base_initial_decision': base_initial_decision,
        'local_context_prompt': '',
        'rule_decision': rule_decision,
        'rule_reason': rule_reason,
        'initial_decision': initial_decision,
        'llm_decision': llm_decision,
        'llm_source': llm_source,
        'llm_raw': llm_raw[:500] if isinstance(llm_raw, str) else '',
        'llm_reason': llm_reason,
        'llm_confidence': llm_confidence,
        'final_decision': final_decision,
        'context_candidate': context_candidate,
        'reason': reason,
        'link': link,
        'trace_count': trace_count,
    }


def _apply_llm_result_to_decision(decision, result, full1, full2):
    global _openai_key_missing_logged

    llm_decision, llm_source, llm_raw, llm_reason, llm_confidence = result
    decision['llm_decision'] = llm_decision
    decision['llm_source'] = llm_source
    decision['llm_raw'] = llm_raw[:500] if isinstance(llm_raw, str) else ''
    decision['llm_reason'] = llm_reason
    decision['llm_confidence'] = llm_confidence

    if llm_decision == 'accept':
        decision['final_decision'] = 'accept'
        decision['reason'] = 'llm_accept'
        decision['link'] = True
        decision['final_edge_score'] = max(
            float(decision.get('final_edge_score', 0.0)),
            float(_parm('tahaLikelyLinkThreshold', 0.74))
        )
        if decision.get('edge_type', '') not in ('must_link', 'likely_link'):
            decision['edge_type'] = 'likely_link'
        _run_stats['openai_review_accept'] += 1
        if decision.get('context_candidate', False):
            _record_context(
                decision.get('refID1', ''),
                decision.get('refID2', ''),
                full1,
                full2,
                decision.get('similarity', 0.0)
            )
    elif llm_decision == 'reject':
        override_accept = False
        override_reason = ''
        if _is_openai_source(llm_source):
            override_accept, override_reason = _should_override_llm_reject(
                float(decision.get('similarity', 0.0)),
                {
                    'name_similarity': float(decision.get('name_similarity', 0.0)),
                    'first_name_similarity': float(decision.get('first_name_similarity', 0.0)),
                    'last_name_similarity': float(decision.get('last_name_similarity', 0.0)),
                    'name_positional_similarity': float(decision.get('name_positional_similarity', 0.0)),
                },
                float(decision.get('mu', _parm('mu', 0.5)))
            )

        if override_accept:
            decision['final_decision'] = 'accept'
            decision['reason'] = override_reason
            decision['link'] = True
            _run_stats['openai_review_accept'] += 1
            _run_stats['llm_reject_overrides'] += 1
            if decision.get('context_candidate', False):
                _record_context(
                    decision.get('refID1', ''),
                    decision.get('refID2', ''),
                    full1,
                    full2,
                    decision.get('similarity', 0.0)
                )
        else:
            decision['final_decision'] = 'reject'
            decision['reason'] = 'llm_reject'
            decision['link'] = False
            decision['edge_type'] = 'cannot_link'
            decision['final_edge_score'] = min(
                float(decision.get('final_edge_score', 0.0)),
                float(_parm('tahaCannotLinkThreshold', 0.28))
            )
            _run_stats['openai_review_reject'] += 1
    else:
        decision['final_decision'] = 'pending_llm'
        decision['link'] = False
        if llm_source == 'none':
            decision['reason'] = 'llm_pending_no_api_key'
            _run_stats['openai_review_skipped_no_key'] += 1
            if not _openai_key_missing_logged:
                print('Warning: OpenAI review enabled but API key is missing; review-band pairs stay pending.')
                _openai_key_missing_logged = True
        elif llm_source == 'openai_error':
            decision['reason'] = 'llm_pending_openai_error'
            _run_stats['openai_review_errors'] += 1
        else:
            decision['reason'] = 'llm_pending_manual_review'

    if isinstance(llm_source, str) and ('chat_fallback' in llm_source):
        _run_stats['openai_chat_fallback_used'] += 1


def _build_anchor_batches(candidates, batch_size):
    grouped = {}
    for idx, decision in candidates:
        anchor = str(decision.get('refID1', '')).strip()
        if anchor == '':
            anchor = f'__missing_anchor_{idx}'
        if anchor not in grouped:
            grouped[anchor] = []
        grouped[anchor].append((idx, decision))

    batches = []
    for anchor, rows in grouped.items():
        rows.sort(key=lambda item: _review_priority(item[1]), reverse=True)
        for start in range(0, len(rows), batch_size):
            batches.append((anchor, rows[start:start + batch_size]))

    batches.sort(key=lambda x: (-len(x[1]), x[0]))
    return batches


def _format_duration(seconds):
    try:
        total = int(round(float(seconds)))
    except Exception:
        return '??:??'
    if total < 0:
        total = 0
    hours, rem = divmod(total, 3600)
    mins, secs = divmod(rem, 60)
    if hours > 0:
        return f'{hours}:{mins:02d}:{secs:02d}'
    return f'{mins:02d}:{secs:02d}'


def resolve_pending_reviews_async(decision_rows, refDict, logFile=None):
    global _openai_key_missing_logged

    if not _openai_enabled():
        return

    def _progress_log(message):
        print(message)
        if logFile is not None:
            try:
                print(message, file=logFile)
                logFile.flush()
            except Exception:
                pass

    candidates = []
    for idx, decision in enumerate(decision_rows):
        if decision.get('initial_decision', '') != 'llm_review':
            continue
        if decision.get('final_decision', '') != 'pending_llm':
            continue
        if decision.get('llm_decision', '') not in ('', None):
            continue
        candidates.append((idx, decision))

    if not candidates:
        return

    candidates.sort(key=lambda item: _review_priority(item[1]), reverse=True)

    max_reviews = int(_parm('tahaOpenAIReviewMaxPairs', 0))
    if max_reviews > 0 and len(candidates) > max_reviews:
        overflow = len(candidates) - max_reviews
        for idx, _ in candidates[max_reviews:]:
            decision_rows[idx]['reason'] = 'llm_review_limit_reached'
            decision_rows[idx]['final_decision'] = 'pending_llm'
            decision_rows[idx]['link'] = False
        _run_stats['review_limit_skips'] += overflow
        _progress_log(
            f'DWM55 OpenAI review limit active: processing {max_reviews} of {max_reviews + overflow}; '
            f'{overflow} remain pending.'
        )
        candidates = candidates[:max_reviews]

    if not candidates:
        return

    max_workers = int(_parm('tahaOpenAIAsyncWorkers', 8))
    if max_workers < 1:
        max_workers = 1
    if max_workers > 128:
        max_workers = 128

    total_candidates = len(candidates)
    progress_step = max(1, total_candidates // 50)
    progress_start = time.time()
    last_progress_time = progress_start
    last_progress_count = 0
    use_anchor_batch = _as_bool(_parm('tahaUseAnchorBatchReview', False), default=False)
    anchor_batch_size = int(_parm('tahaAnchorBatchSize', 6))
    if anchor_batch_size < 1:
        anchor_batch_size = 1
    if anchor_batch_size > 50:
        anchor_batch_size = 50

    if use_anchor_batch:
        work_items = _build_anchor_batches(candidates, anchor_batch_size)
        _progress_log(
            f'DWM55 OpenAI review queue: {total_candidates} pair(s), workers={max_workers}, '
            f'mode=anchor_batch, batches={len(work_items)}, batch_size={anchor_batch_size}.'
        )

        def _worker(item):
            anchor_refID, entries = item
            anchor_tokens = refDict.get(anchor_refID, [])
            if not anchor_tokens and entries:
                anchor_tokens = entries[0][1].get('compared_tokens1', [])

            payload_rows = []
            for idx, decision in entries:
                refID2 = decision.get('refID2', '')
                full2 = refDict.get(refID2, decision.get('compared_tokens2', []))
                payload_rows.append({
                    'idx': idx,
                    'refID2': refID2,
                    'full2': full2,
                    'similarity': float(decision.get('similarity', 0.0)),
                    'mu': float(decision.get('mu', _parm('mu', 0.5))),
                    'local_context': str(decision.get('local_context_prompt', '')).strip(),
                    'name_details': {
                        'name_similarity': float(decision.get('name_similarity', 0.0)),
                        'first_name_similarity': float(decision.get('first_name_similarity', 0.0)),
                        'middle_name_similarity': float(decision.get('middle_name_similarity', 0.0)),
                        'last_name_similarity': float(decision.get('last_name_similarity', 0.0)),
                        'name_positional_similarity': float(decision.get('name_positional_similarity', 0.0)),
                    },
                })

            return entries, _call_openai_for_anchor_batch(anchor_refID, anchor_tokens, payload_rows)
    else:
        work_items = candidates
        _progress_log(
            f'DWM55 OpenAI review queue: {total_candidates} pair(s), workers={max_workers}, mode=pair.'
        )

        def _worker(item):
            idx, decision = item
            refID1 = decision.get('refID1', '')
            refID2 = decision.get('refID2', '')
            full1 = refDict.get(refID1, decision.get('compared_tokens1', []))
            full2 = refDict.get(refID2, decision.get('compared_tokens2', []))
            name_details = {
                'name_similarity': float(decision.get('name_similarity', 0.0)),
                'first_name_similarity': float(decision.get('first_name_similarity', 0.0)),
                'middle_name_similarity': float(decision.get('middle_name_similarity', 0.0)),
                'last_name_similarity': float(decision.get('last_name_similarity', 0.0)),
                'name_positional_similarity': float(decision.get('name_positional_similarity', 0.0)),
            }
            sim = float(decision.get('similarity', 0.0))
            mu = float(decision.get('mu', _parm('mu', 0.5)))
            local_context_text = str(decision.get('local_context_prompt', '')).strip()
            result = _call_openai_for_decision(
                refID1,
                refID2,
                full1,
                full2,
                sim,
                name_details,
                mu,
                local_context_text=local_context_text
            )
            return [(idx, decision)], {idx: result}

    def _log_progress(completed):
        nonlocal last_progress_time
        nonlocal last_progress_count
        now = time.time()
        should_log = (
            completed == 1 or
            completed == total_candidates or
            (completed % progress_step == 0) or
            (now - last_progress_time >= 10.0 and completed > last_progress_count)
        )
        if not should_log:
            return
        elapsed = now - progress_start
        rate = (completed / elapsed) if elapsed > 0.0 else 0.0
        remaining = total_candidates - completed
        eta = (remaining / rate) if rate > 0.0 else 0.0
        pct = (100.0 * completed / total_candidates) if total_candidates > 0 else 100.0
        bar_len = 24
        filled = int((completed * bar_len) / total_candidates) if total_candidates > 0 else bar_len
        bar = '[' + ('#' * filled) + ('-' * (bar_len - filled)) + ']'
        _progress_log(
            f'DWM55 OpenAI review progress {bar} {completed}/{total_candidates} '
            f'({pct:5.1f}%) elapsed={_format_duration(elapsed)} '
            f'eta={_format_duration(eta)} rate={rate:.1f}/s'
        )
        last_progress_time = now
        last_progress_count = completed

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(_worker, item): item for item in work_items}
        completed = 0

        for future in concurrent.futures.as_completed(future_map):
            source_item = future_map[future]

            try:
                entries, result_by_idx = future.result()
            except Exception as e:
                if use_anchor_batch:
                    entries = source_item[1]
                else:
                    entries = [source_item]
                result_by_idx = {idx: ('', 'openai_error', str(e), '', '') for idx, _ in entries}

            for idx, _ in entries:
                decision = decision_rows[idx]
                _run_stats['openai_review_requests'] += 1
                result = result_by_idx.get(idx, ('', 'openai_error', 'Missing OpenAI review result', '', ''))
                full1 = refDict.get(decision.get('refID1', ''), decision.get('compared_tokens1', []))
                full2 = refDict.get(decision.get('refID2', ''), decision.get('compared_tokens2', []))
                _apply_llm_result_to_decision(decision, result, full1, full2)
                completed += 1
                _log_progress(completed)

    total_elapsed = time.time() - progress_start
    avg_rate = (total_candidates / total_elapsed) if total_elapsed > 0.0 else 0.0
    _progress_log(
        f'DWM55 OpenAI review complete: {total_candidates}/{total_candidates} '
        f'in {_format_duration(total_elapsed)} (avg {avg_rate:.1f}/s).'
    )


def record_decision(decision):
    _run_decisions.append(decision)
    _run_stats['total_pairs'] += 1
    reason = str(decision.get('reason', '')).strip()

    if str(decision.get('base_initial_decision', decision.get('initial_decision', ''))) != str(decision.get('initial_decision', '')):
        _run_stats['context_pass_revisions'] += 1
    if reason == 'context_disagreement_review':
        _run_stats['context_disagreement_reviews'] += 1

    if decision['initial_decision'] == 'reject':
        _run_stats['auto_reject_pairs'] += 1
    elif decision['initial_decision'] == 'accept':
        _run_stats['auto_accept_pairs'] += 1
    elif decision['initial_decision'] == 'llm_review':
        _run_stats['llm_review_pairs'] += 1

    if decision['final_decision'] == 'accept':
        _run_stats['linked_pairs'] += 1
        if decision['initial_decision'] == 'llm_review':
            _run_stats['llm_accept_pairs'] += 1
    elif decision['final_decision'] == 'reject' and decision['initial_decision'] == 'llm_review':
        _run_stats['llm_reject_pairs'] += 1
    elif decision['final_decision'] == 'pending_llm':
        _run_stats['pending_llm_pairs'] += 1

    if decision['context_candidate']:
        _run_stats['context_pairs'] += 1

    rule_decision = str(decision.get('rule_decision', '')).strip()
    rule_reason = str(decision.get('rule_reason', '')).strip()
    if rule_decision == 'reject':
        _run_stats['rule_reject_pairs'] += 1
    elif rule_decision == 'accept':
        _run_stats['rule_accept_pairs'] += 1
    if rule_reason == 'rule_address_match_name_conflict_poison':
        _run_stats['rule_poison_reject_pairs'] += 1
    elif rule_reason == 'rule_core_name_conflict':
        _run_stats['rule_name_conflict_reject_pairs'] += 1
    elif rule_reason == 'rule_strong_name_accept':
        _run_stats['rule_strong_name_accept_pairs'] += 1


def get_run_stats():
    return dict(_run_stats)


def get_last_decisions():
    return list(_run_decisions)
