#!/usr/bin/env python
# coding: utf-8

import argparse
import csv
import datetime
import os

import DWM10_Parms
import DWM55_LinkBlockPairs
import DWM67_Tahacomparator
import DWM80_TransitiveClosure
import DWM90_IterateClusters
import DWM99_ERmetrics
import DWM_DataCapture


def _print_both(message, log_file):
    print(message)
    print(message, file=log_file)


def _split_tokens(value):
    if value is None:
        return []
    return [token.strip() for token in str(value).split(',') if token.strip()]


def _choose_existing(paths):
    for path in paths:
        if os.path.exists(path):
            return path
    return ''


def _load_ref_dict(path):
    ref_dict = {}
    with open(path, 'r', newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            ref_id = str(row.get('refID', '')).strip()
            if ref_id == '':
                continue
            ref_dict[ref_id] = _split_tokens(row.get('tokens', ''))
    return ref_dict


def _load_link_index(path):
    link_index = {}
    with open(path, 'r', newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            ref_id = str(row.get('refID', '')).strip()
            cluster_id = str(row.get('clusterID', '')).strip()
            if ref_id == '':
                continue
            link_index[ref_id] = cluster_id if cluster_id else ref_id
    return link_index


def _load_token_freq_dict(path):
    token_freq_dict = {}
    with open(path, 'r', newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            token = str(row.get('token', '')).strip()
            freq_text = str(row.get('frequency', '')).strip()
            if token == '':
                continue
            try:
                token_freq_dict[token] = int(freq_text)
            except Exception:
                token_freq_dict[token] = 0
    return token_freq_dict


def _load_block_pair_list(path):
    block_pair_list = []
    with open(path, 'r', newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            ref_id1 = str(row.get('refID1', '')).strip()
            ref_id2 = str(row.get('refID2', '')).strip()
            if ref_id1 == '' or ref_id2 == '':
                continue
            block_pair_list.append(ref_id1 + '|' + ref_id2)
    return block_pair_list


def _default_paths(capture_root, iteration):
    iter_tag = str(iteration).zfill(2)
    iteration_dir = os.path.join(capture_root, 'iteration_' + iter_tag)
    ref_dict_path = _choose_existing([
        os.path.join(capture_root, '04_refDict_after_global_correction.csv'),
        os.path.join(capture_root, '01_refDict.csv'),
    ])
    token_freq_path = _choose_existing([
        os.path.join(capture_root, '04_tokenFreqDict_after_global_correction.csv'),
        os.path.join(capture_root, '03_tokenFreqDict.csv'),
    ])
    block_pair_path = _choose_existing([
        os.path.join(iteration_dir, '06_blockPairList_after_block_correction.csv'),
        os.path.join(iteration_dir, '05_blockPairList.csv'),
    ])
    link_index_path = os.path.join(capture_root, '02_linkIndex_initial.csv')
    return ref_dict_path, token_freq_path, link_index_path, block_pair_path, iteration_dir


def main():
    parser = argparse.ArgumentParser(
        description='Replay TahaComparator-CX on captured block pairs without rerunning DWM42.'
    )
    parser.add_argument(
        '--parms-file',
        default='S12-parms.localtest.txt',
        help='Parameter file used to configure comparator and cluster gate.'
    )
    parser.add_argument(
        '--capture-root',
        default=os.path.join('..', 'KNN10__Tahacomparator'),
        help='Path to a captured DWM run folder containing ref/token/block CSV files.'
    )
    parser.add_argument(
        '--iteration',
        type=int,
        default=1,
        help='Iteration number to replay from the capture folder.'
    )
    parser.add_argument('--ref-dict', default='', help='Override refDict CSV path.')
    parser.add_argument('--token-freq', default='', help='Override tokenFreqDict CSV path.')
    parser.add_argument('--link-index', default='', help='Override linkIndex CSV path.')
    parser.add_argument('--block-pairs', default='', help='Override blockPairList CSV path.')
    parser.add_argument(
        '--output-dir',
        default='',
        help='Directory for replay artifacts. Default: capture_root/replay_iteration_XX_<timestamp>.'
    )
    args = parser.parse_args()

    bundle_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(bundle_dir)

    capture_root = os.path.abspath(args.capture_root)
    ref_dict_path, token_freq_path, link_index_path, block_pair_path, iteration_dir = _default_paths(
        capture_root, args.iteration
    )
    if args.ref_dict.strip():
        ref_dict_path = os.path.abspath(args.ref_dict)
    if args.token_freq.strip():
        token_freq_path = os.path.abspath(args.token_freq)
    if args.link_index.strip():
        link_index_path = os.path.abspath(args.link_index)
    if args.block_pairs.strip():
        block_pair_path = os.path.abspath(args.block_pairs)

    required_paths = {
        'ref_dict': ref_dict_path,
        'token_freq': token_freq_path,
        'link_index': link_index_path,
        'block_pairs': block_pair_path,
    }
    missing = [name for name, path in required_paths.items() if not path or not os.path.exists(path)]
    if missing:
        raise FileNotFoundError('Missing replay inputs: ' + ', '.join(missing))

    tag = datetime.datetime.now().strftime('%Y%m%d_%H_%M_%S')
    if args.output_dir.strip():
        output_dir = os.path.abspath(args.output_dir)
    else:
        output_dir = os.path.join(
            capture_root, 'replay_iteration_' + str(args.iteration).zfill(2) + '_' + tag
        )
    os.makedirs(output_dir, exist_ok=True)

    log_path = os.path.join(output_dir, 'replay_log.txt')
    log_file = open(log_path, 'w', encoding='utf-8')

    DWM10_Parms.getParms(args.parms_file, log_file)
    truth_dict = DWM_DataCapture.load_truth_dict(DWM10_Parms.truthFileName)

    _print_both('Replay mode: captured block-pair evaluation', log_file)
    _print_both('Capture root = ' + capture_root, log_file)
    _print_both('Iteration dir = ' + iteration_dir, log_file)
    _print_both('refDict path = ' + ref_dict_path, log_file)
    _print_both('tokenFreq path = ' + token_freq_path, log_file)
    _print_both('linkIndex path = ' + link_index_path, log_file)
    _print_both('blockPair path = ' + block_pair_path, log_file)
    _print_both('output dir = ' + output_dir, log_file)

    ref_dict = _load_ref_dict(ref_dict_path)
    token_freq_dict = _load_token_freq_dict(token_freq_path)
    link_index = _load_link_index(link_index_path)
    block_pair_list = _load_block_pair_list(block_pair_path)

    _print_both('Loaded references = ' + str(len(ref_dict)), log_file)
    _print_both('Loaded token frequencies = ' + str(len(token_freq_dict)), log_file)
    _print_both('Loaded linkIndex rows = ' + str(len(link_index)), log_file)
    _print_both('Loaded block pairs = ' + str(len(block_pair_list)), log_file)

    if truth_dict:
        DWM99_ERmetrics.generateBlockingMetrics(block_pair_list, args.iteration, ref_dict)

    linked_pair_list = DWM55_LinkBlockPairs.linkBlockPairs(block_pair_list, ref_dict, token_freq_dict)
    DWM_DataCapture.save_linked_pair_list(
        linked_pair_list,
        os.path.join(output_dir, '07_linkedPairList.csv'),
        ref_dict,
        truth_dict
    )
    DWM_DataCapture.save_pair_comparison_view(
        [a + '|' + b for a, b in linked_pair_list],
        os.path.join(output_dir, '07_linkedPairList'),
        ref_dict,
        token_freq_dict,
        truth_dict
    )

    if DWM10_Parms.comparator in ('TahaComparator', 'ScoringMatrixTaha'):
        decision_rows = DWM67_Tahacomparator.get_last_decisions()
        DWM_DataCapture.save_taha_decision_view(
            decision_rows,
            os.path.join(output_dir, '07_tahaComparator'),
            ref_dict,
            truth_dict
        )
        stats = DWM67_Tahacomparator.get_run_stats()
        for key in sorted(stats.keys()):
            _print_both('taha_stat ' + str(key) + ' = ' + str(stats[key]), log_file)

    cluster_list = DWM80_TransitiveClosure.transitiveClosure(linked_pair_list)
    cluster_list_for_save = cluster_list.copy()
    DWM_DataCapture.save_cluster_list(
        cluster_list_for_save,
        os.path.join(output_dir, '08_clusterList.csv'),
        ref_dict,
        truth_dict
    )
    DWM_DataCapture.save_cluster_json(
        cluster_list_for_save,
        os.path.join(output_dir, '08_clusterList.json'),
        ref_dict
    )

    DWM90_IterateClusters.iterateClusters(cluster_list, ref_dict, link_index)
    DWM_DataCapture.save_link_index(
        link_index,
        os.path.join(output_dir, '09_linkIndex.csv'),
        ref_dict
    )

    if truth_dict:
        DWM99_ERmetrics.generateMetrics(link_index)

    _print_both('Replay complete.', log_file)
    _print_both('Artifacts written to ' + output_dir, log_file)
    log_file.close()


if __name__ == '__main__':
    main()
