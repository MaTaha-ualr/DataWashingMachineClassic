#!/usr/bin/env python
# coding: utf-8

import argparse
import csv
import datetime as dt
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path


VARIANT_SPECS = {
    'cosine': {
        'display_name': 'Cosine',
        'overrides': {
            'comparator': 'Cosine',
        },
    },
    'monge-elkan': {
        'display_name': 'MongeElkan',
        'overrides': {
            'comparator': 'MongeElkan',
        },
    },
    'scoring-matrix-std': {
        'display_name': 'ScoringMatrixStd',
        'overrides': {
            'comparator': 'ScoringMatrixStd',
        },
    },
    'scoring-matrix-kris': {
        'display_name': 'ScoringMatrixKris',
        'overrides': {
            'comparator': 'ScoringMatrixKris',
        },
    },
    'taha-baseline': {
        'display_name': 'TahaComparator',
        'overrides': {
            'comparator': 'TahaComparator',
            'tahaUseSoftRoleScoring': 'False',
            'tahaUseProvisionalContext': 'False',
            'tahaUseSchemaLightClusterQuality': 'False',
        },
    },
    'taha-soft-only': {
        'display_name': 'TahaComparator-SoftOnly',
        'overrides': {
            'comparator': 'TahaComparator',
            'tahaUseSoftRoleScoring': 'True',
            'tahaUseProvisionalContext': 'False',
            'tahaUseSchemaLightClusterQuality': 'False',
            'tahaUseOpenAIReview': 'False',
            'tahaUseAsyncOpenAIReview': 'False',
            'tahaUseAnchorBatchReview': 'False',
        },
    },
    'taha-context-only': {
        'display_name': 'TahaComparator-ContextOnly',
        'overrides': {
            'comparator': 'TahaComparator',
            'tahaUseSoftRoleScoring': 'False',
            'tahaUseProvisionalContext': 'True',
            'tahaUseSchemaLightClusterQuality': 'False',
            'tahaUseOpenAIReview': 'False',
            'tahaUseAsyncOpenAIReview': 'False',
            'tahaUseAnchorBatchReview': 'False',
        },
    },
    'taha-cx': {
        'display_name': 'TahaComparator-CX',
        'overrides': {
            'comparator': 'TahaComparator',
            'tahaUseSoftRoleScoring': 'True',
            'tahaUseProvisionalContext': 'True',
            'tahaUseSchemaLightClusterQuality': 'False',
            'tahaUseOpenAIReview': 'False',
            'tahaUseAsyncOpenAIReview': 'False',
            'tahaUseAnchorBatchReview': 'False',
        },
    },
}

DEFAULT_VARIANTS = [
    'cosine',
    'monge-elkan',
    'scoring-matrix-std',
    'scoring-matrix-kris',
]

PATH_PARMS = {
    'inputFileName',
    'truthFileName',
    'tahaAliasFile',
    'tahaLlmDecisionFile',
}


def parse_args():
    parser = argparse.ArgumentParser(
        description='Systematic DWM comparator benchmark runner.'
    )
    parser.add_argument(
        '--base-parms',
        required=True,
        help='Base parameter file used as the fixed experiment template.',
    )
    parser.add_argument(
        '--variants',
        nargs='+',
        default=DEFAULT_VARIANTS,
        help='Variant keys to benchmark. Default compares the four classic comparators.',
    )
    parser.add_argument(
        '--output-root',
        default='benchmark_runs',
        help='Directory where benchmark artifacts and summaries are written.',
    )
    parser.add_argument(
        '--run-label',
        default='',
        help='Optional fixed benchmark directory name. Use this with --resume on Colab.',
    )
    parser.add_argument(
        '--python-exe',
        default=sys.executable,
        help='Python interpreter used to launch DWM00_Driver.py.',
    )
    parser.add_argument(
        '--force-embedding-device',
        default='cpu',
        help='Override embeddingDevice for every run. Use empty string to keep base setting.',
    )
    parser.add_argument(
        '--disable-openai-review',
        action='store_true',
        help='Force-disable OpenAI review for Taha variants to keep runs deterministic.',
    )
    parser.add_argument(
        '--keep-failed',
        action='store_true',
        help='Keep partially produced artifacts for failed runs.',
    )
    parser.add_argument(
        '--prepare-only',
        action='store_true',
        help='Only generate parameter snapshots and manifest, do not launch DWM runs.',
    )
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Skip variants whose summary row already exists with status=success.',
    )
    return parser.parse_args()


def parse_param_file(parm_path):
    parm_path = Path(parm_path).resolve()
    lines = parm_path.read_text(encoding='utf-8').splitlines()
    values = {}
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        values[key.strip()] = value.strip()
    return parm_path, lines, values


def resolve_path_value(base_dir, value):
    value = str(value).strip()
    if value == '':
        return value
    path = Path(value)
    if path.is_absolute():
        return str(path)
    return str((base_dir / path).resolve())


def rewrite_param_lines(base_lines, base_values, overrides, base_dir):
    merged = dict(base_values)
    merged.update(overrides)

    rewritten = []
    seen = set()
    for raw in base_lines:
        stripped = raw.strip()
        if stripped and not stripped.startswith('#') and '=' in raw:
            key, _ = raw.split('=', 1)
            key = key.strip()
            if key in merged:
                value = merged[key]
                if key in PATH_PARMS and str(value).strip() != '':
                    value = resolve_path_value(base_dir, value)
                rewritten.append(f'{key}={value}')
                seen.add(key)
                continue
        rewritten.append(raw)

    for key, value in merged.items():
        if key in seen:
            continue
        if key in PATH_PARMS and str(value).strip() != '':
            value = resolve_path_value(base_dir, value)
        rewritten.append(f'{key}={value}')
        seen.add(key)

    return '\n'.join(rewritten) + '\n'


def collect_source_state(source_dir, dataset_linkindex_name):
    data_capture_dir = source_dir / 'data_capture'
    capture_subdirs = set()
    if data_capture_dir.exists():
        capture_subdirs = {p.name for p in data_capture_dir.iterdir() if p.is_dir()}

    return {
        'logs': {p.name for p in source_dir.glob('DWM_Log_*.txt')},
        'results': {p.name for p in source_dir.glob('DWM_Results_*.xlsx')},
        'capture_subdirs': capture_subdirs,
        'linkindex_exists': (source_dir / dataset_linkindex_name).exists(),
    }


def move_new_artifacts(source_dir, before_state, run_dir, dataset_linkindex_name):
    run_dir.mkdir(parents=True, exist_ok=True)
    moved = {
        'logs': [],
        'results': [],
        'capture_dirs': [],
        'linkindex': '',
    }

    for path in sorted(source_dir.glob('DWM_Log_*.txt')):
        if path.name not in before_state['logs']:
            target = run_dir / path.name
            shutil.move(str(path), str(target))
            moved['logs'].append(str(target))

    for path in sorted(source_dir.glob('DWM_Results_*.xlsx')):
        if path.name not in before_state['results']:
            target = run_dir / path.name
            shutil.move(str(path), str(target))
            moved['results'].append(str(target))

    capture_root = source_dir / 'data_capture'
    if capture_root.exists():
        target_capture_root = run_dir / 'data_capture'
        target_capture_root.mkdir(parents=True, exist_ok=True)
        for subdir in sorted(capture_root.iterdir()):
            if subdir.is_dir() and subdir.name not in before_state['capture_subdirs']:
                target = target_capture_root / subdir.name
                shutil.move(str(subdir), str(target))
                moved['capture_dirs'].append(str(target))
        if not any(capture_root.iterdir()):
            capture_root.rmdir()

    linkindex_path = source_dir / dataset_linkindex_name
    if linkindex_path.exists():
        target = run_dir / f'{run_dir.name}_{dataset_linkindex_name}'
        shutil.copy2(str(linkindex_path), str(target))
        moved['linkindex'] = str(target)

    return moved


def cleanup_failed_artifacts(source_dir, before_state, dataset_linkindex_name):
    for path in sorted(source_dir.glob('DWM_Log_*.txt')):
        if path.name not in before_state['logs']:
            path.unlink(missing_ok=True)
    for path in sorted(source_dir.glob('DWM_Results_*.xlsx')):
        if path.name not in before_state['results']:
            path.unlink(missing_ok=True)
    capture_root = source_dir / 'data_capture'
    if capture_root.exists():
        for subdir in sorted(capture_root.iterdir()):
            if subdir.is_dir() and subdir.name not in before_state['capture_subdirs']:
                shutil.rmtree(subdir, ignore_errors=True)
        if not any(capture_root.iterdir()):
            capture_root.rmdir()
    linkindex_path = source_dir / dataset_linkindex_name
    if not before_state['linkindex_exists'] and linkindex_path.exists():
        linkindex_path.unlink(missing_ok=True)


def parse_log_metrics(log_path):
    text = Path(log_path).read_text(encoding='utf-8', errors='ignore')

    def last_float(pattern):
        matches = re.findall(pattern, text, flags=re.MULTILINE)
        return float(matches[-1]) if matches else None

    def last_text(pattern):
        matches = re.findall(pattern, text, flags=re.MULTILINE)
        return matches[-1].strip() if matches else ''

    metrics = {
        'true_pairs': last_float(r'^True Pairs =\s*([0-9.]+)\s*$'),
        'expected_pairs': last_float(r'^Expected Pairs =\s*([0-9.]+)\s*$'),
        'linked_pairs': last_float(r'^Linked Pairs =\s*([0-9.]+)\s*$'),
        'precision': last_float(r'^Precision=\s*([0-9.]+)\s*$'),
        'recall': last_float(r'^Recall=\s*([0-9.]+)\s*$'),
        'f_measure': last_float(r'^F-measure=\s*([0-9.]+)\s*$'),
        'runtime_minutes': last_float(r'^All Files Total Runtime =\s*([0-9.]+)\s+minutes\s*$'),
        'runtime_text': last_text(r'^Total File Runtime =\s*(.+?)\s*$'),
        'iterations': len(re.findall(r'^\*\*\*\*New Iteration', text, flags=re.MULTILINE)),
        'block_precision_final': last_float(r'^Block Precision =\s*([0-9.]+)\s*$'),
        'block_recall_final': last_float(r'^Block Recall =\s*([0-9.]+)\s*$'),
        'block_f_measure_final': last_float(r'^Block F-measure =\s*([0-9.]+)\s*$'),
        'context_revisions': last_float(r'^TahaComparator Context Pass Revisions =\s*([0-9.]+)\s*$'),
        'llm_review_pairs': last_float(r'^TahaComparator LLM-Review Pairs =\s*([0-9.]+)\s*$'),
        'openai_review_requests': last_float(r'^TahaComparator OpenAI Review Requests =\s*([0-9.]+)\s*$'),
    }
    return metrics


def write_csv(path, rows):
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with open(path, 'w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _fmt(value, digits=4):
    if value is None or value == '':
        return ''
    if isinstance(value, (int, float)):
        return f'{value:.{digits}f}'
    return str(value)


def write_markdown(path, rows, manifest):
    lines = []
    lines.append('# Comparator Benchmark Summary')
    lines.append('')
    lines.append('This benchmark keeps the dataset and the non-comparator DWM pipeline fixed unless an explicit override is recorded in the manifest.')
    lines.append('')
    lines.append('## Configuration')
    lines.append('')
    lines.append(f'- Created: `{manifest["created_at"]}`')
    lines.append(f'- Base parms: `{manifest["base_parms"]}`')
    lines.append(f'- Input file: `{manifest["input_file"]}`')
    lines.append(f'- Truth file: `{manifest["truth_file"]}`')
    lines.append(f'- Forced embedding device: `{manifest["force_embedding_device"]}`')
    lines.append(f'- Disable OpenAI review: `{manifest["disable_openai_review"]}`')
    lines.append('')
    lines.append('## Results')
    lines.append('')
    lines.append('| Variant | Comparator | Status | Precision | Recall | F1 | Linked Pairs | Runtime (min) | Iterations |')
    lines.append('|---|---|---|---:|---:|---:|---:|---:|---:|')
    for row in rows:
        lines.append(
            '| {variant_key} | {comparator} | {status} | {precision} | {recall} | {f_measure} | {linked_pairs} | {runtime_minutes} | {iterations} |'.format(
                variant_key=row['variant_key'],
                comparator=row['comparator'],
                status=row['status'],
                precision=_fmt(row['precision']),
                recall=_fmt(row['recall']),
                f_measure=_fmt(row['f_measure']),
                linked_pairs=_fmt(row['linked_pairs'], digits=0),
                runtime_minutes=_fmt(row['runtime_minutes'], digits=2),
                iterations=_fmt(row['iterations'], digits=0),
            )
        )
    lines.append('')
    lines.append('## Run Directories')
    lines.append('')
    for row in rows:
        lines.append(f'- `{row["variant_key"]}`: `{row["run_dir"]}`')
    Path(path).write_text('\n'.join(lines) + '\n', encoding='utf-8')


def read_existing_summary(summary_csv):
    if not summary_csv.exists():
        return {}
    rows = {}
    with open(summary_csv, 'r', newline='', encoding='utf-8') as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows[row['variant_key']] = row
    return rows


def main():
    args = parse_args()

    base_parms_path, base_lines, base_values = parse_param_file(args.base_parms)
    base_dir = base_parms_path.parent
    source_dir = Path(__file__).resolve().parent
    driver_path = source_dir / 'DWM00_Driver.py'

    if not driver_path.exists():
        raise FileNotFoundError(f'Driver not found: {driver_path}')

    input_file_value = base_values.get('inputFileName', '')
    truth_file_value = base_values.get('truthFileName', '')
    input_file_name = Path(input_file_value).name
    dataset_stem = Path(input_file_name).stem
    dataset_linkindex_name = f'{dataset_stem}-LinkIndex.txt'

    output_root = Path(args.output_root).resolve()
    if args.run_label:
        benchmark_root = output_root / args.run_label
    else:
        timestamp = dt.datetime.now().strftime('%Y%m%d_%H%M%S')
        benchmark_root = output_root / f'comparator_benchmark_{dataset_stem}_{timestamp}'
    benchmark_root.mkdir(parents=True, exist_ok=True)

    summary_csv = benchmark_root / 'summary.csv'
    summary_md = benchmark_root / 'summary.md'
    manifest_path = benchmark_root / 'manifest.json'

    manifest = {
        'created_at': dt.datetime.now().isoformat(),
        'base_parms': str(base_parms_path),
        'input_file': resolve_path_value(base_dir, input_file_value),
        'truth_file': resolve_path_value(base_dir, truth_file_value) if truth_file_value else '',
        'force_embedding_device': args.force_embedding_device,
        'disable_openai_review': bool(args.disable_openai_review),
        'variants': args.variants,
        'source_dir': str(source_dir),
        'driver_path': str(driver_path),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding='utf-8')

    existing_summary = read_existing_summary(summary_csv) if args.resume else {}
    rows = []

    linkindex_backup_path = benchmark_root / f'backup_{dataset_linkindex_name}'
    original_linkindex = source_dir / dataset_linkindex_name
    if original_linkindex.exists():
        shutil.copy2(str(original_linkindex), str(linkindex_backup_path))

    try:
        for variant_key in args.variants:
            if variant_key not in VARIANT_SPECS:
                raise ValueError(f'Unknown variant: {variant_key}')

            if args.resume:
                existing = existing_summary.get(variant_key)
                if existing and existing.get('status') == 'success':
                    rows.append(existing)
                    continue

            spec = VARIANT_SPECS[variant_key]
            overrides = dict(spec['overrides'])
            if args.force_embedding_device:
                overrides['embeddingDevice'] = args.force_embedding_device
            if args.disable_openai_review:
                overrides['tahaUseOpenAIReview'] = 'False'
                overrides['tahaUseAsyncOpenAIReview'] = 'False'
                overrides['tahaUseAnchorBatchReview'] = 'False'
                overrides['tahaOpenAIReviewMaxPairs'] = '0'

            run_dir = benchmark_root / variant_key
            run_dir.mkdir(parents=True, exist_ok=True)

            parm_text = rewrite_param_lines(base_lines, base_values, overrides, base_dir)
            parm_path = run_dir / f'{variant_key}.parms.txt'
            parm_path.write_text(parm_text, encoding='utf-8')

            row = {
                'variant_key': variant_key,
                'comparator': spec['display_name'],
                'status': 'prepared' if args.prepare_only else 'pending',
                'precision': '',
                'recall': '',
                'f_measure': '',
                'linked_pairs': '',
                'expected_pairs': '',
                'true_pairs': '',
                'runtime_minutes': '',
                'runtime_text': '',
                'iterations': '',
                'block_precision_final': '',
                'block_recall_final': '',
                'block_f_measure_final': '',
                'llm_review_pairs': '',
                'openai_review_requests': '',
                'context_revisions': '',
                'parm_file': str(parm_path),
                'run_dir': str(run_dir),
                'log_file': '',
                'result_xlsx': '',
                'capture_dir': '',
                'linkindex_copy': '',
                'notes': '',
            }

            if args.prepare_only:
                rows.append(row)
                continue

            before_state = collect_source_state(source_dir, dataset_linkindex_name)
            stdout_path = run_dir / 'driver_stdout.txt'
            command = [
                args.python_exe,
                str(driver_path),
                '--parms-file',
                str(parm_path),
            ]

            with open(stdout_path, 'w', encoding='utf-8') as stdout_handle:
                stdout_handle.write('COMMAND: ' + ' '.join(command) + '\n')
                stdout_handle.write('CWD: ' + str(source_dir) + '\n\n')
                completed = subprocess.run(
                    command,
                    cwd=str(source_dir),
                    stdout=stdout_handle,
                    stderr=subprocess.STDOUT,
                    check=False,
                )

            if completed.returncode != 0:
                row['status'] = 'failed'
                row['notes'] = f'DWM00_Driver.py exited with code {completed.returncode}'
                if not args.keep_failed:
                    cleanup_failed_artifacts(source_dir, before_state, dataset_linkindex_name)
                else:
                    moved = move_new_artifacts(source_dir, before_state, run_dir, dataset_linkindex_name)
                    row['log_file'] = '; '.join(moved['logs'])
                    row['result_xlsx'] = '; '.join(moved['results'])
                    row['capture_dir'] = '; '.join(moved['capture_dirs'])
                    row['linkindex_copy'] = moved['linkindex']
                rows.append(row)
                write_csv(summary_csv, rows)
                write_markdown(summary_md, rows, manifest)
                continue

            moved = move_new_artifacts(source_dir, before_state, run_dir, dataset_linkindex_name)
            if not moved['logs']:
                row['status'] = 'failed'
                row['notes'] = 'Run completed but no new DWM log was detected.'
                if not args.keep_failed:
                    shutil.rmtree(run_dir, ignore_errors=True)
                rows.append(row)
                write_csv(summary_csv, rows)
                write_markdown(summary_md, rows, manifest)
                continue

            log_path = Path(moved['logs'][0])
            metrics = parse_log_metrics(log_path)

            row.update({
                'status': 'success',
                'precision': metrics['precision'],
                'recall': metrics['recall'],
                'f_measure': metrics['f_measure'],
                'linked_pairs': metrics['linked_pairs'],
                'expected_pairs': metrics['expected_pairs'],
                'true_pairs': metrics['true_pairs'],
                'runtime_minutes': metrics['runtime_minutes'],
                'runtime_text': metrics['runtime_text'],
                'iterations': metrics['iterations'],
                'block_precision_final': metrics['block_precision_final'],
                'block_recall_final': metrics['block_recall_final'],
                'block_f_measure_final': metrics['block_f_measure_final'],
                'llm_review_pairs': metrics['llm_review_pairs'],
                'openai_review_requests': metrics['openai_review_requests'],
                'context_revisions': metrics['context_revisions'],
                'log_file': '; '.join(moved['logs']),
                'result_xlsx': '; '.join(moved['results']),
                'capture_dir': '; '.join(moved['capture_dirs']),
                'linkindex_copy': moved['linkindex'],
            })
            rows.append(row)
            write_csv(summary_csv, rows)
            write_markdown(summary_md, rows, manifest)

    finally:
        if linkindex_backup_path.exists():
            shutil.copy2(str(linkindex_backup_path), str(original_linkindex))
        else:
            original_linkindex.unlink(missing_ok=True)

    write_csv(summary_csv, rows)
    write_markdown(summary_md, rows, manifest)

    print(f'Benchmark root: {benchmark_root}')
    print(f'Summary CSV: {summary_csv}')
    print(f'Summary MD: {summary_md}')


if __name__ == '__main__':
    main()
