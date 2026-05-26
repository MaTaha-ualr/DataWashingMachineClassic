#!/usr/bin/env python
# coding: utf-8

import argparse
import csv
import datetime as dt
import json
import subprocess
import sys
from pathlib import Path

import DWM_Comparator_Benchmark as single_bench


DEFAULT_VARIANTS = [
    'cosine',
    'monge-elkan',
    'scoring-matrix-std',
    'scoring-matrix-kris',
    'taha-baseline',
    'taha-cx',
]


def parse_args():
    parser = argparse.ArgumentParser(
        description='Run DWM comparator benchmarks across many parameter files.'
    )
    parser.add_argument(
        '--parms-glob',
        default='data/*-parms.txt',
        help='Glob pattern for dataset parameter files, relative to repo root unless absolute.',
    )
    parser.add_argument(
        '--variants',
        nargs='+',
        default=DEFAULT_VARIANTS,
        help='Variant keys to benchmark for each dataset.',
    )
    parser.add_argument(
        '--output-root',
        default='benchmark_runs_all',
        help='Root directory for all dataset benchmark outputs.',
    )
    parser.add_argument(
        '--run-label',
        default='',
        help='Optional fixed root run label for the multi-dataset benchmark.',
    )
    parser.add_argument(
        '--python-exe',
        default=sys.executable,
        help='Python interpreter used to launch the single-dataset benchmark runner.',
    )
    parser.add_argument(
        '--force-embedding-device',
        default='cpu',
        help='Override embeddingDevice for every run. Use empty string to keep per-file settings.',
    )
    parser.add_argument(
        '--disable-openai-review',
        action='store_true',
        help='Force-disable OpenAI review to keep runs deterministic.',
    )
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Resume previously completed dataset/variant runs.',
    )
    parser.add_argument(
        '--keep-failed',
        action='store_true',
        help='Keep failed single-dataset artifacts.',
    )
    parser.add_argument(
        '--prepare-only',
        action='store_true',
        help='Prepare all dataset benchmark folders without launching DWM.',
    )
    return parser.parse_args()


def _fmt(value, digits=4):
    if value in (None, ''):
        return ''
    try:
        num = float(value)
    except (TypeError, ValueError):
        return str(value)
    if digits == 0:
        return str(int(round(num)))
    return f'{num:.{digits}f}'


def write_csv(path, rows):
    if not rows:
        return
    with open(path, 'w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_markdown(path, rows, manifest):
    lines = []
    lines.append('# All-Datasets Comparator Benchmark Summary')
    lines.append('')
    lines.append('This benchmark runs the same comparator set across multiple dataset parameter files and aggregates the per-dataset results.')
    lines.append('')
    lines.append('## Configuration')
    lines.append('')
    lines.append(f'- Created: `{manifest["created_at"]}`')
    lines.append(f'- Parameter glob: `{manifest["parms_glob"]}`')
    lines.append(f'- Forced embedding device: `{manifest["force_embedding_device"]}`')
    lines.append(f'- Disable OpenAI review: `{manifest["disable_openai_review"]}`')
    lines.append(f'- Variants: `{", ".join(manifest["variants"])}`')
    lines.append('')
    lines.append('## Results')
    lines.append('')
    lines.append('| Dataset | Variant | Comparator | Status | Precision | Recall | F1 | Linked Pairs | Runtime (min) | Iterations |')
    lines.append('|---|---|---|---|---:|---:|---:|---:|---:|---:|')
    for row in rows:
        lines.append(
            '| {dataset} | {variant_key} | {comparator} | {status} | {precision} | {recall} | {f_measure} | {linked_pairs} | {runtime_minutes} | {iterations} |'.format(
                dataset=row['dataset'],
                variant_key=row['variant_key'],
                comparator=row['comparator'],
                status=row['status'],
                precision=_fmt(row['precision']),
                recall=_fmt(row['recall']),
                f_measure=_fmt(row['f_measure']),
                linked_pairs=_fmt(row['linked_pairs'], 0),
                runtime_minutes=_fmt(row['runtime_minutes'], 2),
                iterations=_fmt(row['iterations'], 0),
            )
        )
    lines.append('')
    lines.append('## Dataset Benchmark Roots')
    lines.append('')
    roots = []
    seen = set()
    for row in rows:
        if row['dataset_run_root'] in seen:
            continue
        seen.add(row['dataset_run_root'])
        roots.append((row['dataset'], row['dataset_run_root']))
    for dataset, run_root in roots:
        lines.append(f'- `{dataset}`: `{run_root}`')
    path.write_text('\n'.join(lines) + '\n', encoding='utf-8')


def main():
    args = parse_args()
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent

    parms_glob = args.parms_glob
    parms_path = Path(parms_glob)
    if parms_path.is_absolute():
        discovered = sorted(parms_path.parent.glob(parms_path.name))
    else:
        discovered = sorted(repo_root.glob(parms_glob))

    if not discovered:
        raise FileNotFoundError(f'No parameter files matched: {parms_glob}')

    for variant in args.variants:
        if variant not in single_bench.VARIANT_SPECS:
            raise ValueError(f'Unknown variant: {variant}')

    timestamp = dt.datetime.now().strftime('%Y%m%d_%H%M%S')
    output_root = Path(args.output_root).resolve()
    if args.run_label:
        multi_root = output_root / args.run_label
    else:
        multi_root = output_root / f'all_datasets_benchmark_{timestamp}'
    multi_root.mkdir(parents=True, exist_ok=True)

    dataset_runs_root = multi_root / 'dataset_runs'
    dataset_runs_root.mkdir(parents=True, exist_ok=True)
    summary_csv = multi_root / 'summary.csv'
    summary_md = multi_root / 'summary.md'
    manifest_path = multi_root / 'manifest.json'

    manifest = {
        'created_at': dt.datetime.now().isoformat(),
        'repo_root': str(repo_root),
        'parms_glob': parms_glob,
        'force_embedding_device': args.force_embedding_device,
        'disable_openai_review': bool(args.disable_openai_review),
        'variants': args.variants,
        'datasets': [str(p.resolve()) for p in discovered],
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding='utf-8')

    all_rows = []
    single_script = script_dir / 'DWM_Comparator_Benchmark.py'

    for parm_file in discovered:
        dataset_label = parm_file.stem
        dataset_run_root = dataset_runs_root / dataset_label
        dataset_run_root.mkdir(parents=True, exist_ok=True)
        driver_stdout = dataset_run_root / 'batch_stdout.txt'
        command = [
            args.python_exe,
            str(single_script),
            '--base-parms',
            str(parm_file.resolve()),
            '--variants',
            *args.variants,
            '--output-root',
            str(dataset_runs_root),
            '--run-label',
            dataset_label,
        ]
        if args.force_embedding_device:
            command.extend(['--force-embedding-device', args.force_embedding_device])
        if args.disable_openai_review:
            command.append('--disable-openai-review')
        if args.resume:
            command.append('--resume')
        if args.keep_failed:
            command.append('--keep-failed')
        if args.prepare_only:
            command.append('--prepare-only')

        with open(driver_stdout, 'a', encoding='utf-8') as handle:
            handle.write('COMMAND: ' + ' '.join(command) + '\n')
            handle.write('START: ' + dt.datetime.now().isoformat() + '\n')
            completed = subprocess.run(
                command,
                cwd=str(repo_root),
                stdout=handle,
                stderr=subprocess.STDOUT,
                check=False,
            )
            handle.write('END: ' + dt.datetime.now().isoformat() + '\n')
            handle.write('RETURN_CODE: ' + str(completed.returncode) + '\n\n')

        dataset_summary = dataset_run_root / 'summary.csv'
        if not dataset_summary.exists():
            all_rows.append({
                'dataset': dataset_label,
                'parm_file': str(parm_file.resolve()),
                'variant_key': '',
                'comparator': '',
                'status': 'failed_to_start',
                'precision': '',
                'recall': '',
                'f_measure': '',
                'linked_pairs': '',
                'runtime_minutes': '',
                'iterations': '',
                'notes': f'Single benchmark summary missing. Return code = {completed.returncode}',
                'dataset_run_root': str(dataset_run_root),
                'variant_run_root': '',
            })
            continue

        with open(dataset_summary, 'r', newline='', encoding='utf-8') as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                all_rows.append({
                    'dataset': dataset_label,
                    'parm_file': str(parm_file.resolve()),
                    'variant_key': row.get('variant_key', ''),
                    'comparator': row.get('comparator', ''),
                    'status': row.get('status', ''),
                    'precision': row.get('precision', ''),
                    'recall': row.get('recall', ''),
                    'f_measure': row.get('f_measure', ''),
                    'linked_pairs': row.get('linked_pairs', ''),
                    'runtime_minutes': row.get('runtime_minutes', ''),
                    'iterations': row.get('iterations', ''),
                    'notes': row.get('notes', ''),
                    'dataset_run_root': str(dataset_run_root),
                    'variant_run_root': row.get('run_dir', ''),
                })

        write_csv(summary_csv, all_rows)
        write_markdown(summary_md, all_rows, manifest)

    write_csv(summary_csv, all_rows)
    write_markdown(summary_md, all_rows, manifest)
    print(f'All-datasets benchmark root: {multi_root}')
    print(f'Summary CSV: {summary_csv}')
    print(f'Summary MD: {summary_md}')


if __name__ == '__main__':
    main()
