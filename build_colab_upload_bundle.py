#!/usr/bin/env python

import argparse
import fnmatch
import shutil
from pathlib import Path


EXCLUDED_DIRS = {
    "__pycache__",
    "data_capture",
    "benchmark_runs",
    "benchmark_runs_test",
    "benchmark_runs_batch_test",
}

EXCLUDED_FILES = {
    "Data_Parms_Truth.zip",
}

EXCLUDED_PATTERNS = (
    "DWM_Log_*.txt",
    "DWM_Results_*.xlsx",
    "*-LinkIndex.txt",
    "*.pyc",
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Build a clean Colab upload zip for DataWashingMachineClassic."
    )
    parser.add_argument(
        "--zip-name",
        default="DataWashingMachineClassic_ColabUpload.zip",
        help="Output zip filename written at the repository root.",
    )
    return parser.parse_args()


def should_copy(path: Path) -> bool:
    name = path.name
    if path.is_dir() and name in EXCLUDED_DIRS:
        return False
    if path.is_file():
        if name in EXCLUDED_FILES:
            return False
        if any(fnmatch.fnmatch(name, pattern) for pattern in EXCLUDED_PATTERNS):
            return False
    return True


def copy_tree(src: Path, dst: Path) -> None:
    for item in src.iterdir():
        if not should_copy(item):
            continue
        target = dst / item.name
        if item.is_dir():
            target.mkdir(parents=True, exist_ok=True)
            copy_tree(item, target)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(item, target)


def copy_optional_file(repo_root: Path, package_root: Path, relative_name: str) -> None:
    source = repo_root / relative_name
    if source.exists() and should_copy(source):
        target = package_root / relative_name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)


def main():
    args = parse_args()
    repo_root = Path(__file__).resolve().parent
    stage_root = repo_root / "colab_upload_build"
    package_root = stage_root / "DataWashingMachineClassic"
    zip_path = repo_root / args.zip_name

    if stage_root.exists():
        shutil.rmtree(stage_root)
    if zip_path.exists():
        zip_path.unlink()

    package_root.mkdir(parents=True, exist_ok=True)

    copy_tree(repo_root / "DWM_colab_bundle", package_root / "DWM_colab_bundle")
    copy_tree(repo_root / "Data files", package_root / "Data files")

    for relative_name in ("README.md", "LICENSE", "CITATION.cff", "requirements.txt"):
        copy_optional_file(repo_root, package_root, relative_name)

    shutil.make_archive(str(zip_path.with_suffix("")), "zip", stage_root, package_root.name)
    shutil.rmtree(stage_root)

    print(f"Created zip: {zip_path}")


if __name__ == "__main__":
    main()
