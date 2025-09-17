#!/usr/bin/env python3

"""Run the TapeDeck automation scripts in sequence."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Sequence


SCRIPT_SEQUENCE: Sequence[str] = (
    "import.py",
    "media.py",
    "online.py",
    "api.py",
    "prep.py",
    "upload.py",
    "cleanup.py",
)


def run_script(script_path: Path, verbose: bool) -> None:
    """Execute ``script_path`` and raise ``RuntimeError`` if it fails."""

    cmd = [sys.executable, str(script_path)]
    if verbose:
        cmd.append("-v")

    if verbose:
        print(f"Running {script_path.name}...")

    try:
        result = subprocess.run(cmd, text=True, capture_output=True)
    except KeyboardInterrupt:
        raise
    except Exception as exc:  # pragma: no cover - defensive catch
        raise RuntimeError(f"Failed to run {script_path.name}: {exc}") from exc

    if result.returncode != 0:
        details = []
        if result.stderr:
            details.append(result.stderr.strip())
        if result.stdout:
            details.append(result.stdout.strip())
        message = f"{script_path.name} exited with code {result.returncode}"
        if details:
            message = f"{message}\n" + "\n".join(details)
        raise RuntimeError(message)

    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, file=sys.stderr, end="")


def main() -> None:
    """Entry point for executing the automation pipeline."""

    parser = argparse.ArgumentParser(
        description="Run the TapeDeck automation pipeline sequentially."
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Show progress messages and pass -v to each script.",
    )
    args = parser.parse_args()

    scripts_dir = Path(__file__).resolve().parent / "scripts"
    if not scripts_dir.is_dir():
        print(f"Scripts directory not found: {scripts_dir}", file=sys.stderr)
        sys.exit(1)

    try:
        for script_name in SCRIPT_SEQUENCE:
            script_path = scripts_dir / script_name
            if not script_path.is_file():
                raise FileNotFoundError(f"Script not found: {script_path}")
            run_script(script_path, args.verbose)
    except KeyboardInterrupt:
        print("Execution interrupted by user.", file=sys.stderr)
        sys.exit(1)
    except (FileNotFoundError, RuntimeError) as exc:
        print(exc, file=sys.stderr)
        sys.exit(1)

    if args.verbose:
        print("All scripts completed successfully.")


if __name__ == "__main__":
    main()
