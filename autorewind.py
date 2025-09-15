#!/usr/bin/env python3

import argparse
import subprocess
import sys
from pathlib import Path

def run_script(script_path, verbose=False):
    cmd = [sys.executable, str(script_path)]
    if verbose:
        cmd.append('-v')

    if verbose:
        print(f"Running {script_path.name}...")

    try:
        result = subprocess.run(cmd, capture_output=not verbose, text=True)
        if result.returncode != 0:
            print(f"Error running {script_path.name}: {result.stderr}")
            return False
        if verbose and result.stdout:
            print(result.stdout)
        return True
    except Exception as e:
        print(f"Failed to run {script_path.name}: {e}")
        return False

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    scripts_dir = Path(__file__).parent / "scripts"
    scripts = ["import.py", "media.py", "online.py", "api.py", "prep.py", "upload.py"]

    for script_name in scripts:
        script_path = scripts_dir / script_name
        if not script_path.exists():
            print(f"Script not found: {script_path}")
            sys.exit(1)

        if not run_script(script_path, args.verbose):
            print(f"Stopping execution due to error in {script_name}")
            sys.exit(1)

    if args.verbose:
        print("All scripts completed successfully")

if __name__ == "__main__":
    main()