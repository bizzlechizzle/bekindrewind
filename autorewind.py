#!/usr/bin/env python3
"""
AutoRewind - KISS orchestrator script.
Runs the media processing pipeline in the correct order.

Usage:
    python autorewind.py [-v]

Arguments:
    -v : Verbose mode
"""

import argparse
import subprocess
import sys
from pathlib import Path


def main():
    """Main function - KISS approach."""
    parser = argparse.ArgumentParser(description="AutoRewind - Media Processing Pipeline")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose mode")

    args = parser.parse_args()

    # Get script directory
    script_dir = Path(__file__).parent / "scripts"

    # Scripts to run in order
    scripts = [
        "import.py",
        "media.py",
        "online.py",
        "api.py"
    ]

    if args.verbose:
        print("AutoRewind - Starting media processing pipeline...")

    # Run each script in order
    for script in scripts:
        script_path = script_dir / script

        if not script_path.exists():
            print(f"Error: Script not found: {script_path}")
            sys.exit(1)

        # Build command
        cmd = [sys.executable, str(script_path)]
        if args.verbose:
            cmd.append("-v")

        if args.verbose:
            print(f"\nRunning: {script}")

        try:
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            if args.verbose and result.stdout:
                print(result.stdout)
        except subprocess.CalledProcessError as e:
            print(f"Error running {script}: {e}")
            if e.stdout:
                print(f"STDOUT: {e.stdout}")
            if e.stderr:
                print(f"STDERR: {e.stderr}")
            sys.exit(1)

    if args.verbose:
        print("\nAutoRewind - Pipeline completed successfully!")
    else:
        print("Pipeline completed successfully!")


if __name__ == "__main__":
    main()