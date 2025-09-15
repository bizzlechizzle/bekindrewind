#!/usr/bin/env python3

import subprocess
import sys
from pathlib import Path

def run_cmd(cmd):
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error: {e.stderr}")
        return False

def main():
    print("Installing bekindrewind prerequisites...")

    requirements = Path(__file__).parent / "requirements.txt"

    if not requirements.exists():
        print("requirements.txt not found")
        sys.exit(1)

    print("Installing Python packages...")
    pip_cmd = [sys.executable, "-m", "pip", "install", "-r", str(requirements)]

    # Try normal install first
    if not run_cmd(pip_cmd):
        print("Normal pip install failed, trying with --break-system-packages...")
        pip_cmd.append("--break-system-packages")
        if not run_cmd(pip_cmd):
            print("Failed to install Python packages")
            print("Try: pip install -r requirements.txt --break-system-packages")
            sys.exit(1)

    print("Installing Playwright browsers...")
    if not run_cmd([sys.executable, "-m", "playwright", "install"]):
        print("Failed to install Playwright browsers")
        sys.exit(1)

    print("Checking for system dependencies...")

    missing = []

    if not run_cmd(["which", "mktorrent"]):
        missing.append("mktorrent")
        print("mktorrent not found")
    else:
        print("mktorrent found")

    if not run_cmd(["which", "ffmpeg"]):
        missing.append("ffmpeg")
        print("ffmpeg not found")
    else:
        print("ffmpeg found")

    if not run_cmd(["which", "mediainfo"]):
        missing.append("mediainfo")
        print("mediainfo not found")
    else:
        print("mediainfo found")

    if missing:
        print("\nMissing system dependencies:")
        for dep in missing:
            print(f"  {dep}")
        print("\nInstall commands by OS:")
        print("  Ubuntu/Debian: apt install mktorrent ffmpeg mediainfo")
        print("  macOS: brew install mktorrent ffmpeg mediainfo")
        print("  Arch: pacman -S mktorrent ffmpeg mediainfo")

    print("Prerequisites installation complete")

if __name__ == "__main__":
    main()