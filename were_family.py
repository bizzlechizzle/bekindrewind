#!/usr/bin/env python3
"""
Script executor for danger2manifold.py, import_tuner.py, ford_probe.py, miata_info.py, and Fastandthefurious.py
Executes scripts sequentially with logging based on 2jznoshit.json configuration.
"""

import json
import logging
import subprocess
import sys
from pathlib import Path


def setup_logging() -> bool:
    """Setup logging based on configuration, return if logging is enabled."""
    try:
        with open('2jznoshit.json', 'r') as f:
            config = json.load(f)
        log_enabled = config.get('were_family', {}).get('logs', False)
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        log_enabled = False
    
    if log_enabled:
        logging.basicConfig(
            filename='were_family.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filemode='a'
        )
    else:
        logging.disable(logging.CRITICAL)
    
    return log_enabled


def execute_script(script_name: str, args: list = None) -> int:
    """Execute a Python script and return exit code."""
    script_path = Path(script_name)
    
    if not script_path.exists():
        logging.error(f"Script not found: {script_name}")
        return 1
    
    cmd = [sys.executable, script_name]
    if args:
        cmd.extend(args)
    
    logging.info(f"Executing: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, check=False, capture_output=False)
        if result.returncode == 0:
            logging.info(f"Script {script_name} completed successfully")
        else:
            logging.error(f"Script {script_name} failed with exit code {result.returncode}")
        return result.returncode
    except KeyboardInterrupt:
        logging.info(f"Script {script_name} interrupted by user")
        return 130
    except Exception as e:
        logging.error(f"Exception executing {script_name}: {e}")
        return 1


def main():
    """Main execution function."""
    log_enabled = setup_logging()
    
    if log_enabled:
        logging.info("were_family started")
    
    scripts = [
        'danger2manifold.py',
        'import_tuner.py', 
        'ford_probe.py',
        'miata_info.py',
        'Fastandthefurious.py'
    ]
    
    for i, script in enumerate(scripts):
        # Pass command line arguments only to import_tuner.py (index 1)
        args = sys.argv[1:] if i == 1 and len(sys.argv) > 1 else None
        
        exit_code = execute_script(script, args)
        if exit_code != 0:
            if log_enabled:
                logging.error(f"{script} failed, stopping execution")
            sys.exit(exit_code)
    
    if log_enabled:
        logging.info("All scripts completed successfully")


if __name__ == "__main__":
    main()