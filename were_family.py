#!/usr/bin/env python3
"""
Script executor for automotive-themed Python scripts.
Executes scripts sequentially with optional logging based on 2jznoshit.json configuration.
"""

import json
import logging
import subprocess
import sys
from pathlib import Path


def setup_logging():
    """Setup logging based on configuration. Returns True if logging enabled."""
    try:
        with open('2jznoshit.json', 'r') as f:
            config = json.load(f)
        
        if config.get('were_family', {}).get('logs', False):
            logging.basicConfig(
                filename='were_family.log',
                level=logging.INFO,
                format='%(asctime)s - %(levelname)s - %(message)s',
                filemode='a'
            )
            return True
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        pass
    
    logging.disable(logging.CRITICAL)
    return False


def execute_script(script_name, args=None):
    """Execute a Python script and return exit code."""
    if not Path(script_name).exists():
        logging.error(f"Script not found: {script_name}")
        return 1
    
    cmd = [sys.executable, script_name]
    if args:
        cmd.extend(args)
    
    logging.info(f"Executing: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, check=False)
        logging.info(f"Script {script_name} exit code: {result.returncode}")
        return result.returncode
    except KeyboardInterrupt:
        logging.info(f"Script {script_name} interrupted")
        return 130
    except Exception as e:
        logging.error(f"Exception executing {script_name}: {e}")
        return 1


def main():
    """Main execution function."""
    log_enabled = setup_logging()
    
    if log_enabled:
        logging.info("were_family started")
    
    # Script execution order as specified
    scripts = [
        'danger2manifold.py',
        'import_tuner.py', 
        'ford_probe.py',
        'miata_info.py',
        'Fastandthefurious.py',
        '2fast2furious.py',
        'tokyo_drift.py',
        'fastandfurious.py',
        'Nos.py',
        'honda_s2000.py',
        'hector.py',
        'any_flavor.py',
        'doms_charger.py',
        'fast_five.py'
    ]
    
    args = sys.argv[1:] if len(sys.argv) > 1 else None
    
    for i, script in enumerate(scripts):
        # Pass command line arguments only to import_tuner.py (index 1)
        script_args = args if i == 1 else None
        
        exit_code = execute_script(script, script_args)
        if exit_code != 0:
            if log_enabled:
                logging.error(f"{script} failed with exit code {exit_code}, stopping execution")
            sys.exit(exit_code)
    
    if log_enabled:
        logging.info("All scripts completed successfully")


if __name__ == "__main__":
    main()