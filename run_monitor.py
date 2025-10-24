#!/usr/bin/env python3
"""
Snowflake Procedure Monitor - Entry Point

This is the main entry point for the Snowflake Procedure Monitor application.
Run this script to start monitoring.

Usage:
    python run_monitor.py
"""

import sys
import os

# Use Windows certificate store for SSL verification
# This allows Python to use corporate certificates installed in Windows
import pip_system_certs.wrapt_requests

# Add both src and config directories to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.join(project_root, 'src')
config_path = os.path.join(project_root, 'config')
sys.path.insert(0, project_root)
sys.path.insert(0, src_path)
sys.path.insert(0, config_path)

# Import and run the main application
if __name__ == "__main__":
    from src.main import main
    main()