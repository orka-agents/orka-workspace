#!/usr/bin/env python3
"""Bound read-only CLI checks, including kindctl's Docker-engine check."""
import os
import signal
import subprocess
import sys

command = sys.argv[1:]
process = subprocess.Popen(command, start_new_session=True)
try:
    sys.exit(process.wait(timeout=20))
except subprocess.TimeoutExpired:
    os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait()
    print(f"Read-only preflight timed out after 20 seconds: {command[0]}", file=sys.stderr)
    sys.exit(1)
