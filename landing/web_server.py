#!/usr/bin/env python3
"""
Snowflake Monitor - Web Server
A simple web interface to control and monitor the Snowflake procedure monitor.
"""

import os
import sys
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path

# Add project paths
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from flask import Flask, jsonify, send_from_directory, Response
import pip_system_certs.wrapt_requests

app = Flask(__name__, static_folder='landing')

# Global state
monitor_process = None
monitor_start_time = None
monitor_status = "stopped"  # running, stopped, error


def get_log_file_path():
    """Get the path to the log file."""
    return os.path.join(project_root, 'logs', 'snowflake_monitor.log')


def read_last_logs(num_lines=50):
    """Read the last N lines from the log file."""
    log_path = get_log_file_path()
    if not os.path.exists(log_path):
        return []
    
    try:
        with open(log_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            return lines[-num_lines:] if len(lines) > num_lines else lines
    except Exception as e:
        return [f"Error reading logs: {str(e)}"]


def parse_log_line(line):
    """Parse a log line into structured data."""
    try:
        # Format: 2024-12-28 14:02:11,123 - name - LEVEL - message
        parts = line.strip().split(' - ', 3)
        if len(parts) >= 4:
            timestamp = parts[0]
            level = parts[2]
            message = parts[3]
            return {
                'timestamp': timestamp,
                'level': level,
                'message': message
            }
    except:
        pass
    return {'timestamp': '', 'level': 'INFO', 'message': line.strip()}


@app.route('/')
def index():
    """Serve the main landing page."""
    return send_from_directory('landing', 'index.html')


@app.route('/api/status')
def get_status():
    """Get the current monitor status."""
    global monitor_process, monitor_status, monitor_start_time
    
    # Check if process is still running
    if monitor_process is not None:
        poll = monitor_process.poll()
        if poll is not None:
            monitor_status = "stopped"
            monitor_process = None
    
    uptime = None
    if monitor_start_time and monitor_status == "running":
        delta = datetime.now() - monitor_start_time
        days = delta.days
        hours, remainder = divmod(delta.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        uptime = f"{days}d {hours:02d}h {minutes:02d}m"
    
    return jsonify({
        'status': monitor_status,
        'uptime': uptime,
        'start_time': monitor_start_time.isoformat() if monitor_start_time else None
    })


@app.route('/api/logs')
def get_logs():
    """Get the latest logs."""
    lines = read_last_logs(100)
    logs = [parse_log_line(line) for line in lines if line.strip()]
    return jsonify({'logs': logs})


@app.route('/api/logs/stream')
def stream_logs():
    """Stream logs in real-time using Server-Sent Events."""
    def generate():
        log_path = get_log_file_path()
        last_position = 0
        
        # Start from end of file
        if os.path.exists(log_path):
            last_position = os.path.getsize(log_path)
        
        while True:
            if os.path.exists(log_path):
                current_size = os.path.getsize(log_path)
                
                if current_size < last_position:
                    # File was truncated/rotated
                    last_position = 0
                
                if current_size > last_position:
                    with open(log_path, 'r', encoding='utf-8') as f:
                        f.seek(last_position)
                        new_content = f.read()
                        last_position = f.tell()
                        
                        for line in new_content.strip().split('\n'):
                            if line.strip():
                                log_entry = parse_log_line(line)
                                yield f"data: {jsonify(log_entry).get_data(as_text=True)}\n\n"
            
            time.sleep(1)
    
    return Response(generate(), mimetype='text/event-stream')


@app.route('/api/start', methods=['POST'])
def start_monitor():
    """Start the monitor process."""
    global monitor_process, monitor_status, monitor_start_time
    
    if monitor_status == "running":
        return jsonify({'success': False, 'message': 'Monitor is already running'})
    
    try:
        # Start the monitor as a subprocess
        python_exe = sys.executable
        script_path = os.path.join(project_root, 'run_monitor.py')
        
        monitor_process = subprocess.Popen(
            [python_exe, script_path],
            cwd=project_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0
        )
        
        monitor_status = "running"
        monitor_start_time = datetime.now()
        
        return jsonify({'success': True, 'message': 'Monitor started successfully'})
    
    except Exception as e:
        monitor_status = "error"
        return jsonify({'success': False, 'message': str(e)})


@app.route('/api/stop', methods=['POST'])
def stop_monitor():
    """Stop the monitor process."""
    global monitor_process, monitor_status, monitor_start_time
    
    if monitor_process is None:
        monitor_status = "stopped"
        return jsonify({'success': True, 'message': 'Monitor is not running'})
    
    try:
        # Gracefully terminate the process
        monitor_process.terminate()
        
        # Wait up to 5 seconds for graceful shutdown
        try:
            monitor_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            # Force kill if it doesn't stop
            monitor_process.kill()
            monitor_process.wait()
        
        monitor_process = None
        monitor_status = "stopped"
        monitor_start_time = None
        
        return jsonify({'success': True, 'message': 'Monitor stopped successfully'})
    
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})


@app.route('/api/restart', methods=['POST'])
def restart_monitor():
    """Restart the monitor process."""
    stop_result = stop_monitor()
    time.sleep(1)
    return start_monitor()


if __name__ == '__main__':
    print("=" * 50)
    print("Snowflake Monitor - Web Interface")
    print("=" * 50)
    print(f"Starting web server at http://localhost:5000")
    print("Press Ctrl+C to stop")
    print("=" * 50)
    
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
