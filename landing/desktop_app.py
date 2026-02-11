#!/usr/bin/env python3
"""
Snowflake Monitor - Desktop Application
A native desktop application to control and monitor the Snowflake procedure monitor.
Uses pywebview to render the HTML interface in a native window.
"""

import os
import sys
import subprocess
import threading
import time
import logging
from datetime import datetime
from pathlib import Path

# Add project paths - landing folder is inside project root
landing_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(landing_dir)  # Go up one level to project root
sys.path.insert(0, project_root)

try:
    import pip_system_certs.wrapt_requests
except ImportError:
    pass  # Not critical for the desktop app itself

import webview

# Setup logging for the desktop app
log_dir = os.path.join(project_root, 'logs')
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'desktop_app.log'), encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('DesktopApp')

# Global state
monitor_process = None
monitor_start_time = None
monitor_status = "stopped"  # running, starting, stopped, error
app_start_time = None  # Track when the app started to filter old logs


class MonitorAPI:
    """API exposed to JavaScript in the webview."""
    
    def __init__(self):
        self.project_root = project_root
        global app_start_time
        app_start_time = datetime.now()
        logger.info(f"MonitorAPI initialized. Project root: {self.project_root}")
    
    def get_log_file_path(self):
        """Get the path to the log file."""
        return os.path.join(self.project_root, 'logs', 'snowflake_monitor.log')
    
    def _check_if_connected(self):
        """Check logs to see if Snowflake connection was established."""
        log_path = self.get_log_file_path()
        if not os.path.exists(log_path):
            return False
        
        try:
            with open(log_path, 'r', encoding='utf-8') as f:
                content = f.read()
                # Look for successful connection indicators in the logs
                # These messages appear after successful authentication
                success_indicators = [
                    "Startup notification sent",
                    "Telegram connection OK",
                    "Monitor started at",
                    "Snowflake connection test successful"
                ]
                for indicator in success_indicators:
                    if indicator in content:
                        # Make sure this is from the current session
                        lines = content.split('\n')
                        for line in reversed(lines):
                            if indicator in line:
                                try:
                                    ts_str = line.split(',')[0]
                                    log_time = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S')
                                    if log_time >= app_start_time:
                                        return True
                                except:
                                    pass
                                break
        except:
            pass
        return False
    
    def get_status(self):
        """Get the current monitor status."""
        global monitor_process, monitor_status, monitor_start_time
        
        # Check if process is still running
        if monitor_process is not None:
            poll = monitor_process.poll()
            if poll is not None:
                monitor_status = "stopped"
                monitor_process = None
            elif monitor_status == "starting":
                # Check if connection was established
                if self._check_if_connected():
                    monitor_status = "running"
                    monitor_start_time = datetime.now()
        
        uptime = None
        if monitor_start_time and monitor_status == "running":
            delta = datetime.now() - monitor_start_time
            days = delta.days
            hours, remainder = divmod(delta.seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            uptime = f"{days}d {hours:02d}h {minutes:02d}m"
        
        return {
            'status': monitor_status,
            'uptime': uptime,
            'start_time': monitor_start_time.isoformat() if monitor_start_time else None
        }
    
    def get_logs(self, num_lines=100):
        """Get the latest logs from current session only."""
        global app_start_time
        log_path = self.get_log_file_path()
        if not os.path.exists(log_path):
            return {'logs': []}
        
        try:
            with open(log_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                lines = lines[-num_lines:] if len(lines) > num_lines else lines
            
            logs = []
            for line in lines:
                if line.strip():
                    parsed = self._parse_log_line(line)
                    # Filter logs to only show those from current session
                    if app_start_time and parsed.get('timestamp'):
                        try:
                            # Parse timestamp: "2024-12-28 14:02:11,123"
                            ts_str = parsed['timestamp'].split(',')[0]
                            log_time = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S')
                            if log_time < app_start_time:
                                continue  # Skip logs from before app started
                        except:
                            pass  # If parsing fails, include the log
                    logs.append(parsed)
            
            return {'logs': logs}
        except Exception as e:
            return {'logs': [{'timestamp': '', 'level': 'ERROR', 'message': f'Error reading logs: {str(e)}'}]}
    
    def _parse_log_line(self, line):
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
    
    def start_monitor(self):
        """Start the monitor process."""
        global monitor_process, monitor_status, monitor_start_time, app_start_time
        
        if monitor_status == "running" or monitor_status == "starting":
            return {'success': False, 'message': 'Monitor is already running or starting'}
        
        # Reset app_start_time to filter old logs
        app_start_time = datetime.now()
        
        try:
            # Determine the correct Python executable
            # When launched via pythonw, sys.executable points to pythonw.exe
            # We need python.exe to run the monitor subprocess
            python_exe = sys.executable
            if python_exe.lower().endswith('pythonw.exe'):
                python_exe = python_exe[:-len('pythonw.exe')] + 'python.exe'
            
            script_path = os.path.join(self.project_root, 'run_monitor.py')
            
            if not os.path.exists(script_path):
                logger.error(f"Monitor script not found: {script_path}")
                return {'success': False, 'message': f'Monitor script not found: {script_path}'}
            
            logger.info(f"Starting monitor: {python_exe} {script_path}")
            
            # Use CREATE_NO_WINDOW on Windows to hide the console
            creation_flags = 0
            if sys.platform == 'win32':
                creation_flags = subprocess.CREATE_NO_WINDOW
            
            # Set up environment - inherit current env and ensure correct paths
            env = os.environ.copy()
            env['PYTHONPATH'] = self.project_root
            
            # IMPORTANT: Do NOT use subprocess.PIPE for stdout/stderr!
            # Nobody reads from the pipe in this app (we read logs from the file instead).
            # If the pipe buffer fills up (~64KB), the subprocess will BLOCK forever
            # on the next print/logging write, causing the monitor to freeze.
            # Redirect to DEVNULL since all logging goes to the log file anyway.
            monitor_process = subprocess.Popen(
                [python_exe, script_path],
                cwd=self.project_root,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env=env,
                creationflags=creation_flags
            )
            
            # Set status to "starting" - will change to "running" when connection is confirmed
            monitor_status = "starting"
            monitor_start_time = None  # Will be set when actually running
            
            logger.info(f"Monitor process started with PID: {monitor_process.pid}")
            return {'success': True, 'message': 'Monitor starting...'}
        
        except Exception as e:
            monitor_status = "error"
            logger.error(f"Failed to start monitor: {e}")
            return {'success': False, 'message': str(e)}
    
    def stop_monitor(self):
        """Stop the monitor process."""
        global monitor_process, monitor_status, monitor_start_time
        
        if monitor_process is None:
            monitor_status = "stopped"
            return {'success': True, 'message': 'Monitor is not running'}
        
        try:
            logger.info(f"Stopping monitor process (PID: {monitor_process.pid})")
            
            # Gracefully terminate the process
            monitor_process.terminate()
            
            # Wait up to 5 seconds for graceful shutdown
            try:
                monitor_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                # Force kill if it doesn't stop
                logger.warning("Process did not terminate gracefully, force killing...")
                monitor_process.kill()
                monitor_process.wait()
            
            monitor_process = None
            monitor_status = "stopped"
            monitor_start_time = None
            
            logger.info("Monitor stopped successfully")
            return {'success': True, 'message': 'Monitor stopped successfully'}
        
        except Exception as e:
            logger.error(f"Error stopping monitor: {e}")
            return {'success': False, 'message': str(e)}
    
    def restart_monitor(self):
        """Restart the monitor process."""
        self.stop_monitor()
        time.sleep(1)
        return self.start_monitor()
    
    def minimize_window(self):
        """Minimize the window."""
        for window in webview.windows:
            window.minimize()
    
    def close_app(self):
        """Close the application."""
        # Stop monitor if running
        self.stop_monitor()
        # Close all windows
        for window in webview.windows:
            window.destroy()


def on_closed():
    """Handle window close event."""
    global monitor_process
    logger.info("Window closed, cleaning up...")
    if monitor_process is not None:
        try:
            monitor_process.terminate()
            monitor_process.wait(timeout=3)
        except Exception:
            try:
                monitor_process.kill()
            except Exception:
                pass
    logger.info("Desktop app closed.")


def main():
    """Main entry point for the desktop application."""
    logger.info("="*50)
    logger.info("Snowflake Monitor Desktop App Starting")
    logger.info(f"Landing dir: {landing_dir}")
    logger.info(f"Project root: {project_root}")
    logger.info("="*50)
    
    api = MonitorAPI()
    
    # Get the HTML file path (same directory as this script)
    html_path = os.path.join(landing_dir, 'index.html')
    
    if not os.path.exists(html_path):
        logger.error(f"HTML file not found: {html_path}")
        return
    
    # Create the window
    window = webview.create_window(
        title='Snowflake Monitor',
        url=html_path,
        js_api=api,
        width=1100,
        height=800,
        min_size=(800, 600),
        background_color='#0a0e1a',
        frameless=False,
        easy_drag=False,
        text_select=False
    )
    
    # Register close event handler
    window.events.closed += on_closed
    
    # Start the application
    logger.info("Starting pywebview window...")
    webview.start(debug=False)


if __name__ == '__main__':
    main()
