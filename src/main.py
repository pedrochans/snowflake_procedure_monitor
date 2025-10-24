#!/usr/bin/env python3
"""
Snowflake Procedure Monitor - Main Script

This script monitors Snowflake stored procedure executions and sends
notifications via Telegram when procedures complete.
"""

import logging
from logging.handlers import RotatingFileHandler
import time
import signal
import sys
from datetime import datetime
from monitor import SnowflakeProcedureMonitor
from config import CHECK_INTERVAL

# Configure logging with rotation
import os
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
log_path = os.path.join(project_root, 'logs', 'snowflake_monitor.log')

# Create logs directory if it doesn't exist
os.makedirs(os.path.dirname(log_path), exist_ok=True)

# Configure rotating file handler
# Max 10 MB per file, keep 3 backup files (total: 40 MB max)
file_handler = RotatingFileHandler(
    log_path,
    maxBytes=10*1024*1024,  # 10 MB
    backupCount=3,          # Keep 3 old files (.log.1, .log.2, .log.3)
    encoding='utf-8'
)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

# Console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    handlers=[file_handler, console_handler]
)

logger = logging.getLogger(__name__)
logger.info(f"Logging configured with rotation: max 10MB per file, 3 backups (40MB total)")

class MonitorManager:
    """
    Manager class to handle the monitoring loop and graceful shutdown.
    """
    
    def __init__(self):
        self.monitor = SnowflakeProcedureMonitor()
        self.running = True
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """
        Handle shutdown signals gracefully.
        """
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.running = False
        
        # Send shutdown notification
        try:
            if signum == signal.SIGINT:
                reason = "Manual stop (Ctrl+C)"
            elif signum == signal.SIGTERM:
                reason = "System shutdown"
            else:
                reason = f"Signal {signum}"
            
            self.monitor.notifier.send_monitor_shutdown_notification(reason)
            logger.info("Shutdown notification sent")
        except Exception as e:
            logger.warning(f"Failed to send shutdown notification: {e}")
    
    def start_monitoring(self):
        """
        Start the main monitoring loop.
        """
        logger.info("="*50)
        logger.info("Snowflake Procedure Monitor Starting")
        logger.info("="*50)
        
        # Test Snowflake connection
        if not self.monitor.test_connections():
            logger.error("Snowflake connection test failed. Please check your configuration.")
            return False
        
        # Send startup notification (this also tests Telegram connection)
        try:
            if self.monitor.notifier.send_monitor_startup_notification():
                logger.info("Startup notification sent - Telegram connection OK")
            else:
                logger.error("Failed to send startup notification - Telegram connection failed")
                return False
        except Exception as e:
            logger.error(f"Failed to send startup notification: {e}")
            return False
        
        logger.info(f"Monitor started at {datetime.now()}")
        logger.info(f"Check interval: {CHECK_INTERVAL} seconds")
        logger.info("Press Ctrl+C to stop monitoring")
        
        try:
            iteration = 0
            while self.running:
                iteration += 1
                logger.info(f"--- Monitoring iteration {iteration} ---")
                
                try:
                    # Process completed procedures
                    new_procedures = self.monitor.process_completed_procedures()
                    
                    if new_procedures > 0:
                        logger.info(f"Processed {new_procedures} new procedure(s)")
                    else:
                        logger.info("No new procedures found")
                    
                    # Cleanup old records every 100 iterations (roughly every 1.6 hours with 60s interval)
                    if iteration % 100 == 0:
                        logger.info("Performing database cleanup...")
                        self.monitor.cleanup_old_records()
                    
                except Exception as e:
                    logger.error(f"Error in monitoring iteration: {e}")
                    
                    # Send error notification if it's a critical error
                    try:
                        self.monitor.notifier.send_error_notification(
                            f"Monitoring error at {datetime.now()}: {str(e)}"
                        )
                    except Exception as notify_error:
                        logger.error(f"Failed to send error notification: {notify_error}")
                
                # Wait for next iteration
                if self.running:
                    logger.info(f"Waiting {CHECK_INTERVAL} seconds until next check...")
                    time.sleep(CHECK_INTERVAL)
        
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
            # Send shutdown notification for manual interrupt
            try:
                self.monitor.notifier.send_monitor_shutdown_notification("Manual stop (Ctrl+C)")
                logger.info("Shutdown notification sent")
            except Exception as e:
                logger.warning(f"Failed to send shutdown notification: {e}")
        
        except Exception as e:
            logger.error(f"Unexpected error in monitoring loop: {e}")
            return False
        
        finally:
            self._cleanup()
        
        return True
    
    def _cleanup(self):
        """
        Perform cleanup operations before shutdown.
        """
        logger.info("Performing cleanup...")
        
        # Send shutdown notification if not already sent
        try:
            if self.running:  # Only send if not already sent by signal handler
                self.monitor.notifier.send_monitor_shutdown_notification("Normal shutdown")
                logger.info("Shutdown notification sent")
        except Exception as e:
            logger.warning(f"Failed to send shutdown notification: {e}")
        
        # Disconnect from Snowflake
        self.monitor.disconnect_from_snowflake()
        
        logger.info("Monitor stopped successfully")

def main():
    """
    Main entry point for the application.
    """
    try:
        manager = MonitorManager()
        success = manager.start_monitoring()
        
        if success:
            logger.info("Monitor completed successfully")
            sys.exit(0)
        else:
            logger.error("Monitor encountered errors")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()