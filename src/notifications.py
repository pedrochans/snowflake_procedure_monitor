import requests
import logging
from typing import Optional
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

logger = logging.getLogger(__name__)

class TelegramNotifier:
    """
    Class to handle Telegram notifications for Snowflake procedure monitoring.
    """
    
    def __init__(self, bot_token: str = TELEGRAM_BOT_TOKEN, chat_id: str = TELEGRAM_CHAT_ID):
        """
        Initialize the Telegram notifier.
        
        Args:
            bot_token: Telegram bot token
            chat_id: Telegram chat ID to send messages to
        """
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    def send_message(self, message: str) -> bool:
        """
        Send a message to the configured Telegram chat.
        
        Args:
            message: The message to send
            
        Returns:
            bool: True if message was sent successfully, False otherwise
        """
        if not self.bot_token or not self.chat_id:
            logger.error("Telegram bot token or chat ID not configured")
            return False
        
        try:
            payload = {
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': 'HTML'
            }
            
            # Always verify SSL - no exceptions for security compliance
            response = requests.post(self.base_url, json=payload, timeout=10, verify=True)
            response.raise_for_status()
            logger.info("Telegram message sent successfully")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to send Telegram message: {e}")
            return False
    
    def send_procedure_notification(self, procedure_name: str, status: str, 
                                  duration_seconds: int, warehouse: str,
                                  query_id: str) -> bool:
        """
        Send a formatted notification about a stored procedure execution.
        
        Args:
            procedure_name: Name of the executed procedure
            status: Execution status (SUCCESS, FAILED, etc.)
            duration_seconds: Execution duration in seconds
            
        Returns:
            bool: True if notification was sent successfully, False otherwise
        """
        # Format duration
        if duration_seconds < 60:
            duration_str = f"{duration_seconds}s"
        else:
            minutes = duration_seconds // 60
            seconds = duration_seconds % 60
            duration_str = f"{minutes}m {seconds}s"
        
        # Choose emoji based on status
        emoji = "✅" if status == "SUCCESS" else "❌"
        
        # Format message
        message = f"""
{emoji} <b>Stored Procedure Completed</b>
        
📋 <b>Procedure:</b> {procedure_name}
📊 <b>Status:</b> {status}
⏱️ <b>Duration:</b> {duration_str}
🏭 <b>Warehouse:</b> {warehouse}
🔍 <b>Query ID:</b> <code>{query_id}</code>
        """
        
        return self.send_message(message.strip())
    
    def send_enhanced_procedure_notification(self, procedure_name: str, status: str, 
                                           duration_seconds: int, warehouse: str,
                                           query_id: str, compilation_time: int = None,
                                           execution_time: int = None, total_elapsed_time: int = None,
                                           rows_inserted: int = None, user_name: str = None) -> bool:
        """
        Send an enhanced notification about a stored procedure execution with detailed metrics.
        
        Args:
            procedure_name: Name of the executed procedure
            status: Execution status (SUCCESS, FAILED, etc.)
            duration_seconds: Execution duration in seconds
            warehouse: Snowflake warehouse name
            query_id: Snowflake query ID
            compilation_time: Compilation time in milliseconds
            execution_time: Execution time in milliseconds
            total_elapsed_time: Total elapsed time in milliseconds
            rows_inserted: Number of rows inserted
            user_name: User who executed the procedure
            
        Returns:
            bool: True if notification was sent successfully, False otherwise
        """
        # Format duration
        if duration_seconds < 60:
            duration_str = f"{duration_seconds}s"
        elif duration_seconds < 3600:
            minutes = duration_seconds // 60
            seconds = duration_seconds % 60
            duration_str = f"{minutes}m {seconds}s"
        else:
            hours = duration_seconds // 3600
            minutes = (duration_seconds % 3600) // 60
            duration_str = f"{hours}h {minutes}m"
        
        # Choose emoji based on status
        status_emojis = {
            "SUCCESS": "✅",
            "FAILED": "❌",
            "FAILED_WITH_ERROR": "🔥",
            "FAILED_WITH_INCIDENT": "💥",
            "RUNNING": "🔄",
            "QUEUED": "⏳"
        }
        emoji = status_emojis.get(status, "❓")
        
        # Build simplified message: Status + Procedure Name
        # Duration + Compilation time only
        
        # Format timing information
        comp_ms = compilation_time if compilation_time else 0
        
        message = f"{emoji} {status} {procedure_name}\n⏱️ {duration_str} | Comp: {comp_ms:,}ms"
        return self.send_message(message)
    
    def send_error_notification(self, error_message: str) -> bool:
        """
        Send an error notification.
        
        Args:
            error_message: The error message to send
            
        Returns:
            bool: True if notification was sent successfully, False otherwise
        """
        message = f"🚨 <b>Monitor Error</b>\n\n{error_message}"
        return self.send_message(message)
    
    def send_monitor_shutdown_notification(self, reason: str = "Manual stop") -> bool:
        """
        Send a notification when the monitor is shutting down.
        
        Args:
            reason: Reason for shutdown (e.g., "Manual stop", "Error", "System shutdown")
            
        Returns:
            bool: True if notification was sent successfully, False otherwise
        """
        message = f"🔴 <b>Monitor Stopped</b>\n🔹 Reason: {reason}"
        return self.send_message(message)
    
    def send_monitor_startup_notification(self) -> bool:
        """
        Send a notification when the monitor starts up.
        
        Returns:
            bool: True if notification was sent successfully, False otherwise
        """
        message = "🟢 <b>Monitor Started</b>"
        return self.send_message(message)
    
    def test_connection(self) -> bool:
        """
        Test the Telegram connection by sending a test message.
        
        Returns:
            bool: True if test message was sent successfully, False otherwise
        """
        test_message = "🔧 Snowflake Procedure Monitor - Connection Test"
        return self.send_message(test_message)