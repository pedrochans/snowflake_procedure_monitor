import os
from dotenv import load_dotenv

load_dotenv()

# Snowflake config
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT') 
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_MONITOR_WAREHOUSE = os.getenv('SNOWFLAKE_MONITOR_WAREHOUSE')

# Telegram config
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# Monitoring config
CHECK_INTERVAL = int(os.getenv('CHECK_INTERVAL', 60))  # seconds
RUNNING_PROCEDURE_THROTTLE_MINUTES = int(os.getenv('RUNNING_PROCEDURE_THROTTLE_MINUTES', 30))  # minutes between RUNNING notifications

# Query configuration
QUERY_MODE = os.getenv('QUERY_MODE', 'production')  # production, test