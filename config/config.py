import os
from dotenv import load_dotenv

load_dotenv()

# Snowflake config
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT') 
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')  # Optional: for password auth
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')  # Database to connect to
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_MONITOR_WAREHOUSE = os.getenv('SNOWFLAKE_MONITOR_WAREHOUSE')
SNOWFLAKE_AUTHENTICATOR = os.getenv('SNOWFLAKE_AUTHENTICATOR', 'externalbrowser')  # externalbrowser or snowflake (password)

# Telegram config
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# Monitoring config (with safety minimums to prevent excessive resource consumption)
CHECK_INTERVAL = max(60, int(os.getenv('CHECK_INTERVAL', 60)))  # seconds (minimum: 60)
RUNNING_PROCEDURE_THROTTLE_MINUTES = max(1, int(os.getenv('RUNNING_PROCEDURE_THROTTLE_MINUTES', 30)))  # minutes (minimum: 1)
MIN_DURATION_MS = int(os.getenv('MIN_DURATION_MS', 10000))  # Minimum duration to notify (milliseconds)

# Query configuration
QUERY_MODE = os.getenv('QUERY_MODE', 'production')  # production, test
PROCEDURE_FILTER = os.getenv('PROCEDURE_FILTER', 'CALL %')  # SQL LIKE filter for procedures