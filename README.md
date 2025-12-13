# Snowflake Procedure Monitor

A Python application that monitors Snowflake stored procedure executions and sends real-time notifications via Telegram when procedures complete.

## 🚀 Features

- 🔍 **Real-time Monitoring**: Continuously monitors a specific Snowflake warehouse for stored procedure executions
- 📱 **Telegram Notifications**: Sends formatted notifications with status, duration, and procedure details
- 🚫 **Duplicate Prevention**: Uses SQLite database to avoid sending duplicate notifications
- 🔐 **Secure Authentication**: Uses Snowflake's external browser authentication
- 📊 **Detailed Logging**: Comprehensive logging for debugging and monitoring
- 🧹 **Automatic Cleanup**: Periodically cleans old processed records
- 📁 **Organized Structure**: Clean, modular project organization
- ⚙️ **Configurable Queries**: External SQL files for easy customization

## 📁 Project Structure

```
snowflake_procedure_monitor/
├── 📁 src/                          # 🐍 Core Python application code
│   ├── main.py                      # Application entry point
│   ├── monitor.py                   # Core monitoring logic
│   ├── notifications.py             # Telegram notification handling
│   └── __init__.py                  # Package initialization
├── 📁 config/                       # ⚙️ Configuration files
│   ├── config.py                    # Environment variable handling  
│   ├── .env                         # Environment variables (not in git)
│   ├── .env.example                 # Environment variables template
│   └── __init__.py                  # Package initialization
├── 📁 sql/                          # 📊 SQL query files
│   ├── production_query.sql         # Production monitoring query
│   └── test_query.sql               # Test/development query
├── 📁 data/                         # 💾 Database and data files
│   └── procedure_monitor.db         # SQLite tracking database
├── 📁 logs/                         # 📝 Log files
│   └── snowflake_monitor.log        # Application logs
├── 📁 tests/                        # 🧪 Test and utility scripts
│   └── query_tester.py              # Query testing utility
├── 📁 docs/                         # 📚 Documentation
├── run_monitor.py                   # 🎯 Main execution script
├── requirements.txt                 # 📦 Python dependencies
└── README.md                        # 📖 This documentation
```

## Installation

### Prerequisites

- Python 3.7 or higher
- Snowflake account with appropriate permissions
- Telegram bot token and chat ID

### Setup Steps

1. **Clone or download the project**
   ```bash
   cd snowflake_procedure_monitor
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment variables**
   ```bash
   # Copy the example file
   copy config\.env.example config\.env
   
   # Edit config\.env with your actual values
   notepad config\.env
   ```

4. **Set up Telegram Bot** (if not done already)
   - Message @BotFather on Telegram
   - Create a new bot with `/newbot`
   - Get your bot token
   - Add the bot to your chat/group and get the chat ID

## Configuration

Edit the `.env` file with your settings:

```env
# Snowflake Configuration
SNOWFLAKE_USER=your_snowflake_username
SNOWFLAKE_ACCOUNT=your_account.region.cloud
SNOWFLAKE_PASSWORD=                              # Only needed if using password authentication
SNOWFLAKE_DATABASE=your_database                 # Database to connect to
SNOWFLAKE_WAREHOUSE=your_warehouse               # Warehouse for connection
SNOWFLAKE_MONITOR_WAREHOUSE=your_warehouse       # Warehouse to monitor for procedures
SNOWFLAKE_AUTHENTICATOR=externalbrowser          # 'externalbrowser' (SSO) or 'snowflake' (password)

# Telegram Configuration
TELEGRAM_BOT_TOKEN=your_bot_token_from_botfather
TELEGRAM_CHAT_ID=your_chat_id_or_group_id

# Monitoring Configuration
CHECK_INTERVAL=60                                # Seconds between checks (default: 60, minimum: 60)
RUNNING_PROCEDURE_THROTTLE_MINUTES=30            # Minutes between RUNNING notifications (default: 30, minimum: 1)
MIN_DURATION_MS=10000                            # Minimum procedure duration to notify in ms (default: 10000 = 10s)

# Query Configuration
QUERY_MODE=production                            # 'production' or 'test'
PROCEDURE_FILTER=CALL %                          # SQL LIKE filter (e.g., 'CALL MYDB.%%P_PROC_%%')
```

### Configuration Variables Reference

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SNOWFLAKE_USER` | ✅ | - | Snowflake username or email |
| `SNOWFLAKE_ACCOUNT` | ✅ | - | Snowflake account identifier (e.g., `mycompany.us-east-1.aws`) |
| `SNOWFLAKE_PASSWORD` | ❌ | - | Password (only if `SNOWFLAKE_AUTHENTICATOR=snowflake`) |
| `SNOWFLAKE_DATABASE` | ✅ | - | Database to connect to |
| `SNOWFLAKE_WAREHOUSE` | ✅ | - | Warehouse for the connection |
| `SNOWFLAKE_MONITOR_WAREHOUSE` | ✅ | - | Warehouse to monitor for procedure executions |
| `SNOWFLAKE_AUTHENTICATOR` | ❌ | `externalbrowser` | Auth method: `externalbrowser` (SSO) or `snowflake` (password) |
| `TELEGRAM_BOT_TOKEN` | ✅ | - | Telegram bot token from @BotFather |
| `TELEGRAM_CHAT_ID` | ✅ | - | Telegram chat/group ID for notifications |
| `CHECK_INTERVAL` | ❌ | `60` | Seconds between monitoring checks (minimum: 60) |
| `RUNNING_PROCEDURE_THROTTLE_MINUTES` | ❌ | `30` | Minutes between re-notifications for RUNNING procedures (minimum: 1) |
| `MIN_DURATION_MS` | ❌ | `10000` | Minimum procedure duration (ms) to trigger notification |
| `QUERY_MODE` | ❌ | `production` | Query mode: `production` or `test` |
| `PROCEDURE_FILTER` | ❌ | `CALL %` | SQL LIKE pattern to filter procedures |

### Finding Your Snowflake Account Identifier

Your Snowflake account identifier should be in the format:
- `account_name.region.cloud_provider` (e.g., `mycompany.us-east-1.aws`)
- Check your Snowflake URL or contact your administrator

### Getting Telegram Chat ID

To find your chat ID:
1. Add your bot to the desired chat/group
2. Send a message to the bot
3. Visit: `https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates`
4. Look for the `chat` -> `id` field in the response

## Usage

### Running the Monitor

```bash
# Run the main monitor
python run_monitor.py

# Alternative: Run from src directory  
cd src
python main.py
```

The monitor will:
1. Test connections to Snowflake and Telegram
2. Start monitoring for completed procedures
3. Send notifications for new procedure completions
4. Log all activities to `logs/snowflake_monitor.log`

### Stopping the Monitor

- Press `Ctrl+C` to gracefully stop the monitor
- The application will clean up connections and exit safely

### Example Notification

When a stored procedure completes, you'll receive a Telegram message like:

```
✅ Stored Procedure Completed

📋 Procedure: P_DAILY_REPORT
📊 Status: SUCCESS  
⏱️ Duration: 2m 15s
🏭 Warehouse: ANALYTICS_WH
🔍 Query ID: 01a2b3c4-5678-90ab-cdef-1234567890ab
```

## Monitoring Logic

### Query Detection

The monitor looks for queries that:
- Run on the specified warehouse
- Contain "CALL PROCEDURE" in the query text (case-insensitive)
- Have completed execution (SUCCESS or FAILED status)
- Started after the monitor was launched

### Procedure Name Extraction

The application extracts procedure names from queries like:
- `CALL PROCEDURE P_EXAMPLE();`
- `CALL P_DAILY_REPORT(param1, param2);`
- `call procedure schema.p_report();`

### Duplicate Prevention

- Each processed query ID is stored in a local SQLite database
- The monitor skips queries that have already been processed
- Old records are automatically cleaned up every ~100 monitoring cycles

## Troubleshooting

### Common Issues

1. **Snowflake Connection Failed**
   - Verify your account identifier format
   - Ensure your user has proper permissions
   - Check if external browser authentication is allowed

2. **No Procedures Detected**
   - Confirm procedures are running on the specified warehouse
   - Check that query text contains "CALL PROCEDURE"
   - Verify the monitor started before procedure executions

3. **Telegram Notifications Not Received**
   - Verify bot token and chat ID are correct
   - Ensure the bot is added to the chat/group
   - Check network connectivity

### Logging

The application logs to both console and `snowflake_monitor.log`:
- INFO: General monitoring activities
- ERROR: Connection issues and failures
- WARNING: Non-critical issues

### Database

The SQLite database `procedure_monitor.db` stores processed query IDs. You can safely delete this file to reset the monitoring history (all procedures will be treated as new).

## Advanced Configuration

### Authentication Methods

**SSO (External Browser)** - Default:
```env
SNOWFLAKE_AUTHENTICATOR=externalbrowser
```
Opens a browser window for SSO authentication. Best for interactive use.

**Password Authentication**:
```env
SNOWFLAKE_AUTHENTICATOR=snowflake
SNOWFLAKE_PASSWORD=your_password
```
Best for automated/headless environments.

### Procedure Filtering

The `PROCEDURE_FILTER` variable uses SQL LIKE syntax to filter which procedures to monitor:

```env
# Monitor all CALL statements
PROCEDURE_FILTER=CALL %

# Monitor specific database/schema procedures
PROCEDURE_FILTER=CALL MYDB.%%P_DAILY_%%

# Monitor procedures starting with P_MAGIC
PROCEDURE_FILTER=CALL %%P_MAGIC_%%
```

> **Note**: Use `%%` instead of `%` in `.env` files to escape the percent sign.

### Check Interval

Modify `CHECK_INTERVAL` in your `.env` file:
- **Minimum enforced**: 60 seconds (to prevent excessive resource consumption)
- **Default**: 60 seconds
- **Recommended for production**: 60-180 seconds

### Duration Threshold

The `MIN_DURATION_MS` setting filters out short-running procedures:
```env
MIN_DURATION_MS=10000   # Only notify for procedures running > 10 seconds
MIN_DURATION_MS=60000   # Only notify for procedures running > 1 minute
```

### Warehouse Monitoring

To monitor multiple warehouses, you would need to run separate instances with different configurations.

### Query History Permissions

Ensure your Snowflake user has permissions to query `INFORMATION_SCHEMA.QUERY_HISTORY`.

## Security Considerations

- Keep your `.env` file secure and never commit it to version control
- Use appropriate Snowflake user permissions (read-only for monitoring)
- Regularly rotate Telegram bot tokens if needed
- The SQLite database contains only query IDs (no sensitive data)

## Development

### Project Structure

- `main.py`: Entry point and monitoring loop
- `monitor.py`: Core Snowflake monitoring logic
- `notifications.py`: Telegram notification handling
- `config.py`: Environment variable management

### Adding Features

Common enhancements:
- Support for multiple warehouses
- Email notifications in addition to Telegram
- Web dashboard for monitoring status
- Alerting for failed procedures only
- Integration with other monitoring systems

## License

This project is provided as-is for monitoring Snowflake procedure executions. Modify as needed for your environment.

## 📊 Performance & Resource Consumption

**Test scenario**: 24/7 monitoring, checks every 3 minutes (480 iterations/day), 15-20 procedures per check.

### Resource Consumption (30-day operation)

| Component | 24h Usage | 30-day Usage | Status |
|-----------|-----------|--------------|--------|
| **Snowflake Credits** | ~0.005 | ~0.15 | ✅ Negligible (~$0.30/month) |
| **Disk Space** | 2.3 MB | 42 MB | ✅ Capped (log rotation + cleanup) |
| **CPU** | 0.2% avg | 0.2% avg | ✅ Minimal |
| **RAM** | 10 MB | 10 MB | ✅ Constant |
| **Network** | 15.5 MB | 465 MB | ✅ <500 MB/month |

### Key Points

- ✅ **Near-zero Snowflake cost**: Metadata queries are free, heartbeat uses ~0.15 credits/month (~$0.30)
- ✅ **24/7 SSO session**: Heartbeat every hour keeps session alive without manual reconnection
- ✅ **Controlled storage**: Logs rotate at 10 MB (max 40 MB), database auto-cleanup (max 2 MB)
- ✅ **Lightweight**: <1% CPU, ~10 MB RAM
- ✅ **Scalable**: Can run indefinitely without degradation

### Optimizations

- **Session heartbeat**: `SELECT 1` every hour keeps SSO session alive (cost: ~0.00001 credits/hour)
- **Log rotation**: 10 MB per file, keeps 3 backups (40 MB total cap)
- **Database cleanup**: 7-day retention keeps DB at ~2 MB
- **Session tracking**: Prevents duplicate notifications across restarts
- **Throttling**: RUNNING procedures notify max once per configurable interval (default: 30 minutes via `RUNNING_PROCEDURE_THROTTLE_MINUTES`)

## Support

For issues or questions:
1. Check the troubleshooting section
2. Review the log files for detailed error messages
3. Verify your Snowflake and Telegram configurations