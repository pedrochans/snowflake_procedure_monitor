# Snowflake Procedure Monitor - Architecture Documentation

## System Architecture

The Snowflake Procedure Monitor is built with a modular architecture that separates concerns across different components:

```
┌─────────────────┐    ┌───────────────┐    ┌─────────────────┐
│   run_monitor   │───▶│     main      │───▶│    monitor      │
│    (entry)      │    │  (orchestor)  │    │   (core logic)  │
└─────────────────┘    └───────────────┘    └─────────────────┘
                                                     │
                                                     ▼
                       ┌─────────────────┐    ┌─────────────────┐
                       │  notifications  │◀───│   SQLite DB     │
                       │   (telegram)    │    │  (tracking)     │
                       └─────────────────┘    └─────────────────┘
                                │                     ▲
                                ▼                     │
                       ┌─────────────────┐    ┌─────────────────┐
                       │   Telegram      │    │   Snowflake     │
                       │     API         │    │   Database      │
                       └─────────────────┘    └─────────────────┘
```

## Component Responsibilities

### 📂 src/main.py
- **Purpose**: Application orchestration and lifecycle management
- **Responsibilities**:
  - Initialize monitor components
  - Handle application startup/shutdown
  - Manage monitoring loop timing
  - Handle keyboard interrupts gracefully

### 📂 src/monitor.py
- **Purpose**: Core monitoring logic and Snowflake integration
- **Responsibilities**:
  - Establish Snowflake connections
  - Execute monitoring queries
  - Extract procedure information
  - Manage SQLite tracking database
  - Coordinate with notification system

### 📂 src/notifications.py
- **Purpose**: External notification handling
- **Responsibilities**:
  - Format notification messages
  - Handle Telegram API communication
  - Manage SSL/network retries
  - Support different notification formats

### 📂 config/config.py
- **Purpose**: Configuration management
- **Responsibilities**:
  - Load environment variables
  - Provide configuration constants
  - Handle configuration validation

## Data Flow

1. **Initialization**: `run_monitor.py` starts the application
2. **Configuration**: Load settings from `config/.env`
3. **Connection**: Establish Snowflake and Telegram connections
4. **Monitoring Loop**:
   - Execute SQL query from `sql/` directory
   - Extract new procedure completions
   - Check against SQLite tracking database
   - Send notifications for new procedures
   - Update tracking database
   - Sleep until next check

## Database Schema

### SQLite Tracking Database
```sql
CREATE TABLE processed_queries (
    query_id TEXT PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Configuration

### Environment Variables
- `SNOWFLAKE_*`: Connection parameters
- `TELEGRAM_*`: Bot and chat configuration  
- `CHECK_INTERVAL`: Monitoring frequency
- `QUERY_MODE`: SQL query selection

### Query Modes
- **production**: Monitors P_MAGIC procedures with strict filters
- **test**: Broader monitoring for development/testing

## Error Handling

The system includes comprehensive error handling:

- **Connection Failures**: Automatic retry with exponential backoff
- **SSL Issues**: Corporate proxy support with verification bypass
- **Database Errors**: Graceful degradation with logging
- **Notification Failures**: Retry mechanism with fallback options

## Logging Strategy

Structured logging across all components:
- **INFO**: Normal operations and status
- **WARNING**: Recoverable issues (SSL fallbacks, missing data)
- **ERROR**: Failed operations requiring attention
- **DEBUG**: Detailed diagnostic information

All logs are written to `logs/snowflake_monitor.log` with rotation.