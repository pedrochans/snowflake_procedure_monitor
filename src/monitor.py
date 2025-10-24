import snowflake.connector
import sqlite3
import logging
import os
import re
from datetime import datetime, timedelta
from typing import Optional, List, Tuple
from notifications import TelegramNotifier
from config import (
    SNOWFLAKE_USER, 
    SNOWFLAKE_ACCOUNT, 
    SNOWFLAKE_WAREHOUSE, 
    SNOWFLAKE_MONITOR_WAREHOUSE, 
    QUERY_MODE,
    RUNNING_PROCEDURE_THROTTLE_MINUTES
)

logger = logging.getLogger(__name__)

class SnowflakeProcedureMonitor:
    """
    Main class for monitoring Snowflake stored procedure executions.
    """
    
    def __init__(self):
        """
        Initialize the monitor with Snowflake connection and SQLite database.
        """
        self.snowflake_conn = None
        self.notifier = TelegramNotifier()
        
        # Set database path to data directory
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.db_path = os.path.join(project_root, "data", "procedure_monitor.db")
        self.start_time = datetime.now()
        self.last_heartbeat = None  # Track last heartbeat time
        
        # Initialize SQLite database
        self._init_database()
        
        # Create new session
        self.session_id = self._create_session()
    
    def _init_database(self):
        """
        Initialize SQLite database to track processed queries and monitor sessions.
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Table for monitor sessions
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS monitor_sessions (
                    session_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    start_time TIMESTAMP NOT NULL,
                    end_time TIMESTAMP,
                    status TEXT DEFAULT 'ACTIVE'
                )
            ''')
            
            # Table for processed queries - now includes session_id
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS processed_queries (
                    query_id TEXT NOT NULL,
                    session_id INTEGER NOT NULL,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (query_id, session_id),
                    FOREIGN KEY (session_id) REFERENCES monitor_sessions(session_id)
                )
            ''')
            
            # Table for tracking RUNNING procedures notifications
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS running_procedures (
                    query_id TEXT NOT NULL,
                    session_id INTEGER NOT NULL,
                    procedure_name TEXT NOT NULL,
                    first_notified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_notified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'RUNNING',
                    PRIMARY KEY (query_id, session_id),
                    FOREIGN KEY (session_id) REFERENCES monitor_sessions(session_id)
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("SQLite database initialized successfully")
            
        except sqlite3.Error as e:
            logger.error(f"Failed to initialize SQLite database: {e}")
            raise
    
    def _create_session(self) -> int:
        """
        Create a new monitor session in the database.
        
        Returns:
            int: The session ID
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute(
                "INSERT INTO monitor_sessions (start_time, status) VALUES (?, 'ACTIVE')",
                (self.start_time,)
            )
            session_id = cursor.lastrowid
            
            conn.commit()
            conn.close()
            
            logger.info(f"Created new monitor session: {session_id}")
            return session_id
            
        except sqlite3.Error as e:
            logger.error(f"Failed to create session: {e}")
            raise
    
    def _close_session(self):
        """
        Close the current monitor session.
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute(
                "UPDATE monitor_sessions SET end_time = ?, status = 'COMPLETED' WHERE session_id = ?",
                (datetime.now(), self.session_id)
            )
            
            conn.commit()
            conn.close()
            
            logger.info(f"Closed monitor session: {self.session_id}")
            
        except sqlite3.Error as e:
            logger.error(f"Failed to close session: {e}")
    
    def connect_to_snowflake(self) -> bool:
        """
        Establish connection to Snowflake using external browser authentication.
        Sets up database and warehouse context automatically.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.snowflake_conn = snowflake.connector.connect(
                user=SNOWFLAKE_USER,
                account=SNOWFLAKE_ACCOUNT,
                authenticator='externalbrowser',
                warehouse=SNOWFLAKE_WAREHOUSE,
                database='FINANCIERO'  # Establecer BD directamente en la conexión
            )
            
            logger.info("Successfully connected to Snowflake with database FINANCIERO")
            
            # Verificar el contexto establecido
            cursor = self.snowflake_conn.cursor()
            try:
                cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_WAREHOUSE()")
                result = cursor.fetchone()
                logger.info(f"Connection context - Database: {result[0]}, Warehouse: {result[1]}")
            except Exception as e:
                logger.warning(f"Could not verify connection context: {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            return False
    
    def _snowflake_heartbeat(self):
        """
        Send a lightweight heartbeat query to keep Snowflake SSO session alive.
        
        Executes a minimal SELECT 1 query on the warehouse to count as session activity.
        Without this, SSO sessions may timeout after ~4 hours of metadata-only queries.
        
        Should be called periodically (recommended: every hour).
        
        Cost: ~0.00001 credits per heartbeat (negligible)
        
        Returns:
            bool: True if heartbeat successful, False otherwise
        """
        try:
            if self.snowflake_conn is None:
                logger.warning("Cannot send heartbeat: connection is None")
                return False
            
            cursor = self.snowflake_conn.cursor()
            # Simple query that uses the warehouse and counts as activity
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            
            self.last_heartbeat = datetime.now()
            logger.info(f"💓 Snowflake heartbeat successful - session kept alive")
            return True
            
        except Exception as e:
            logger.warning(f"Snowflake heartbeat failed: {e}")
            return False
    
    def _check_and_send_heartbeat(self):
        """
        Check if heartbeat is needed (every hour) and send it.
        
        First heartbeat is sent 1 hour after monitor start (not immediately).
        This avoids unnecessary warehouse activation right after SSO connection.
        
        Returns:
            bool: True if heartbeat was sent or not needed, False if failed
        """
        # Calculate time since monitor started
        time_since_start = datetime.now() - self.start_time
        
        # Don't send heartbeat in the first hour
        if time_since_start.total_seconds() < 3600:
            logger.debug("Skipping heartbeat - less than 1 hour since monitor start")
            return True
        
        # Check if we need to send heartbeat
        if self.last_heartbeat is None:
            # First heartbeat after 1 hour of operation
            logger.info("First heartbeat after 1 hour of operation, sending...")
            return self._snowflake_heartbeat()
        
        time_since_last = datetime.now() - self.last_heartbeat
        # Send heartbeat every hour (3600 seconds)
        if time_since_last.total_seconds() >= 3600:
            logger.info("Heartbeat interval reached (1 hour since last), sending...")
            return self._snowflake_heartbeat()
        
        return True  # No heartbeat needed yet
    
    def disconnect_from_snowflake(self):
        """
        Close Snowflake connection and session.
        """
        if self.snowflake_conn:
            self.snowflake_conn.close()
            self.snowflake_conn = None
            logger.info("Disconnected from Snowflake")
        
        # Close the session
        self._close_session()
    
    def _is_query_processed(self, query_id: str) -> bool:
        """
        Check if a query has already been processed in this session.
        
        Args:
            query_id: Snowflake query ID
            
        Returns:
            bool: True if query was already processed in this session, False otherwise
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute(
                "SELECT 1 FROM processed_queries WHERE query_id = ? AND session_id = ?",
                (query_id, self.session_id)
            )
            result = cursor.fetchone()
            
            conn.close()
            return result is not None
            
        except sqlite3.Error as e:
            logger.error(f"Database error checking processed query: {e}")
            return False
    
    def _mark_query_processed(self, query_id: str):
        """
        Mark a query as processed in the database for this session.
        
        Args:
            query_id: Snowflake query ID to mark as processed
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute(
                "INSERT OR IGNORE INTO processed_queries (query_id, session_id) VALUES (?, ?)",
                (query_id, self.session_id)
            )
            conn.commit()
            conn.close()
            
        except sqlite3.Error as e:
            logger.error(f"Database error marking query as processed: {e}")
    
    def _should_notify_running_procedure(self, query_id: str, procedure_name: str) -> bool:
        """
        Check if we should notify about a RUNNING procedure based on throttling rules.
        
        Rules:
        1) First time seeing this query_id in this session -> NOTIFY
        2) Seen before but configured interval has passed since LAST notification -> NOTIFY
        3) Otherwise -> DON'T NOTIFY (throttled)
        
        This ensures consistent intervals between notifications (e.g., every 15 min: 0min, 15min, 30min, 45min...)
        
        Args:
            query_id: Snowflake query ID
            procedure_name: Name of the procedure
            
        Returns:
            bool: True if we should send notification, False otherwise
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute(
                "SELECT first_notified_at, last_notified_at FROM running_procedures WHERE query_id = ? AND session_id = ?", 
                (query_id, self.session_id)
            )
            result = cursor.fetchone()
            
            conn.close()
            
            if result is None:
                # First time seeing this RUNNING procedure in this session
                logger.info(f"[THROTTLE CHECK] {procedure_name} - NEW (first time)")
                return True
            else:
                # Check if configured throttle time has passed since LAST notification
                first_notified = datetime.fromisoformat(result[0])
                last_notified = datetime.fromisoformat(result[1])
                time_since_first = datetime.now() - first_notified
                time_since_last = datetime.now() - last_notified
                
                minutes_since_first = time_since_first.total_seconds() / 60
                minutes_since_last = time_since_last.total_seconds() / 60
                
                # Convert configured minutes to seconds
                throttle_seconds = RUNNING_PROCEDURE_THROTTLE_MINUTES * 60
                
                logger.info(f"[THROTTLE CHECK] {procedure_name} - First: {minutes_since_first:.1f}min ago, Last: {minutes_since_last:.1f}min ago")
                
                # Compare against LAST notification time (not first)
                if time_since_last.total_seconds() >= throttle_seconds:
                    logger.info(f"[THROTTLE CHECK] {procedure_name} - OK to notify ({RUNNING_PROCEDURE_THROTTLE_MINUTES}+ min since last)")
                    return True
                else:
                    logger.info(f"[THROTTLE CHECK] {procedure_name} - BLOCKED (only {minutes_since_last:.1f} min since last, need {RUNNING_PROCEDURE_THROTTLE_MINUTES})")
                    return False
                
        except sqlite3.Error as e:
            logger.error(f"Database error checking running procedure: {e}")
            return False
    
    def _mark_running_procedure_notified(self, query_id: str, procedure_name: str):
        """
        Mark a RUNNING procedure as notified in the database for this session.
        Updates last_notified_at if record exists, creates new record if not.
        IMPORTANT: Preserves first_notified_at to maintain 30-minute throttling from first notification.
        
        Args:
            query_id: Snowflake query ID
            procedure_name: Name of the procedure
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Current timestamp as Python datetime for consistency
            now = datetime.now().isoformat()
            
            # Check if record already exists
            cursor.execute(
                "SELECT first_notified_at FROM running_procedures WHERE query_id = ? AND session_id = ?",
                (query_id, self.session_id)
            )
            existing = cursor.fetchone()
            
            if existing:
                # Record exists - UPDATE only last_notified_at (preserve first_notified_at)
                cursor.execute("""
                    UPDATE running_procedures 
                    SET last_notified_at = ?, status = 'RUNNING'
                    WHERE query_id = ? AND session_id = ?
                """, (now, query_id, self.session_id))
                logger.info(f"Updated notification timestamp for {procedure_name} (first was at {existing[0]})")
            else:
                # First time - INSERT new record with both timestamps set to now
                cursor.execute("""
                    INSERT INTO running_procedures 
                    (query_id, session_id, procedure_name, first_notified_at, last_notified_at, status)
                    VALUES (?, ?, ?, ?, ?, 'RUNNING')
                """, (query_id, self.session_id, procedure_name, now, now))
                logger.info(f"First notification for {procedure_name} recorded at {now}")
            
            conn.commit()
            conn.close()
            
        except sqlite3.Error as e:
            logger.error(f"Database error marking running procedure as notified: {e}")
    
    def _load_query(self, query_mode: str = None) -> str:
        """
        Load SQL query from external file based on query mode.
        
        Args:
            query_mode: Query mode (production or test). If None, uses config value.
            
        Returns:
            str: SQL query string
            
        Raises:
            FileNotFoundError: If query file is not found
            ValueError: If query mode is invalid
        """
        import os
        
        if query_mode is None:
            query_mode = QUERY_MODE
        
        query_files = {
            'production': 'sql/production_query.sql',
            'test': 'sql/test_query.sql'
        }
        
        if query_mode not in query_files:
            raise ValueError(f"Invalid query mode: {query_mode}. Valid modes: {list(query_files.keys())}")
        
        query_file = query_files[query_mode]
        
        # Get the project root directory (one level up from src)
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        query_path = os.path.join(project_root, query_file)
        
        try:
            with open(query_path, 'r', encoding='utf-8') as f:
                query = f.read()
            
            logger.info(f"Loaded query from: {query_file} (mode: {query_mode})")
            return query
            
        except FileNotFoundError:
            logger.error(f"Query file not found: {query_path}")
            raise FileNotFoundError(f"Required query file not found: {query_file}")
            
        except Exception as e:
            logger.error(f"Error loading query file {query_file}: {e}")
            raise

    def _extract_procedure_name(self, query_text: str) -> str:
        """
        Extract procedure name from CALL PROCEDURE statement.
        
        Args:
            query_text: The SQL query text
            
        Returns:
            str: Extracted procedure name or 'UNKNOWN' if not found
        """
        try:
            # Specific patterns for FINANCIERO schemas
            # Target structures: 'CALL FINANCIERO.MAGIC_CI_DATA_STG.[procedure_name](...)'
            #                   'CALL FINANCIERO.MAGIC_CI_DATA.[procedure_name](...)'
            patterns = [
                r'CALL\s+FINANCIERO\.MAGIC_CI_DATA_STG\.([A-Za-z_][A-Za-z0-9_]*)\s*\(',  # MAGIC_CI_DATA_STG schema
                r'CALL\s+FINANCIERO\.MAGIC_CI_DATA\.([A-Za-z_][A-Za-z0-9_]*)\s*\(',      # MAGIC_CI_DATA schema
                r'CALL\s+FINANCIERO\.([A-Za-z_][A-Za-z0-9_]*\.)?([A-Za-z_][A-Za-z0-9_]*)\s*\(',  # Fallback for FINANCIERO
            ]
            
            for i, pattern in enumerate(patterns):
                match = re.search(pattern, query_text, re.IGNORECASE)
                if match:
                    # For the first two patterns, return group 1 (procedure name)
                    # For the fallback pattern, return group 2 (procedure name after optional schema)
                    return match.group(1) if i < 2 else match.group(2)
            
            # If no pattern matches, log the full query text for debugging
            logger.warning(f"Could not extract procedure name from: {query_text[:150]}...")
            return "UNKNOWN"
                
        except Exception as e:
            logger.error(f"Error extracting procedure name: {e}")
            return "UNKNOWN"
    
    def get_completed_procedures(self) -> List[Tuple]:
        """
        Query Snowflake for completed procedure calls since the monitor started.
        
        Includes automatic heartbeat check to keep SSO session alive.
        
        Returns:
            List[Tuple]: List of completed procedure information tuples
        """
        if not self.snowflake_conn:
            logger.error("No Snowflake connection available")
            return []
        
        try:
            # Check and send heartbeat if needed (every hour)
            self._check_and_send_heartbeat()
            
            cursor = self.snowflake_conn.cursor()
            
            # Load query from external file
            query = self._load_query()
            
            cursor.execute(query, {
                'warehouse': SNOWFLAKE_MONITOR_WAREHOUSE
            })
            results = cursor.fetchall()
            
            logger.info(f"Found {len(results)} completed procedure calls")
            return results
            
        except Exception as e:
            logger.error(f"Error querying Snowflake: {e}")
            return []
    
    def process_completed_procedures(self) -> int:
        """
        Process completed procedures and send notifications for new ones.
        Also monitors RUNNING procedures with intelligent throttling.
        
        Returns:
            int: Number of new procedures processed
        """
        completed_procedures = self.get_completed_procedures()
        new_procedures_count = 0
        
        logger.info(f"Processing {len(completed_procedures)} procedures from query results")
        
        for procedure_info in completed_procedures:
            query_id = procedure_info[0]
            status = procedure_info[2]  # EXECUTION_STATUS
            procedure_name = self._extract_procedure_name(procedure_info[1])
            
            logger.info(f"[LOOP] Examining: {procedure_name}, Status: {status}, Query ID: {query_id[:20]}...")
            
            # Handle RUNNING procedures with throttling
            if status == 'RUNNING':
                # Check throttling FIRST before processing
                if not self._should_notify_running_procedure(query_id, procedure_name):
                    # Skip notification if throttled (within configured throttle time)
                    logger.info(f"[THROTTLED] Skipping RUNNING notification for {procedure_name} - within {RUNNING_PROCEDURE_THROTTLE_MINUTES} min")
                    continue
                
                # Calculate duration manually for RUNNING procedures (already done in SQL, but as backup)
                duration_seconds = int(procedure_info[8]) if procedure_info[8] else 0
                compilation_time = procedure_info[5]
                
                logger.info(f"Notifying RUNNING procedure: {procedure_name} (Duration so far: {duration_seconds}s)")
                
                success = self.notifier.send_enhanced_procedure_notification(
                    procedure_name=procedure_name,
                    status=status,
                    duration_seconds=duration_seconds,
                    warehouse=procedure_info[12],
                    query_id=query_id,
                    compilation_time=compilation_time,
                    execution_time=procedure_info[6],
                    total_elapsed_time=procedure_info[7],
                    rows_inserted=procedure_info[11],
                    user_name=procedure_info[13]
                )
                
                if success:
                    self._mark_running_procedure_notified(query_id, procedure_name)
                    new_procedures_count += 1
                    logger.info(f"Successfully notified RUNNING procedure: {procedure_name}")
                else:
                    logger.error(f"Failed to send RUNNING notification for procedure: {procedure_name}")
                
                continue  # Skip further processing for RUNNING procedures
            
            # Skip completed procedures if already processed
            if self._is_query_processed(query_id):
                logger.info(f"[SKIP] {status} procedure {procedure_name} - already processed")
                continue
            
            logger.info(f"[NEW] Processing {status} procedure: {procedure_name}")
            
            # Extract procedure information based on new query structure
            query_text = procedure_info[1]           # QUERY_TEXT
            status = procedure_info[2]               # EXECUTION_STATUS
            start_time = procedure_info[3]           # START_TIME
            end_time = procedure_info[4]             # END_TIME
            compilation_time = procedure_info[5]     # COMPILATION_TIME
            execution_time = procedure_info[6]       # EXECUTION_TIME
            total_elapsed_time = procedure_info[7]   # TOTAL_ELAPSED_TIME (milliseconds)
            duration_seconds = int(procedure_info[8]) if procedure_info[8] else 0  # DURATION_SECONDS
            duration_minutes = procedure_info[9]     # DURATION_MINUTES
            duration_hours = procedure_info[10]      # DURATION_HOURS
            rows_inserted = procedure_info[11]       # ROWS_INSERTED
            warehouse = procedure_info[12]           # WAREHOUSE_NAME
            user_name = procedure_info[13]           # USER_NAME
            
            # Extract procedure name (already extracted above, no need to do it again)
            
            # Send notification with enhanced information
            success = self.notifier.send_enhanced_procedure_notification(
                procedure_name=procedure_name,
                status=status,
                duration_seconds=duration_seconds,
                warehouse=warehouse,
                query_id=query_id,
                compilation_time=compilation_time,
                execution_time=execution_time,
                total_elapsed_time=total_elapsed_time,
                rows_inserted=rows_inserted,
                user_name=user_name
            )
            
            if success:
                # Mark as processed only if notification was sent successfully
                self._mark_query_processed(query_id)
                new_procedures_count += 1
                logger.info(f"Successfully processed and notified for procedure: {procedure_name}")
            else:
                logger.error(f"Failed to send notification for procedure: {procedure_name}")
        
        return new_procedures_count
    
    def cleanup_old_records(self, days: int = 7):
        """
        Clean up old sessions and their associated records from SQLite database.
        
        Args:
            days: Number of days to keep records (default: 7)
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cutoff_date = datetime.now() - timedelta(days=days)
            
            # Get old session IDs to delete
            cursor.execute(
                "SELECT session_id FROM monitor_sessions WHERE start_time < ?",
                (cutoff_date,)
            )
            old_sessions = [row[0] for row in cursor.fetchall()]
            
            if old_sessions:
                placeholders = ','.join('?' * len(old_sessions))
                
                # Clean up processed queries from old sessions
                cursor.execute(
                    f"DELETE FROM processed_queries WHERE session_id IN ({placeholders})",
                    old_sessions
                )
                deleted_processed = cursor.rowcount
                
                # Clean up running procedures from old sessions
                cursor.execute(
                    f"DELETE FROM running_procedures WHERE session_id IN ({placeholders})",
                    old_sessions
                )
                deleted_running = cursor.rowcount
                
                # Clean up old sessions
                cursor.execute(
                    f"DELETE FROM monitor_sessions WHERE session_id IN ({placeholders})",
                    old_sessions
                )
                deleted_sessions = cursor.rowcount
                
                conn.commit()
                logger.info(
                    f"Cleaned up {deleted_sessions} old sessions, "
                    f"{deleted_processed} processed queries, "
                    f"{deleted_running} running procedures"
                )
            
            conn.close()
                
        except sqlite3.Error as e:
            logger.error(f"Error cleaning up old records: {e}")
    
    def test_connections(self) -> bool:
        """
        Test both Snowflake and Telegram connections.
        
        Returns:
            bool: True if both connections work, False otherwise
        """
        logger.info("Testing connections...")
        
        # Test Snowflake connection
        sf_success = self.connect_to_snowflake()
        if sf_success:
            logger.info("[OK] Snowflake connection successful")
        else:
            logger.error("[ERROR] Snowflake connection failed")
        
        return sf_success