#!/usr/bin/env python3
"""
Utility script to inspect the running procedures database

This script helps debug and monitor the state of running procedure notifications.
"""

import sqlite3
import os
import sys
from datetime import datetime

def get_db_path():
    """Get the path to the procedure monitor database."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    return os.path.join(project_root, "data", "procedure_monitor.db")

def show_running_procedures():
    """Display current running procedures in the database."""
    db_path = get_db_path()
    
    if not os.path.exists(db_path):
        print("❌ Database not found. Run the monitor first to create it.")
        return
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Show running procedures
        cursor.execute("""
            SELECT query_id, procedure_name, first_notified_at, last_notified_at, status
            FROM running_procedures 
            ORDER BY last_notified_at DESC
        """)
        
        running_results = cursor.fetchall()
        
        print("🔄 RUNNING PROCEDURES TRACKING:")
        print("="*80)
        
        if not running_results:
            print("  No running procedures tracked yet.")
        else:
            print(f"{'Query ID':<25} {'Procedure':<35} {'First':<12} {'Last':<12}")
            print("-" * 80)
            
            for row in running_results:
                query_id, proc_name, first_time, last_time, status = row
                
                # Parse timestamps
                first_dt = datetime.fromisoformat(first_time)
                last_dt = datetime.fromisoformat(last_time)
                
                # Calculate time since last notification
                now = datetime.now()
                time_since_last = now - last_dt
                minutes_since = int(time_since_last.total_seconds() / 60)
                
                print(f"{query_id[:24]:<25} {proc_name[:34]:<35} "
                      f"{first_dt.strftime('%H:%M'):<12} {last_dt.strftime('%H:%M'):<12} "
                      f"({minutes_since}m ago)")
        
        print()
        
        # Show processed queries count
        cursor.execute("SELECT COUNT(*) FROM processed_queries")
        processed_count = cursor.fetchone()[0]
        
        print(f"✅ COMPLETED PROCEDURES: {processed_count} tracked")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error reading database: {e}")

def cleanup_test():
    """Test the cleanup functionality."""
    db_path = get_db_path()
    
    if not os.path.exists(db_path):
        print("❌ Database not found. Run the monitor first to create it.")
        return
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Count records before cleanup
        cursor.execute("SELECT COUNT(*) FROM processed_queries")
        processed_before = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM running_procedures")
        running_before = cursor.fetchone()[0]
        
        print(f"📊 Before cleanup: {processed_before} processed, {running_before} running")
        
        # Simulate cleanup (7 days ago)
        from datetime import timedelta
        cutoff_date = datetime.now() - timedelta(days=7)
        
        cursor.execute("SELECT COUNT(*) FROM processed_queries WHERE processed_at < ?", (cutoff_date,))
        old_processed = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM running_procedures WHERE last_notified_at < ?", (cutoff_date,))
        old_running = cursor.fetchone()[0]
        
        print(f"🧹 Would clean up: {old_processed} processed, {old_running} running")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error accessing database: {e}")

def main():
    """Main function."""
    if len(sys.argv) > 1 and sys.argv[1] == "--cleanup-test":
        cleanup_test()
    else:
        show_running_procedures()

if __name__ == "__main__":
    main()