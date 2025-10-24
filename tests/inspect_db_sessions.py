#!/usr/bin/env python3
"""
Utility script to inspect the procedure monitor database with session support
"""

import sqlite3
import os
from datetime import datetime

# Get database path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = os.path.join(project_root, "data", "procedure_monitor.db")

# Connect to database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print("\n" + "="*80)
print("📊 MONITOR SESSIONS:")
print("="*80)

# Get sessions
cursor.execute("""
    SELECT 
        session_id,
        start_time,
        end_time,
        status
    FROM monitor_sessions
    ORDER BY session_id DESC
    LIMIT 10
""")

sessions = cursor.fetchall()

if sessions:
    for session in sessions:
        session_id, start_time, end_time, status = session
        print(f"\n  🔑 Session ID: {session_id}")
        print(f"     Status: {status}")
        print(f"     Started: {start_time}")
        print(f"     Ended: {end_time if end_time else 'STILL ACTIVE'}")
        
        # Count procedures processed in this session
        cursor.execute("SELECT COUNT(*) FROM processed_queries WHERE session_id = ?", (session_id,))
        proc_count = cursor.fetchone()[0]
        print(f"     Procedures processed: {proc_count}")
else:
    print("  No sessions found.")

print("\n" + "="*80)
print("🔄 RUNNING PROCEDURES TRACKING:")
print("="*80)

# Get running procedures with notification history
cursor.execute("""
    SELECT 
        rp.query_id,
        rp.procedure_name,
        rp.first_notified_at,
        rp.last_notified_at,
        rp.status,
        rp.session_id
    FROM running_procedures rp
    ORDER BY rp.last_notified_at DESC
    LIMIT 20
""")

running_procs = cursor.fetchall()

if running_procs:
    for proc in running_procs:
        query_id, proc_name, first_notified, last_notified, status, session_id = proc
        
        # Calculate time since last notification
        last_notified_dt = datetime.fromisoformat(last_notified)
        time_since_last = datetime.now() - last_notified_dt
        minutes_since_last = int(time_since_last.total_seconds() / 60)
        
        print(f"\n  📌 {proc_name}")
        print(f"     Session: {session_id}")
        print(f"     Query ID: {query_id[:40]}...")
        print(f"     Status: {status}")
        print(f"     First notified: {first_notified}")
        print(f"     Last notified: {last_notified}")
        print(f"     Minutes since last notification: {minutes_since_last}")
else:
    print("  No running procedures tracked yet.")

print("\n" + "="*80)
print("✅ COMPLETED PROCEDURES:")
print("="*80)

# Count completed procedures
cursor.execute("SELECT COUNT(*) FROM processed_queries")
completed_count = cursor.fetchone()[0]

# Count by session (last 5 sessions)
cursor.execute("""
    SELECT 
        pq.session_id,
        COUNT(*) as count
    FROM processed_queries pq
    GROUP BY pq.session_id
    ORDER BY pq.session_id DESC
    LIMIT 5
""")
session_counts = cursor.fetchall()

print(f"  Total: {completed_count} tracked")
if session_counts:
    print(f"\n  Recent sessions:")
    for session_id, count in session_counts:
        print(f"    Session {session_id}: {count} procedures")

conn.close()

print("\n" + "="*80)
