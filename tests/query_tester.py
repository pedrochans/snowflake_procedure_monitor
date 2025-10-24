#!/usr/bin/env python3
"""
Utilidad para cambiar entre modos de query y probar las consultas
"""

import os
import sys
from monitor import SnowflakeProcedureMonitor
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_query_mode(mode: str):
    """
    Test a specific query mode
    
    Args:
        mode: Query mode to test ('production' or 'test')
    """
    print(f"=== Testing Query Mode: {mode.upper()} ===")
    
    monitor = SnowflakeProcedureMonitor()
    
    try:
        # Test loading the query
        query = monitor._load_query(mode)
        print(f"✅ Query loaded successfully from queries/{mode}_query.sql")
        
        # Show query preview (first 200 characters)
        print(f"\n📋 Query Preview:")
        print("-" * 50)
        query_preview = query.strip()[:200].replace('\n', ' ')
        print(f"{query_preview}...")
        print("-" * 50)
        
        # Test Snowflake connection
        print(f"\n🔗 Testing Snowflake connection...")
        if monitor.connect_to_snowflake():
            print("✅ Connected to Snowflake")
            
            # Test query execution (but don't process results)
            print(f"🔍 Testing query execution...")
            try:
                cursor = monitor.snowflake_conn.cursor()
                
                # Load and execute the query
                test_query = monitor._load_query(mode)
                cursor.execute(test_query, {
                    'warehouse': os.getenv('SNOWFLAKE_MONITOR_WAREHOUSE'), 
                    'start_time': monitor.start_time
                })
                
                results = cursor.fetchall()
                print(f"✅ Query executed successfully")
                print(f"📊 Found {len(results)} procedure calls")
                
                if results:
                    print(f"\n📋 Sample results:")
                    for i, result in enumerate(results[:3], 1):  # Show first 3
                        query_id = result[0]
                        status = result[2]
                        duration = result[8] if len(result) > 8 else "N/A"
                        print(f"   {i}. {query_id} | Status: {status} | Duration: {duration}s")
                    
                    if len(results) > 3:
                        print(f"   ... and {len(results) - 3} more results")
                
            except Exception as e:
                print(f"❌ Query execution failed: {e}")
                return False
            finally:
                monitor.disconnect_from_snowflake()
        else:
            print("❌ Failed to connect to Snowflake")
            return False
            
    except FileNotFoundError as e:
        print(f"❌ {e}")
        return False
    except ValueError as e:
        print(f"❌ {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False
    
    print(f"\n🎉 Query mode '{mode}' test completed successfully!")
    return True

def show_query_files():
    """Show available query files and their content"""
    print("=== Available Query Files ===")
    
    query_dir = "queries"
    if not os.path.exists(query_dir):
        print(f"❌ Query directory '{query_dir}' not found")
        return
    
    for filename in os.listdir(query_dir):
        if filename.endswith('.sql'):
            filepath = os.path.join(query_dir, filename)
            print(f"\n📄 {filename}")
            print("-" * 40)
            
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()
                    # Show first few lines (comments usually)
                    lines = content.split('\n')[:5]
                    for line in lines:
                        if line.strip().startswith('--'):
                            print(f"   {line}")
                    
                print(f"   📏 File size: {len(content)} characters")
                print(f"   📏 Lines: {len(content.split(chr(10)))} lines")
                
            except Exception as e:
                print(f"   ❌ Error reading file: {e}")

def switch_mode(new_mode: str):
    """
    Switch the QUERY_MODE in the .env file
    
    Args:
        new_mode: New mode to set ('production' or 'test')
    """
    if new_mode not in ['production', 'test']:
        print(f"❌ Invalid mode: {new_mode}. Valid modes: production, test")
        return False
    
    env_file = '.env'
    if not os.path.exists(env_file):
        print(f"❌ .env file not found. Create one first.")
        return False
    
    try:
        # Read current .env file
        with open(env_file, 'r') as f:
            lines = f.readlines()
        
        # Update or add QUERY_MODE line
        updated = False
        for i, line in enumerate(lines):
            if line.startswith('QUERY_MODE='):
                lines[i] = f'QUERY_MODE={new_mode}\n'
                updated = True
                break
        
        if not updated:
            lines.append(f'QUERY_MODE={new_mode}\n')
        
        # Write back to file
        with open(env_file, 'w') as f:
            f.writelines(lines)
        
        print(f"✅ Switched to {new_mode} mode in .env file")
        print(f"💡 Restart the monitor for changes to take effect")
        return True
        
    except Exception as e:
        print(f"❌ Error updating .env file: {e}")
        return False

def main():
    """Main menu"""
    print("🔍 QUERY MODE TESTER")
    print("=" * 40)
    print("1. 🧪 Test production mode")
    print("2. 🧪 Test test mode") 
    print("3. 📄 Show query files")
    print("4. 🔄 Switch to production mode")
    print("5. 🔄 Switch to test mode")
    print("6. ❌ Exit")
    
    while True:
        choice = input("\nSelect option (1-6): ").strip()
        
        if choice == "1":
            test_query_mode('production')
        elif choice == "2":
            test_query_mode('test')
        elif choice == "3":
            show_query_files()
        elif choice == "4":
            switch_mode('production')
        elif choice == "5":
            switch_mode('test')
        elif choice == "6":
            print("👋 Goodbye!")
            break
        else:
            print("❌ Invalid option. Choose 1-6.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n👋 Interrupted by user")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)