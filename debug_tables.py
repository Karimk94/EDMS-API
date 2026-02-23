import asyncio
import os
import sys

# Add current directory to path so we can import database.connection
sys.path.append(os.getcwd())

from database.connection import get_async_connection
import logging

# Configure logging to output to console
logging.basicConfig(level=logging.INFO)

async def check_tables():
    print("Connecting to database...")
    conn = await get_async_connection()
    if not conn:
        print("Failed to connect to database.")
        return

    tables_to_check = ['LKP_MAIN_FIELDS', 'SEARCH_FORM', 'FORMS', 'PEOPLEGROUPS', 'PEOPLE', 'GROUPS']
    
    print("\n--- Checking Target Tables ---")
    try:
        async with conn.cursor() as cursor:
            for table in tables_to_check:
                try:
                    # Try to select 1 row to verify access
                    await cursor.execute(f"SELECT 1 FROM {table} FETCH FIRST 1 ROWS ONLY")
                    print(f"[OK] {table} exists and is accessible.")
                except Exception as e:
                    print(f"[FAIL] {table}: {e}")

            print("\n--- Searching for Similar Tables (USER_TABLES) ---")
            # Check user_tables (tables owned by the user)
            await cursor.execute("SELECT table_name FROM user_tables WHERE table_name LIKE 'LKP_%' OR table_name LIKE 'SEARCH_%' OR table_name LIKE 'FORM%' OR table_name LIKE 'GROUP%' OR table_name LIKE 'PEOPLE%' ORDER BY table_name")
            rows = await cursor.fetchall()
            for row in rows:
                print(f"User Table: {row[0]}")

            print("\n--- Searching for Similar Tables (ALL_TABLES - accessible) ---")
             # Check all_tables (tables accessible to the user, potentially with schema prefix)
            await cursor.execute("SELECT owner, table_name FROM all_tables WHERE (table_name LIKE 'LKP_MAIN%' OR table_name LIKE 'SEARCH_FORM%' OR table_name = 'FORMS' OR table_name LIKE 'PEOPLE%') AND owner != 'SYS' AND owner != 'SYSTEM' ORDER BY owner, table_name")
            rows = await cursor.fetchall()
            if rows:
                for row in rows:
                    print(f"Accessible Table: {row[0]}.{row[1]}")
            else:
                 print("No matching tables found in ALL_TABLES (excluding SYS/SYSTEM).")

    except Exception as e:
        print(f"General Error: {e}")
    finally:
        await conn.close()
        print("\nConnection closed.")

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(check_tables())
