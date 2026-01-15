import oracledb
import logging
from database.connection import get_async_connection, get_connection

async def get_all_groups_from_db():
    """
    Fetches all groups directly from the GROUPS table in the database.
    This bypasses DMS API limitations which don't expose all groups through views.
    
    Returns:
        list: A list of dictionaries with 'group_id', 'group_name', and 'description' keys.
    """
    conn = await get_async_connection()
    if not conn:
        logging.error("Could not connect to database for get_all_groups_from_db")
        return []
    
    try:
        cursor = await conn.cursor()
        # Query the GROUPS table directly for GROUP_ID and GROUP_NAME
        query = """
            SELECT GROUP_ID, GROUP_NAME 
            FROM GROUPS 
            ORDER BY GROUP_ID
        """
        await cursor.execute(query)
        rows = await cursor.fetchall()
        
        groups = []
        for row in rows:
            group_id = row[0].strip() if row[0] else None
            group_name = row[1].strip() if row[1] else group_id
            if group_id:
                groups.append({
                    'group_id': group_id,
                    'group_name': group_name or group_id,
                    'description': ''
                })
        
        # logging.info(f"[database/groups.py] Fetched {len(groups)} groups from database")
        return groups
        
    except oracledb.Error as e:
        logging.error(f"Oracle Database error in get_all_groups_from_db: {e}", exc_info=True)
        return []
    finally:
        await conn.close()


def get_all_groups_from_db_sync():
    """
    Synchronous version: Fetches all groups directly from the GROUPS table.
    """
    conn = get_connection()
    if not conn:
        logging.error("Could not connect to database for get_all_groups_from_db_sync")
        return []
    
    try:
        cursor = conn.cursor()
        query = """
            SELECT GROUP_ID, GROUP_NAME 
            FROM GROUPS 
            ORDER BY GROUP_ID
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        
        groups = []
        for row in rows:
            group_id = row[0].strip() if row[0] else None
            group_name = row[1].strip() if row[1] else group_id
            if group_id:
                groups.append({
                    'group_id': group_id,
                    'group_name': group_name or group_id,
                    'description': ''
                })
        
        # logging.info(f"[database/groups.py] Fetched {len(groups)} groups from database (sync)")
        return groups
        
    except oracledb.Error as e:
        logging.error(f"Oracle Database error in get_all_groups_from_db_sync: {e}", exc_info=True)
        return []
    finally:
        conn.close()
