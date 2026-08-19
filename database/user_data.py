import oracledb
import logging
from database.connection import get_async_connection

DEFAULT_QUOTA = 1073741824  # 1GB in bytes

async def initialize_user_quota(edms_user_id, quota=DEFAULT_QUOTA):
    """
    Initializes a row in LKP_EDMS_USR_DATA for a new EDMS user.
    edms_user_id: The SYSTEM_ID from LKP_EDMS_USR_SECUR
    """
    conn = await get_async_connection()
    if not conn:
        return False, "Database connection failed"

    try:
        async with conn.cursor() as cursor:
            # Check if exists first to avoid dupes if run redundantly
            await cursor.execute("SELECT 1 FROM LKP_EDMS_USR_DATA WHERE USER_ID = :1", [edms_user_id])
            if await cursor.fetchone():
                return True, "User quota already initialized."

            query = """
                INSERT INTO LKP_EDMS_USR_DATA (SYSTEM_ID, USER_ID, REMAINING_QUOTA, DISABLED)
                VALUES ((SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_EDMS_USR_DATA), :user_id, :quota, '0')
            """
            await cursor.execute(query, user_id=edms_user_id, quota=quota)
            await conn.commit()
            return True, "User quota initialized successfully."
    except oracledb.Error as e:
        logging.error(f"Oracle error in initialize_user_quota: {e}", exc_info=True)
        return False, str(e)
    finally:
        if conn:
            await conn.close()

async def get_user_quota(edms_user_id):
    """
    Returns the remaining quota for a user.
    If no record exists, it lazily initializes it with default quota.
    """
    conn = await get_async_connection()
    if not conn:
        return 0

    try:
        async with conn.cursor() as cursor:
            query = "SELECT REMAINING_QUOTA FROM LKP_EDMS_USR_DATA WHERE USER_ID = :1"
            await cursor.execute(query, [edms_user_id])
            result = await cursor.fetchone()

            if result:
                return result[0]
            else:
                # Lazy initialization logic could go here, or just return 0/Default.
                # Let's return DEFAULT_QUOTA but NOT save it implicitly here to ensure transactional safety during reads.
                # However, for robustness, if we assume every valid EDMS user *should* have one, we can return DEFAULT.
                return DEFAULT_QUOTA

    except oracledb.Error as e:
        logging.error(f"Oracle error in get_user_quota: {e}", exc_info=True)
        return 0
    finally:
        if conn:
            await conn.close()

async def update_user_quota(edms_user_id, new_quota):
    """
    Updates the remaining quota for a specific user (Admin override).
    """
    conn = await get_async_connection()
    if not conn:
        return False, "Database connection failed"

    try:
        async with conn.cursor() as cursor:
            # Upsert logic akin to merge might be better, or check existence.
            # We'll try update, if 0 rows, we insert.
            update_sql = "UPDATE LKP_EDMS_USR_DATA SET REMAINING_QUOTA = :1 WHERE USER_ID = :2"
            await cursor.execute(update_sql, [new_quota, edms_user_id])
            
            if cursor.rowcount == 0:
                # Insert if not exists
                insert_sql = "INSERT INTO LKP_EDMS_USR_DATA (SYSTEM_ID, USER_ID, REMAINING_QUOTA, DISABLED) VALUES ((SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_EDMS_USR_DATA), :1, :2, '0')"
                await cursor.execute(insert_sql, [edms_user_id, new_quota])

            await conn.commit()
            return True, "Quota updated successfully"

    except oracledb.Error as e:
        logging.error(f"Oracle error in update_user_quota: {e}", exc_info=True)
        return False, str(e)
    finally:
        if conn:
            await conn.close()

async def deduct_user_quota(edms_user_id, amount_bytes):
    """
    Deducts the uploaded amount from user's quota.
    Returns: (Success (bool), Message (str))
    Prior check should be done before calling this to ensure sufficient funds, 
    but this will double check or just enforce.
    """
    conn = await get_async_connection()
    if not conn:
        return False, "Database connection failed"

    try:
        async with conn.cursor() as cursor:
            # We want atomic update
            # Check current first? Or just update and check resulting value?
            # Better to check first.
            await cursor.execute("SELECT REMAINING_QUOTA FROM LKP_EDMS_USR_DATA WHERE USER_ID = :1 FOR UPDATE", [edms_user_id])
            row = await cursor.fetchone()
            
            current_quota = row[0] if row else DEFAULT_QUOTA # If no row, assume default? Or 0? 
            # If no row exists, we probably need to create it. 
            # In this flow, let's assume if it doesn't exist, they have full default quota.
            
            if row is None:
                # Create the row with Default - amount
                new_quota = DEFAULT_QUOTA - amount_bytes
                if new_quota < 0:
                     return False, "Insufficient quota (Implicit Default)"
                
                await cursor.execute(
                    "INSERT INTO LKP_EDMS_USR_DATA (SYSTEM_ID, USER_ID, REMAINING_QUOTA, DISABLED) VALUES ((SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_EDMS_USR_DATA), :1, :2, '0')",
                    [edms_user_id, new_quota]
                )
            else:
                if current_quota < amount_bytes:
                    return False, "Insufficient quota"
                
                await cursor.execute(
                    "UPDATE LKP_EDMS_USR_DATA SET REMAINING_QUOTA = REMAINING_QUOTA - :1 WHERE USER_ID = :2",
                    [amount_bytes, edms_user_id]
                )
            
            await conn.commit()
            return True, "Quota deducted"

    except oracledb.Error as e:
        logging.error(f"Oracle error in deduct_user_quota: {e}", exc_info=True)
        await conn.rollback()
        return False, str(e)
    finally:
        if conn:
            await conn.close()

async def restore_user_quota(edms_user_id, amount_bytes):
    """
    Restores quota when a file is deleted.
    Adds the given amount back to REMAINING_QUOTA, capped at the user's total QUOTA.
    Returns: (Success (bool), Message (str))
    """
    if amount_bytes <= 0:
        return True, "Nothing to restore"

    conn = await get_async_connection()
    if not conn:
        return False, "Database connection failed"

    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                "SELECT REMAINING_QUOTA, COALESCE(QUOTA, :1) FROM LKP_EDMS_USR_DATA WHERE USER_ID = :2 FOR UPDATE",
                [DEFAULT_QUOTA, edms_user_id]
            )
            row = await cursor.fetchone()

            if row is None:
                # No quota row exists — nothing to restore
                return True, "No quota record found, skipping restore"

            current_remaining = row[0]
            total_quota = row[1]

            # Cap restored value at total quota
            new_remaining = min(current_remaining + amount_bytes, total_quota)

            await cursor.execute(
                "UPDATE LKP_EDMS_USR_DATA SET REMAINING_QUOTA = :1 WHERE USER_ID = :2",
                [new_remaining, edms_user_id]
            )

            await conn.commit()
            logging.info(f"Restored {amount_bytes} bytes for user {edms_user_id}. New remaining: {new_remaining}")
            return True, "Quota restored"

    except oracledb.Error as e:
        logging.error(f"Oracle error in restore_user_quota: {e}", exc_info=True)
        await conn.rollback()
        return False, str(e)
    finally:
        if conn:
            await conn.close()

async def get_edms_user_id(people_system_id):
    """
    Looks up the EDMS user SYSTEM_ID from LKP_EDMS_USR_SECUR given a PEOPLE.SYSTEM_ID.
    Returns the EDMS user ID or None if not found.
    """
    conn = await get_async_connection()
    if not conn:
        return None

    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                "SELECT SYSTEM_ID FROM LKP_EDMS_USR_SECUR WHERE USER_ID = :1",
                [people_system_id]
            )
            res = await cursor.fetchone()
            return res[0] if res else None
    except oracledb.Error as e:
        logging.error(f"Oracle error in get_edms_user_id: {e}", exc_info=True)
        return None
    finally:
        if conn:
            await conn.close()
