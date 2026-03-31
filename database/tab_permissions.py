import oracledb
import logging
from database.connection import get_async_connection


# All valid tab keys in the application
VALID_TAB_KEYS = ['recent', 'folders', 'profilesearch', 'ems_admin']


async def get_tab_permissions_for_user(user_id: int):
    """Fetches tab permissions for a specific user by their PEOPLE.SYSTEM_ID.
    Returns a list of dicts: [{tab_key, can_read, can_write}, ...]
    """
    conn = await get_async_connection()
    if not conn:
        return []

    perms = []
    try:
        async with conn.cursor() as cursor:
            query = """
                SELECT TAB_KEY, CAN_READ, CAN_WRITE
                FROM LKP_SEDMS_SECURITY
                WHERE USER_ID = :user_id AND (DISABLED = '0' OR DISABLED = 'N' OR DISABLED IS NULL)
                ORDER BY TAB_KEY
            """
            await cursor.execute(query, user_id=user_id)
            rows = await cursor.fetchall()

            for row in rows:
                perms.append({
                    'tab_key': row[0],
                    'can_read': row[1] in (1, '1', 'Y', 'y', 'True', 'true', True),
                    'can_write': row[2] in (1, '1', 'Y', 'y', 'True', 'true', True)
                })
    except oracledb.Error as e:
        logging.error(f"Oracle Database error in get_tab_permissions_for_user: {e}", exc_info=True)
    finally:
        if conn:
            await conn.close()

    return perms


async def get_tab_permissions_for_user_admin(user_id: int):
    """Fetches all tab permissions for a user (for admin panel display).
    Returns all rows including disabled ones.
    """
    conn = await get_async_connection()
    if not conn:
        return []

    perms = []
    try:
        async with conn.cursor() as cursor:
            query = """
                SELECT SYSTEM_ID, TAB_KEY, CAN_READ, CAN_WRITE, DISABLED
                FROM LKP_SEDMS_SECURITY
                WHERE USER_ID = :user_id
                ORDER BY TAB_KEY
            """
            await cursor.execute(query, user_id=user_id)
            rows = await cursor.fetchall()

            for row in rows:
                perms.append({
                    'id': row[0],
                    'tab_key': row[1],
                    'can_read': row[2] in (1, '1', 'Y', 'y', 'True', 'true', True),
                    'can_write': row[3] in (1, '1', 'Y', 'y', 'True', 'true', True),
                    'disabled': row[4] in ('1', 'Y') if row[4] else False
                })
    except oracledb.Error as e:
        logging.error(f"Oracle Database error in get_tab_permissions_for_user_admin: {e}", exc_info=True)
    finally:
        if conn:
            await conn.close()

    return perms


async def upsert_tab_permission(user_id: int, tab_key: str, can_read: bool, can_write: bool):
    """Insert or update a tab permission for a specific user.
    Uses MERGE to handle both insert and update in one statement.
    """
    if tab_key not in VALID_TAB_KEYS:
        return False, f"Invalid tab_key: {tab_key}. Must be one of {VALID_TAB_KEYS}"

    conn = await get_async_connection()
    if not conn:
        return False, "Database connection failed"

    try:
        async with conn.cursor() as cursor:
            merge_query = """
                MERGE INTO LKP_SEDMS_SECURITY tp
                USING (
                    SELECT :user_id AS UID_VAL, :tab_key AS TK_VAL,
                           NVL(MAX(s.SYSTEM_ID), 0) + 1 AS NEXT_ID
                    FROM LKP_SEDMS_SECURITY s
                ) src
                ON (tp.USER_ID = src.UID_VAL AND tp.TAB_KEY = src.TK_VAL)
                WHEN MATCHED THEN
                    UPDATE SET CAN_READ = :can_read, CAN_WRITE = :can_write
                WHEN NOT MATCHED THEN
                    INSERT (SYSTEM_ID, USER_ID, TAB_KEY, CAN_READ, CAN_WRITE, DISABLED)
                    VALUES (src.NEXT_ID, :user_id2, :tab_key2, :can_read2, :can_write2, 'N')
            """
            await cursor.execute(
                merge_query,
                user_id=user_id,
                tab_key=tab_key,
                can_read=1 if can_read else 0,
                can_write=1 if can_write else 0,
                user_id2=user_id,
                tab_key2=tab_key,
                can_read2=1 if can_read else 0,
                can_write2=1 if can_write else 0
            )
            await conn.commit()
            return True, "Tab permission updated successfully"
    except oracledb.Error as e:
        logging.error(f"Oracle Database error in upsert_tab_permission: {e}", exc_info=True)
        await conn.rollback()
        return False, str(e)
    finally:
        if conn:
            await conn.close()


async def create_default_permissions_for_user(user_id: int):
    """Creates default tab permissions for a new user (all tabs visible, read-only).
    Called when a user is added via the admin panel.
    """
    conn = await get_async_connection()
    if not conn:
        return False, "Database connection failed"

    try:
        async with conn.cursor() as cursor:
            for tab in VALID_TAB_KEYS:
                default_can_read = 0 if tab == 'ems_admin' else 1
                default_can_write = 0
                merge_query = """
                    MERGE INTO LKP_SEDMS_SECURITY tp
                    USING (
                        SELECT :user_id AS UID_VAL, :tab_key AS TK_VAL,
                               NVL(MAX(s.SYSTEM_ID), 0) + 1 AS NEXT_ID
                        FROM LKP_SEDMS_SECURITY s
                    ) src
                    ON (tp.USER_ID = src.UID_VAL AND tp.TAB_KEY = src.TK_VAL)
                    WHEN NOT MATCHED THEN
                        INSERT (SYSTEM_ID, USER_ID, TAB_KEY, CAN_READ, CAN_WRITE, DISABLED)
                        VALUES (src.NEXT_ID, :user_id2, :tab_key2, :can_read, :can_write, 'N')
                """
                await cursor.execute(
                    merge_query,
                    user_id=user_id,
                    tab_key=tab,
                    user_id2=user_id,
                    tab_key2=tab,
                    can_read=default_can_read,
                    can_write=default_can_write
                )
            await conn.commit()
            return True, "Default permissions created"
    except oracledb.Error as e:
        logging.error(f"Oracle Database error in create_default_permissions_for_user: {e}", exc_info=True)
        await conn.rollback()
        return False, str(e)
    finally:
        if conn:
            await conn.close()


async def delete_tab_permission(system_id: int):
    """Deletes a specific tab permission row by its SYSTEM_ID."""
    conn = await get_async_connection()
    if not conn:
        return False, "Database connection failed"

    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                "DELETE FROM LKP_SEDMS_SECURITY WHERE SYSTEM_ID = :system_id",
                system_id=system_id
            )
            if cursor.rowcount == 0:
                return False, "Permission not found"

            await conn.commit()
            return True, "Tab permission deleted successfully"
    except oracledb.Error as e:
        logging.error(f"Oracle Database error in delete_tab_permission: {e}", exc_info=True)
        await conn.rollback()
        return False, str(e)
    finally:
        if conn:
            await conn.close()


def get_admin_full_permissions():
    """Returns full permissions for admin users (all tabs, read+write).
    Used as an override — admin users bypass the DB table entirely.
    """
    return [
        {'tab_key': tab, 'can_read': True, 'can_write': True}
        for tab in VALID_TAB_KEYS
    ]
