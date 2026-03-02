import oracledb
import logging
from database.connection import get_async_connection


async def get_all_edms_users(search: str = "", page: int = 1, limit: int = 20):
    """Fetches users from LKP_EDMS_USR_SECUR with pagination and search."""
    conn = await get_async_connection()
    if not conn:
        return {"users": [], "total": 0, "has_more": False}

    users = []
    total = 0
    try:
        async with conn.cursor() as cursor:
            # Build search condition
            search_condition = ""
            if search.strip():
                search_condition = """
                    AND (
                        UPPER(p.USER_ID) LIKE UPPER(:search)
                        OR UPPER(sl.NAME) LIKE UPPER(:search)
                    )
                """
            
            # Count total matching records
            count_query = f"""
                SELECT COUNT(*)
                FROM LKP_EDMS_USR_SECUR us
                JOIN PEOPLE p ON us.USER_ID = p.SYSTEM_ID
                JOIN LKP_EDMS_SECURITY sl ON us.SECURITY_LEVEL_ID = sl.SYSTEM_ID
                WHERE 1=1 {search_condition}
            """
            search_pattern = f"%{search}%"
            if search.strip():
                await cursor.execute(count_query, search=search_pattern)
            else:
                await cursor.execute(count_query)
            count_result = await cursor.fetchone()
            total = count_result[0] if count_result else 0
            
            # Calculate offset
            offset = (page - 1) * limit
            
            # Fetch paginated results
            data_query = f"""
                SELECT * FROM (
                    SELECT 
                        p.USER_ID as username,
                        p.SYSTEM_ID as people_system_id,
                        us.SYSTEM_ID as edms_user_id,
                        us.USER_ID as user_ref_id,
                        sl.NAME as security_level,
                        us.SECURITY_LEVEL_ID,
                        us.LANG,
                        us.THEME,
                        COALESCE(ud.REMAINING_QUOTA, 1073741824) as REMAINING_QUOTA,
                        COALESCE(ud.QUOTA, 1073741824) as QUOTA,
                        ROW_NUMBER() OVER (ORDER BY p.USER_ID) as rn
                    FROM LKP_EDMS_USR_SECUR us
                    JOIN PEOPLE p ON us.USER_ID = p.SYSTEM_ID
                    JOIN LKP_EDMS_SECURITY sl ON us.SECURITY_LEVEL_ID = sl.SYSTEM_ID
                    LEFT JOIN LKP_EDMS_USR_DATA ud ON us.SYSTEM_ID = ud.USER_ID
                    WHERE 1=1 {search_condition}
                )
                WHERE rn > :offset AND rn <= :end_row
            """
            end_row = offset + limit
            if search.strip():
                await cursor.execute(data_query, search=search_pattern, offset=offset, end_row=end_row)
            else:
                await cursor.execute(data_query, offset=offset, end_row=end_row)
            rows = await cursor.fetchall()
            
            for row in rows:
                users.append({
                    'username': row[0],
                    'people_system_id': row[1],
                    'edms_user_id': row[2],
                    'user_ref_id': row[3],
                    'security_level': row[4],
                    'security_level_id': row[5],
                    'lang': row[6] or 'en',
                    'theme': row[7] or 'light',
                    'remaining_quota': row[8],
                    'quota': row[9],
                })
    except oracledb.Error as e:
        logging.error(f"Oracle Database error in get_all_edms_users: {e}", exc_info=True)
    finally:
        if conn:
            await conn.close()
    
    has_more = (page * limit) < total
    return {"users": users, "total": total, "has_more": has_more}


async def get_security_levels():
    """Fetches all available security levels from LKP_EDMS_SECURITY."""
    conn = await get_async_connection()
    if not conn:
        return []

    levels = []
    try:
        async with conn.cursor() as cursor:
            query = """
                SELECT SYSTEM_ID, NAME
                FROM LKP_EDMS_SECURITY
                ORDER BY NAME
            """
            await cursor.execute(query)
            rows = await cursor.fetchall()
            
            for row in rows:
                levels.append({
                    'id': row[0],
                    'name': row[1]
                })
    except oracledb.Error as e:
        logging.error(f"Oracle Database error in get_security_levels: {e}", exc_info=True)
    finally:
        if conn:
            await conn.close()
    
    return levels


async def add_edms_user(user_system_id: int, security_level_id: int, lang: str = 'en', theme: str = 'light', quota: int = 1073741824):
    """Adds a new user to LKP_EDMS_USR_SECUR table."""
    conn = await get_async_connection()
    if not conn:
        return False, "Database connection failed"

    try:
        async with conn.cursor() as cursor:
            # Check if user already exists
            check_query = "SELECT COUNT(*) FROM LKP_EDMS_USR_SECUR WHERE USER_ID = :user_id"
            await cursor.execute(check_query, user_id=user_system_id)
            result = await cursor.fetchone()
            
            if result and result[0] > 0:
                return False, "User already exists in EDMS security table"
            
            # Insert new user
            insert_query = """
                INSERT INTO LKP_EDMS_USR_SECUR (SYSTEM_ID, USER_ID, SECURITY_LEVEL_ID, LANG, THEME, DISABLED)
                VALUES ((SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_EDMS_USR_SECUR), :user_id, :security_level_id, :lang, :theme, '0')
            """
            await cursor.execute(
                insert_query,
                user_id=user_system_id,
                security_level_id=security_level_id,
                lang=lang,
                theme=theme
            )
            
            # Get the newly created EDMS User ID
            await cursor.execute("SELECT SYSTEM_ID FROM LKP_EDMS_USR_SECUR WHERE USER_ID = :1", [user_system_id])
            edms_user_id = (await cursor.fetchone())[0]

            # Initialize Quota
            await cursor.execute(
                "INSERT INTO LKP_EDMS_USR_DATA (SYSTEM_ID, USER_ID, REMAINING_QUOTA, QUOTA, DISABLED) VALUES ((SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_EDMS_USR_DATA), :1, :2, :3, '0')",
                [edms_user_id, quota, quota]
            )

            await conn.commit()
            return True, "User added successfully"
            
    except oracledb.Error as e:
        logging.error(f"Oracle Database error in add_edms_user: {e}", exc_info=True)
        await conn.rollback()
        return False, str(e)
    finally:
        if conn:
            await conn.close()


async def delete_edms_user(edms_user_id: int):
    """Deletes a user from LKP_EDMS_USR_SECUR table by their EDMS user record ID."""
    conn = await get_async_connection()
    if not conn:
        return False, "Database connection failed"

    try:
        async with conn.cursor() as cursor:
            # Delete quota data first (FK)
            await cursor.execute("DELETE FROM LKP_EDMS_USR_DATA WHERE USER_ID = :edms_user_id", edms_user_id=edms_user_id)

            delete_query = "DELETE FROM LKP_EDMS_USR_SECUR WHERE SYSTEM_ID = :edms_user_id"
            await cursor.execute(delete_query, edms_user_id=edms_user_id)
            
            if cursor.rowcount == 0:
                return False, "User not found"
            
            await conn.commit()
            return True, "User deleted successfully"
            
    except oracledb.Error as e:
        logging.error(f"Oracle Database error in delete_edms_user: {e}", exc_info=True)
        await conn.rollback()
        return False, str(e)
    finally:
        if conn:
            await conn.close()


async def update_edms_user(edms_user_id: int, security_level_id: int, lang: str = 'en', theme: str = 'light', remaining_quota: int = None, quota: int = None):
    """Updates an existing user in LKP_EDMS_USR_SECUR table."""
    conn = await get_async_connection()
    if not conn:
        return False, "Database connection failed"

    try:
        async with conn.cursor() as cursor:
            update_query = """
                UPDATE LKP_EDMS_USR_SECUR 
                SET SECURITY_LEVEL_ID = :security_level_id,
                    LANG = :lang,
                    THEME = :theme
                WHERE SYSTEM_ID = :edms_user_id
            """
            await cursor.execute(
                update_query,
                security_level_id=security_level_id,
                lang=lang,
                theme=theme,
                edms_user_id=edms_user_id
            )
            
            if cursor.rowcount == 0:
                return False, "User not found"

            # Update User Data if quota or remaining_quota is provided
            if remaining_quota is not None or quota is not None:
                # Check if record exists
                await cursor.execute("SELECT COUNT(*) FROM LKP_EDMS_USR_DATA WHERE USER_ID = :1", [edms_user_id])
                exists = (await cursor.fetchone())[0] > 0
                
                if exists:
                    if remaining_quota is not None:
                         await cursor.execute("UPDATE LKP_EDMS_USR_DATA SET REMAINING_QUOTA = :1 WHERE USER_ID = :2", [remaining_quota, edms_user_id])
                    if quota is not None:
                         await cursor.execute("UPDATE LKP_EDMS_USR_DATA SET QUOTA = :1 WHERE USER_ID = :2", [quota, edms_user_id])
                else:
                    # Insert new record if not exists
                     current_quota = quota if quota is not None else 1073741824
                     current_remaining = remaining_quota if remaining_quota is not None else current_quota
                     await cursor.execute(
                        "INSERT INTO LKP_EDMS_USR_DATA (SYSTEM_ID, USER_ID, REMAINING_QUOTA, QUOTA, DISABLED) VALUES ((SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_EDMS_USR_DATA), :1, :2, :3, '0')", 
                        [edms_user_id, current_remaining, current_quota]
                    )

            
            await conn.commit()
            return True, "User updated successfully"
            
    except oracledb.Error as e:
        logging.error(f"Oracle Database error in update_edms_user: {e}", exc_info=True)
        await conn.rollback()
        return False, str(e)
    finally:
        if conn:
            await conn.close()


async def search_people(search_term: str = "", limit: int = 50):
    """Search for users in PEOPLE table who are not yet in LKP_EDMS_USR_SECUR."""
    conn = await get_async_connection()
    if not conn:
        return []

    users = []
    try:
        async with conn.cursor() as cursor:
            query = """
                SELECT p.SYSTEM_ID, p.USER_ID, p.FULL_NAME
                FROM PEOPLE p
                WHERE NOT EXISTS (
                    SELECT 1 FROM LKP_EDMS_USR_SECUR us WHERE us.USER_ID = p.SYSTEM_ID
                )
                AND (
                    UPPER(p.USER_ID) LIKE UPPER(:search) 
                    OR UPPER(p.FULL_NAME) LIKE UPPER(:search)
                )
                AND ROWNUM <= :limit
                ORDER BY p.USER_ID
            """
            search_pattern = f"%{search_term}%"
            await cursor.execute(query, search=search_pattern, limit=limit)
            rows = await cursor.fetchall()
            
            for row in rows:
                users.append({
                    'system_id': row[0],
                    'user_id': row[1],
                    'name': row[2] or row[1]
                })
    except oracledb.Error as e:
        logging.error(f"Oracle Database error in search_people: {e}", exc_info=True)
    finally:
        if conn:
            await conn.close()
    
    return users
