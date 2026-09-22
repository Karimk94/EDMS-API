import oracledb
import logging
from datetime import datetime, timezone, timedelta
from database.connection import get_async_connection

# UTC+4 timezone (Gulf Standard Time, e.g. Dubai)
UTC_PLUS_4 = timezone(timedelta(hours=4))


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
                        OR UPPER(p.FULL_NAME) LIKE UPPER(:search)
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
                        p.FULL_NAME as full_name,
                        p.SYSTEM_ID as people_system_id,
                        us.SYSTEM_ID as edms_user_id,
                        us.USER_ID as user_ref_id,
                        sl.NAME as security_level,
                        us.SECURITY_LEVEL_ID,
                        us.LANG,
                        us.THEME,
                        COALESCE(ud.QUOTA, 1073741824) as QUOTA,
                        us.LARGE_DOWNLOADS,
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
                    'full_name': row[1] or row[0],
                    'people_system_id': row[2],
                    'edms_user_id': row[3],
                    'user_ref_id': row[4],
                    'security_level': row[5],
                    'security_level_id': row[6],
                    'lang': row[7] or 'en',
                    'theme': row[8] or 'light',
                    'remaining_quota': row[9],
                    'quota': row[10],
                    'LARGE_DOWNLOADS': row[11] == '1' if row[11] is not None else False,
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


async def add_edms_user(user_system_id: int, security_level_id: int, lang: str = 'en', theme: str = 'light', quota: int = 1073741824, LARGE_DOWNLOADS: bool = False):
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
                INSERT INTO LKP_EDMS_USR_SECUR (SYSTEM_ID, USER_ID, SECURITY_LEVEL_ID, LANG, THEME, DISABLED, SECTION, LARGE_DOWNLOADS)
                VALUES ((SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_EDMS_USR_SECUR), :user_id, :security_level_id, :lang, :theme, '0', '0000', :LARGE_DOWNLOADS)
            """
            await cursor.execute(
                insert_query,
                user_id=user_system_id,
                security_level_id=security_level_id,
                lang=lang,
                theme=theme,
                LARGE_DOWNLOADS='1' if LARGE_DOWNLOADS else '0'
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


async def update_edms_user(edms_user_id: int, security_level_id: int, lang: str = 'en', theme: str = 'light', remaining_quota: int = None, quota: int = None, LARGE_DOWNLOADS: bool = None):
    """Updates an existing user in LKP_EDMS_USR_SECUR table."""
    conn = await get_async_connection()
    if not conn:
        return False, "Database connection failed"

    try:
        async with conn.cursor() as cursor:
            update_fields = [
                "SECURITY_LEVEL_ID = :security_level_id",
                "LANG = :lang",
                "THEME = :theme"
            ]
            params = {
                "security_level_id": security_level_id,
                "lang": lang,
                "theme": theme,
                "edms_user_id": edms_user_id
            }

            if LARGE_DOWNLOADS is not None:
                update_fields.append("LARGE_DOWNLOADS = :LARGE_DOWNLOADS")
                params["LARGE_DOWNLOADS"] = '1' if LARGE_DOWNLOADS else '0'

            update_query = f"""
                UPDATE LKP_EDMS_USR_SECUR 
                SET {', '.join(update_fields)}
                WHERE SYSTEM_ID = :edms_user_id
            """
            await cursor.execute(update_query, params)
            
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


async def get_document_history(docnumber: int):
    """Fetches activity history for a specific document/folder from SIEM_EDMS_VIEW."""
    conn = await get_async_connection()
    if not conn:
        return []

    history = []
    try:
        async with conn.cursor() as cursor:
            query = """
                SELECT 
                    ACTIVITY_DESCRIPTION,
                    DOCNUMBER,
                    TYPIST,
                    DOCNAME,
                    AUTHOR,
                    START_DATE,
                    FORM_NAME
                FROM SIEM_EDMS_VIEW
                WHERE DOCNUMBER = :docnumber
                ORDER BY START_DATE DESC
            """
            await cursor.execute(query, docnumber=docnumber)
            rows = await cursor.fetchall()
            
            for row in rows:
                history.append({
                    'activity_description': row[0],
                    'docnumber': row[1],
                    'typist': row[2],
                    'docname': row[3],
                    'author': row[4],
                    'start_date': row[5].isoformat() if row[5] else None,
                    'form_name': row[6]
                })
    except oracledb.Error as e:
        logging.error(f"Oracle Database error in get_document_history: {e}", exc_info=True)
    finally:
        if conn:
            await conn.close()
            
    return history


async def add_edms_user(user_system_id: int, security_level_id: int, lang: str = 'en', theme: str = 'light', quota: int = 1073741824):
    """Adds a new user to LKP_EDMS_USR_SECUR table."""
    conn = await get_async_connection()
    if not conn:
        return False, "Database connection failed"

    try:
        async with conn.cursor() as cursor:
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


async def get_document_history(docnumber: int):
    """Fetches activity history for a specific document/folder from SIEM_EDMS_VIEW."""
    conn = await get_async_connection()
    if not conn:
        return []

    history = []
    try:
        async with conn.cursor() as cursor:
            query = """
                SELECT 
                    ACTIVITY_DESCRIPTION,
                    DOCNUMBER,
                    TYPIST,
                    DOCNAME,
                    AUTHOR,
                    START_DATE,
                    FORM_NAME
                FROM SIEM_EDMS_VIEW
                WHERE DOCNUMBER = :docnumber
                ORDER BY START_DATE DESC
            """
            await cursor.execute(query, docnumber=docnumber)
            rows = await cursor.fetchall()
            
            for row in rows:
                history.append({
                    'activity_description': row[0],
                    'docnumber': row[1],
                    'typist': row[2],
                    'docname': row[3],
                    'author': row[4],
                    'start_date': row[5].isoformat() if row[5] else None,
                    'form_name': row[6]
                })
    except oracledb.Error as e:
        logging.error(f"Oracle Database error in get_document_history: {e}", exc_info=True)
    finally:
        if conn:
            await conn.close()
            
    return history


async def get_admin_profiles(search: str = ""):
    """Fetches profiles from the FORMS table (form_type = 'P' and name not containing search)."""
    conn = await get_async_connection()
    if not conn:
        return []

    profiles = []
    try:
        async with conn.cursor() as cursor:
            search_condition = ""
            bind_vars = {}
            if search.strip():
                search_condition = """
                    AND (
                        UPPER(FORM_NAME) LIKE UPPER(:search)
                        OR UPPER(FORM_TITLE) LIKE UPPER(:search)
                    )
                """
                bind_vars['search'] = f"%{search}%"

            query = f"""
                SELECT SYSTEM_ID, FORM_NAME, FORM_TITLE 
                FROM FORMS 
                WHERE FORM_TYPE = 'P' 
                AND UPPER(FORM_NAME) NOT LIKE UPPER('%SEARCH%')
                {search_condition}
                ORDER BY FORM_NAME
            """
            await cursor.execute(query, **bind_vars)
            rows = await cursor.fetchall()
            
            for row in rows:
                profiles.append({
                    'system_id': row[0],
                    'form_name': row[1],
                    'form_title': row[2]
                })
    except oracledb.Error as e:
        logging.error(f"Oracle Database error in get_admin_profiles: {e}", exc_info=True)
    finally:
        if conn:
            await conn.close()
            
    return profiles


async def get_profile_groups(profile_system_id: int, search: str = ""):
    """Fetches groups mapped to a specific profile from FORM_USER and GROUPS."""
    conn = await get_async_connection()
    if not conn:
        return []

    groups = []
    try:
        async with conn.cursor() as cursor:
            search_condition = ""
            bind_vars = {'profile_system_id': profile_system_id}
            if search.strip():
                search_condition = """
                    AND (
                        UPPER(g.GROUP_NAME) LIKE UPPER(:search)
                        OR UPPER(g.GROUP_ID) LIKE UPPER(:search)
                    )
                """
                bind_vars['search'] = f"%{search}%"

            query = f"""
                SELECT DISTINCT g.GROUP_ID, g.GROUP_NAME, g.SYSTEM_ID
                FROM FORM_USER fu
                JOIN GROUPS g ON fu.PERSONORGROUP = g.SYSTEM_ID
                WHERE fu.FORM = :profile_system_id
                {search_condition}
                ORDER BY g.GROUP_NAME
            """
            await cursor.execute(query, **bind_vars)
            rows = await cursor.fetchall()
            
            for row in rows:
                groups.append({
                    'group_id': row[0],
                    'group_name': row[1],
                    'system_id': row[2]
                })
    except oracledb.Error as e:
        logging.error(f"Oracle Database error in get_profile_groups: {e}", exc_info=True)
    finally:
        if conn:
            await conn.close()
            
    return groups


async def get_group_users(group_system_id: int, search: str = ""):
    """Fetches active users mapped to a specific group from PEOPLEGROUPS and PEOPLE."""
    conn = await get_async_connection()
    if not conn:
        return []

    users = []
    try:
        async with conn.cursor() as cursor:
            search_condition = ""
            bind_vars = {'group_system_id': group_system_id}
            if search.strip():
                search_condition = """
                    AND (
                        UPPER(p.USER_ID) LIKE UPPER(:search)
                        OR UPPER(p.FULL_NAME) LIKE UPPER(:search)
                    )
                """
                bind_vars['search'] = f"%{search}%"

            query = f"""
                SELECT p.USER_ID, p.FULL_NAME, p.SYSTEM_ID
                FROM PEOPLEGROUPS pg
                JOIN PEOPLE p ON pg.PEOPLE_SYSTEM_ID = p.SYSTEM_ID
                WHERE pg.GROUPS_SYSTEM_ID = :group_system_id
                  AND p.DISABLED = 'N'
                {search_condition}
                ORDER BY p.USER_ID
            """
            await cursor.execute(query, **bind_vars)
            rows = await cursor.fetchall()
            
            for row in rows:
                users.append({
                    'user_id': row[0],
                    'full_name': row[1] or row[0],
                    'system_id': row[2]
                })
    except oracledb.Error as e:
        logging.error(f"Oracle Database error in get_group_users: {e}", exc_info=True)
    finally:
        if conn:
            await conn.close()
            
    return users

async def fetch_checked_out_docs(days: int = 1, date_from: str = None, date_to: str = None) -> list:
    """
    Async query: returns documents checked out where CHECKIN_DATE is NULL.
    Joins PROFILE for DOCNAME.

    Date handling (all interpreted/returned in UTC+4):
      - By default, fetches values as of today (start of today in UTC+4) and
        ignores anything checked out before today.
      - If date_from and/or date_to are provided (YYYY-MM-DD, treated as UTC+4
        dates), the filter uses that range instead.

    The `days` parameter is retained for backwards compatibility of the
    signature but is no longer used as a look-back window.
    """
    conn = None
    try:
        conn = await get_async_connection()
        async with conn.cursor() as cursor:
            params = {}
            if date_from and date_to:
                date_filter = ("c.CHECKOUT_DATE >= TO_TIMESTAMP_TZ(:date_from || ' 00:00:00 +04:00', 'YYYY-MM-DD HH24:MI:SS TZH:TZM') "
                               "AND c.CHECKOUT_DATE < TO_TIMESTAMP_TZ(:date_to || ' 00:00:00 +04:00', 'YYYY-MM-DD HH24:MI:SS TZH:TZM') + 1")
                params["date_from"] = date_from
                params["date_to"] = date_to
            elif date_from:
                date_filter = "c.CHECKOUT_DATE >= TO_TIMESTAMP_TZ(:date_from || ' 00:00:00 +04:00', 'YYYY-MM-DD HH24:MI:SS TZH:TZM')"
                params["date_from"] = date_from
            elif date_to:
                date_filter = "c.CHECKOUT_DATE < TO_TIMESTAMP_TZ(:date_to || ' 00:00:00 +04:00', 'YYYY-MM-DD HH24:MI:SS TZH:TZM') + 1"
                params["date_to"] = date_to
            else:
                # Default: fetch rows from the hardcoded start date 22/09/2026 (UTC+4) onward.
                # Discards anything checked out before 22/09/2026 00:00:00 +04:00.
                date_filter = ("c.CHECKOUT_DATE >= TO_TIMESTAMP_TZ('2026-09-22 00:00:00 +04:00', "
                                "'YYYY-MM-DD HH24:MI:SS TZH:TZM')")

            sql = f"""
                SELECT
                    c.DOCNUMBER,
                    c.SYSTEM_ID,
                    c.CHECKOUT_DATE,
                    c.TYPIST,
                    NVL(p.DOCNAME, '(unknown)') AS DOCNAME
                FROM CHECKOUT c
                LEFT JOIN PROFILE p
                    ON c.DOCNUMBER = p.DOCNUMBER
                WHERE {date_filter}
                ORDER BY c.CHECKOUT_DATE DESC
            """
            await cursor.execute(sql, params)
            rows = await cursor.fetchall()
            columns = [col[0].lower() for col in cursor.description]
            result = []
            for row in rows:
                doc = dict(zip(columns, row))
                # Serialize Oracle Date/timestamp to ISO string in UTC+4
                for k, v in doc.items():
                    if hasattr(v, 'isoformat'):
                        if hasattr(v, 'tzinfo') and v.tzinfo is None:
                            # Naive value coming from DB -> assume it is already
                            # in UTC+4 server time and attach the offset.
                            v = v.replace(tzinfo=UTC_PLUS_4)
                        doc[k] = v.isoformat()
                result.append(doc)
            return result
    finally:
        if conn:
            await conn.close()
