import oracledb
import logging
from database.connection import get_async_connection


async def log_login(
    people_system_id: int,
    session_id: str,
    app_source: str = None,
    ip_address: str = None,
):
    """Inserts a successful login record into LKP_AS_SE with an active session marker."""
    if not people_system_id:
        logging.warning("Cannot log login: missing people_system_id")
        return False
    if not session_id:
        logging.warning("Cannot log login: missing session_id")
        return False

    conn = await get_async_connection()
    if not conn:
        return False

    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                INSERT INTO LKP_AS_SE (
                    SYSTEM_ID, PEOPLE_SYSTEM_ID, SESSION_ID, LOGIN_DATE, LAST_ACTIVITY,
                    IS_ACTIVE, APP_SOURCE, IP_ADDRESS, LAST_UPDATE
                ) VALUES (
                    (SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_AS_SE),
                    :people_system_id,
                    :session_id,
                    SYSDATE,
                    SYSDATE,
                    'Y',
                    :app_source,
                    :ip_address,
                    SYSDATE
                )
                """,
                people_system_id=people_system_id,
                session_id=session_id,
                app_source=app_source,
                ip_address=ip_address,
            )
            await conn.commit()
            return True
    except oracledb.Error as e:
        logging.error(f"Oracle Database error in log_login: {e}", exc_info=True)
        try:
            await conn.rollback()
        except Exception:
            pass
        return False
    finally:
        if conn:
            await conn.close()


async def mark_session_logout(session_id: str):
    """Marks a session inactive with a logout timestamp. Used on normal logout."""
    if not session_id:
        return

    conn = await get_async_connection()
    if not conn:
        return

    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                UPDATE LKP_AS_SE
                SET IS_ACTIVE = 'N',
                    LOGOUT_DATE = SYSDATE,
                    LAST_UPDATE = SYSDATE
                WHERE SESSION_ID = :session_id
                  AND IS_ACTIVE = 'Y'
                """,
                session_id=session_id,
            )
            await conn.commit()
    except oracledb.Error as e:
        logging.error(f"Oracle Database error in mark_session_logout: {e}", exc_info=True)
        try:
            await conn.rollback()
        except Exception:
            pass
    finally:
        if conn:
            await conn.close()


async def terminate_session(session_id: str):
    """
    Terminates an active session by setting IS_ACTIVE='N' and recording the
    termination timestamp. Returns (success, message).
    """
    if not session_id:
        return False, "session_id is required"

    conn = await get_async_connection()
    if not conn:
        return False, "Database connection failed"

    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                UPDATE LKP_AS_SE
                SET IS_ACTIVE = 'N',
                    LOGOUT_DATE = SYSDATE,
                    TERMINATED_DATE = SYSDATE,
                    LAST_UPDATE = SYSDATE
                WHERE SESSION_ID = :session_id
                  AND IS_ACTIVE = 'Y'
                """,
                session_id=session_id,
            )
            if cursor.rowcount == 0:
                return False, "Session not found or already inactive"
            await conn.commit()
            return True, "Session terminated successfully"
    except oracledb.Error as e:
        logging.error(f"Oracle Database error in terminate_session: {e}", exc_info=True)
        try:
            await conn.rollback()
        except Exception:
            pass
        return False, str(e)
    finally:
        if conn:
            await conn.close()


async def is_session_active(session_id: str) -> bool:
    """
    Returns True if the session is active (or if no DB row exists, to avoid
    breaking legacy sessions). Returns False only when explicitly IS_ACTIVE='N'.
    """
    if not session_id:
        return True

    conn = await get_async_connection()
    if not conn:
        return True

    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                "SELECT IS_ACTIVE FROM LKP_AS_SE WHERE SESSION_ID = :session_id",
                session_id=session_id,
            )
            row = await cursor.fetchone()
            if not row:
                return True
            return str(row[0]).strip().upper() == 'Y'
    except oracledb.Error as e:
        logging.error(f"Oracle Database error in is_session_active: {e}", exc_info=True)
        return True
    finally:
        if conn:
            await conn.close()


async def get_active_sessions():
    """Fetches all active sessions joined with PEOPLE and security level."""
    conn = await get_async_connection()
    if not conn:
        return []

    sessions = []
    try:
        async with conn.cursor() as cursor:
            query = """
                SELECT
                    l.SYSTEM_ID,
                    l.SESSION_ID,
                    l.PEOPLE_SYSTEM_ID,
                    p.USER_ID,
                    p.FULL_NAME,
                    sl.NAME as security_level,
                    l.LOGIN_DATE,
                    l.LAST_ACTIVITY,
                    l.APP_SOURCE,
                    l.IP_ADDRESS
                FROM LKP_AS_SE l
                JOIN PEOPLE p ON l.PEOPLE_SYSTEM_ID = p.SYSTEM_ID
                LEFT JOIN LKP_EDMS_USR_SECUR us ON us.USER_ID = p.SYSTEM_ID
                LEFT JOIN LKP_EDMS_SECURITY sl ON us.SECURITY_LEVEL_ID = sl.SYSTEM_ID
                WHERE l.IS_ACTIVE = 'Y'
                ORDER BY l.LAST_ACTIVITY DESC, l.LOGIN_DATE DESC
            """
            await cursor.execute(query)
            rows = await cursor.fetchall()

            for row in rows:
                sessions.append({
                    'system_id': row[0],
                    'session_id': row[1],
                    'people_system_id': row[2],
                    'username': row[3],
                    'full_name': row[4] or row[3],
                    'security_level': row[5],
                    'login_date': row[6].isoformat() if row[6] else None,
                    'last_activity': row[7].isoformat() if row[7] else None,
                    'app_source': row[8],
                    'ip_address': row[9],
                })
    except oracledb.Error as e:
        logging.error(f"Oracle Database error in get_active_sessions: {e}", exc_info=True)
    finally:
        if conn:
            await conn.close()

    return sessions


async def get_login_history(search: str = "", page: int = 1, limit: int = 20):
    """
    Fetches Smart EDMS login history from LKP_AS_SE, joining PEOPLE for
    username/full name and LKP_EDMS_USR_SECUR + LKP_EDMS_SECURITY for security level.
    """
    conn = await get_async_connection()
    if not conn:
        return {"logs": [], "total": 0, "has_more": False}

    logs = []
    total = 0
    try:
        async with conn.cursor() as cursor:
            search_condition = ""
            if search and search.strip():
                search_condition = """
                    AND (
                        UPPER(p.USER_ID) LIKE UPPER(:search)
                        OR UPPER(p.FULL_NAME) LIKE UPPER(:search)
                        OR UPPER(sl.NAME) LIKE UPPER(:search)
                        OR UPPER(l.APP_SOURCE) LIKE UPPER(:search)
                    )
                """

            count_query = f"""
                SELECT COUNT(*)
                FROM LKP_AS_SE l
                JOIN PEOPLE p ON l.PEOPLE_SYSTEM_ID = p.SYSTEM_ID
                LEFT JOIN LKP_EDMS_USR_SECUR us ON us.USER_ID = p.SYSTEM_ID
                LEFT JOIN LKP_EDMS_SECURITY sl ON us.SECURITY_LEVEL_ID = sl.SYSTEM_ID
                WHERE 1=1 {search_condition}
            """
            search_pattern = f"%{search}%"
            if search and search.strip():
                await cursor.execute(count_query, search=search_pattern)
            else:
                await cursor.execute(count_query)
            count_result = await cursor.fetchone()
            total = count_result[0] if count_result else 0

            offset = (page - 1) * limit
            data_query = f"""
                SELECT * FROM (
                    SELECT
                        l.SYSTEM_ID,
                        l.PEOPLE_SYSTEM_ID,
                        l.SESSION_ID,
                        p.USER_ID,
                        p.FULL_NAME,
                        sl.NAME as security_level,
                        l.LOGIN_DATE,
                        l.LOGOUT_DATE,
                        l.LAST_ACTIVITY,
                        l.IS_ACTIVE,
                        l.APP_SOURCE,
                        l.IP_ADDRESS,
                        ROW_NUMBER() OVER (ORDER BY l.LOGIN_DATE DESC, l.SYSTEM_ID DESC) as rn
                    FROM LKP_AS_SE l
                    JOIN PEOPLE p ON l.PEOPLE_SYSTEM_ID = p.SYSTEM_ID
                    LEFT JOIN LKP_EDMS_USR_SECUR us ON us.USER_ID = p.SYSTEM_ID
                    LEFT JOIN LKP_EDMS_SECURITY sl ON us.SECURITY_LEVEL_ID = sl.SYSTEM_ID
                    WHERE 1=1 {search_condition}
                )
                WHERE rn > :offset AND rn <= :end_row
            """
            end_row = offset + limit
            if search and search.strip():
                await cursor.execute(
                    data_query,
                    search=search_pattern,
                    offset=offset,
                    end_row=end_row,
                )
            else:
                await cursor.execute(data_query, offset=offset, end_row=end_row)
            rows = await cursor.fetchall()

            for row in rows:
                logs.append({
                    'system_id': row[0],
                    'people_system_id': row[1],
                    'session_id': row[2],
                    'username': row[3],
                    'full_name': row[4] or row[3],
                    'security_level': row[5],
                    'login_date': row[6].isoformat() if row[6] else None,
                    'logout_date': row[7].isoformat() if row[7] else None,
                    'last_activity': row[8].isoformat() if row[8] else None,
                    'is_active': str(row[9]).strip().upper() == 'Y' if row[9] is not None else False,
                    'app_source': row[10],
                    'ip_address': row[11],
                })
    except oracledb.Error as e:
        logging.error(f"Oracle Database error in get_login_history: {e}", exc_info=True)
    finally:
        if conn:
            await conn.close()

    has_more = (page * limit) < total
    return {"logs": logs, "total": total, "has_more": has_more}