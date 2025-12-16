import oracledb
import logging
from database.connection import get_async_connection

async def get_user_security_level(username):
    """Fetches the user's security level name from the database using their user ID from the PEOPLE table."""
    conn = await get_async_connection()
    if not conn:
        return None  # Return None if DB connection fails

    security_level = None  # Default value is now None
    try:
        async with conn.cursor() as cursor:
            # Use upper for case-insensitive comparison
            await cursor.execute("SELECT SYSTEM_ID FROM PEOPLE WHERE UPPER(USER_ID) = UPPER(:username)", username=username)
            user_result = await cursor.fetchone()

            if user_result:
                user_id = user_result[0]

                # Now, get the security level using the user_id
                query = """
                    SELECT sl.NAME
                    FROM LKP_EDMS_USR_SECUR us
                    JOIN LKP_EDMS_SECURITY sl ON us.SECURITY_LEVEL_ID = sl.SYSTEM_ID
                    WHERE us.USER_ID = :user_id
                """
                await cursor.execute(query, user_id=user_id)
                level_result = await cursor.fetchone()
                if level_result:
                    security_level = level_result[0]
                else:
                    logging.warning(f"No security level found for user_id {user_id} (DMS user: {username})")
            else:
                 logging.warning(f"No PEOPLE record found for DMS user: {username}")
                 # If level_result is None, security_level remains None and will be returned
    except oracledb.Error as e:
        logging.error(f"Oracle Database error in get_user_security_level for {username}: {e}", exc_info=True)
    finally:
        if conn:
            await conn.close()
    return security_level

async def get_user_details(username):
    """Fetches user details including security level, language, and theme preference."""
    conn = await get_async_connection()
    if not conn:
        return None

    user_details = None
    try:
        async with conn.cursor() as cursor:
            # First, get the USER_ID from the PEOPLE table
            await cursor.execute("SELECT SYSTEM_ID FROM PEOPLE WHERE UPPER(USER_ID) = UPPER(:username)", username=username)
            user_result = await cursor.fetchone()

            if user_result:
                user_id = user_result[0]

                # Now, get details from the EDMS security table
                query = """
                    SELECT sl.NAME, us.LANG, us.THEME
                    FROM LKP_EDMS_USR_SECUR us
                    JOIN LKP_EDMS_SECURITY sl ON us.SECURITY_LEVEL_ID = sl.SYSTEM_ID
                    WHERE us.USER_ID = :user_id
                """
                await cursor.execute(query, user_id=user_id)
                details_result = await cursor.fetchone()

                if details_result:
                    security_level, lang, theme = details_result
                    user_details = {
                        'username': username,
                        'security_level': security_level,
                        'lang': lang or 'en',  # Default to 'en'
                        'theme': theme or 'light' # Default to 'light'
                    }
                else:
                    logging.warning(f"No security details found for user_id {user_id} (DMS user: {username})")
            else:
                logging.warning(f"No PEOPLE record found for DMS user: {username}")

    except oracledb.Error as e:
        logging.error(f"Oracle Database error in get_user_details for {username}: {e}", exc_info=True)
    finally:
        if conn:
            await conn.close()

    return user_details

async def update_user_language(username, lang):
    """Updates the language preference for a user."""
    conn = await get_async_connection()
    if not conn:
        return False

    try:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT SYSTEM_ID FROM PEOPLE WHERE UPPER(USER_ID) = UPPER(:username)", username=username)
            user_result = await cursor.fetchone()

            if not user_result:
                logging.error(f"Cannot update language. User '{username}' not found in PEOPLE table.")
                return False

            user_id = user_result[0]

            # Update the LANG in the security table
            update_query = """
                UPDATE LKP_EDMS_USR_SECUR
                SET LANG = :lang
                WHERE USER_ID = :user_id
            """
            await cursor.execute(update_query, lang=lang, user_id=user_id)

            if cursor.rowcount == 0:
                logging.warning(f"No rows updated for user '{username}' (user_id: {user_id}). They may not have a security record.")
                return False

            await conn.commit()
            return True

    except oracledb.Error as e:
        logging.error(f"Oracle Database error in update_user_language for {username}: {e}", exc_info=True)
        await conn.rollback()
        return False
    finally:
        if conn:
            await conn.close()

async def update_user_theme(username, theme):
    """Updates the theme preference for a user."""
    conn = await get_async_connection()
    if not conn:
        return False

    if theme not in ['light', 'dark']:
        logging.error(f"Invalid theme value '{theme}' for user '{username}'.")
        return False

    try:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT SYSTEM_ID FROM PEOPLE WHERE UPPER(USER_ID) = UPPER(:username)", username=username)
            user_result = await cursor.fetchone()

            if not user_result:
                logging.error(f"Cannot update theme. User '{username}' not found in PEOPLE table.")
                return False

            user_id = user_result[0]

            # Update the THEME in the security table
            update_query = """
                UPDATE LKP_EDMS_USR_SECUR
                SET THEME = :theme
                WHERE USER_ID = :user_id
            """
            await cursor.execute(update_query, theme=theme, user_id=user_id)

            if cursor.rowcount == 0:
                logging.warning(f"No rows updated for user '{username}' (user_id: {user_id}). They may not have a security record.")
                return False

            await conn.commit()
            return True

    except oracledb.Error as e:
        logging.error(f"Oracle Database error in update_user_theme for {username}: {e}", exc_info=True)
        await conn.rollback()
        return False
    finally:
        if conn:
            await conn.close()

async def get_user_system_id(username):
    """Fetches the SYSTEM_ID from the PEOPLE table for a given username."""
    conn = await get_async_connection()
    if not conn:
        return None

    system_id = None
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT SYSTEM_ID FROM PEOPLE WHERE UPPER(USER_ID) = UPPER(:username)", username=username)
            result = await cursor.fetchone()
            if result:
                system_id = result[0]
            else:
                logging.warning(f"No SYSTEM_ID found for user: {username}")
    except oracledb.Error as e:
        logging.error(f"Oracle Database error in get_user_system_id for {username}: {e}", exc_info=True)
    finally:
        if conn:
            await conn.close()
    return system_id