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

                # Now, get details from the EDMS security and data tables
                query = """
                    SELECT sl.NAME, us.LANG, us.THEME, us.VIEW_MODE, us.PROFILE_VIEW_MODE, us.LARGE_DOWNLOADS,
                           COALESCE(ud.QUOTA, 1073741824), 
                           COALESCE(ud.REMAINING_QUOTA, COALESCE(ud.QUOTA, 1073741824)),
                           p.FULL_NAME
                    FROM LKP_EDMS_USR_SECUR us
                    JOIN LKP_EDMS_SECURITY sl ON us.SECURITY_LEVEL_ID = sl.SYSTEM_ID
                    LEFT JOIN LKP_EDMS_USR_DATA ud ON us.SYSTEM_ID = ud.USER_ID
                    JOIN PEOPLE p ON us.USER_ID = p.SYSTEM_ID
                    WHERE us.USER_ID = :user_id
                """
                try:
                    await cursor.execute(query, user_id=user_id)
                    details_result = await cursor.fetchone()
                    has_profile_col = True
                except oracledb.DatabaseError as db_err:
                    err_obj, = db_err.args
                    if hasattr(err_obj, 'code') and err_obj.code == 904:
                        # Fallback for when PROFILE_VIEW_MODE column is not yet created in Oracle
                        legacy_query = """
                            SELECT sl.NAME, us.LANG, us.THEME, us.VIEW_MODE, us.LARGE_DOWNLOADS,
                                   COALESCE(ud.QUOTA, 1073741824), 
                                   COALESCE(ud.REMAINING_QUOTA, COALESCE(ud.QUOTA, 1073741824)),
                                   p.FULL_NAME
                            FROM LKP_EDMS_USR_SECUR us
                            JOIN LKP_EDMS_SECURITY sl ON us.SECURITY_LEVEL_ID = sl.SYSTEM_ID
                            LEFT JOIN LKP_EDMS_USR_DATA ud ON us.SYSTEM_ID = ud.USER_ID
                            JOIN PEOPLE p ON us.USER_ID = p.SYSTEM_ID
                            WHERE us.USER_ID = :user_id
                        """
                        await cursor.execute(legacy_query, user_id=user_id)
                        details_result = await cursor.fetchone()
                        has_profile_col = False
                    else:
                        raise

                if details_result:
                    if has_profile_col:
                        security_level, lang, theme, view_mode, profile_view_mode, LARGE_DOWNLOADS, quota, remaining_quota, full_name = details_result
                    else:
                        security_level, lang, theme, view_mode, LARGE_DOWNLOADS, quota, remaining_quota, full_name = details_result
                        profile_view_mode = 'grid'

                    # Also fetch the security_level_id for tab permissions lookup
                    await cursor.execute(
                        "SELECT SECURITY_LEVEL_ID FROM LKP_EDMS_USR_SECUR WHERE USER_ID = :user_id",
                        user_id=user_id
                    )
                    sec_id_result = await cursor.fetchone()
                    security_level_id = sec_id_result[0] if sec_id_result else None

                    user_details = {
                        'username': username,
                        'full_name': full_name,
                        'people_system_id': user_id,
                        'security_level': security_level,
                        'security_level_id': security_level_id,
                        'lang': lang or 'en',
                        'theme': theme or 'light',
                        'view_mode': view_mode or 'grid',
                        'profile_view_mode': profile_view_mode or 'grid',
                        'quota': quota,
                        'remaining_quota': remaining_quota,
                        'LARGE_DOWNLOADS': LARGE_DOWNLOADS == '1' if LARGE_DOWNLOADS is not None else False
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

async def update_user_profile_view_mode(username, profile_view_mode):
    """Updates the profile search view mode preference for a user."""
    conn = await get_async_connection()
    if not conn:
        return False

    try:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT SYSTEM_ID FROM PEOPLE WHERE UPPER(USER_ID) = UPPER(:username)", username=username)
            user_result = await cursor.fetchone()

            if not user_result:
                logging.error(f"Cannot update profile view mode. User '{username}' not found in PEOPLE table.")
                return False

            user_id = user_result[0]

            update_query = """
                UPDATE LKP_EDMS_USR_SECUR
                SET PROFILE_VIEW_MODE = :profile_view_mode
                WHERE USER_ID = :user_id
            """
            await cursor.execute(update_query, profile_view_mode=profile_view_mode, user_id=user_id)

            if cursor.rowcount == 0:
                logging.warning(f"No rows updated for user '{username}' (user_id: {user_id}). They may not have a security record.")
                return False

            await conn.commit()
            return True

    except oracledb.Error as e:
        logging.error(f"Oracle Database error in update_user_profile_view_mode for {username}: {e}", exc_info=True)
        await conn.rollback()
        return False
    finally:
        if conn:
            await conn.close()

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

async def update_user_view_mode(username, view_mode):
    """Updates the view mode preference for a user."""
    conn = await get_async_connection()
    if not conn:
        return False

    try:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT SYSTEM_ID FROM PEOPLE WHERE UPPER(USER_ID) = UPPER(:username)", username=username)
            user_result = await cursor.fetchone()

            if not user_result:
                logging.error(f"Cannot update view mode. User '{username}' not found in PEOPLE table.")
                return False

            user_id = user_result[0]

            update_query = """
                UPDATE LKP_EDMS_USR_SECUR
                SET VIEW_MODE = :view_mode
                WHERE USER_ID = :user_id
            """
            await cursor.execute(update_query, view_mode=view_mode, user_id=user_id)

            if cursor.rowcount == 0:
                logging.warning(f"No rows updated for user '{username}' (user_id: {user_id}). They may not have a security record.")
                return False

            await conn.commit()
            return True

    except oracledb.Error as e:
        logging.error(f"Oracle Database error in update_user_view_mode for {username}: {e}", exc_info=True)
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

async def ensure_edms_user(username: str) -> tuple[dict | None, str | None]:
    """
    Checks if a user has an active Smart EDMS account.
    If not, but they exist in the PEOPLE table, automatically provisions a minimal
    account in LKP_EDMS_USR_SECUR (minimal Viewer level, default quota, no tab permissions).
    Returns (user_details, None) on success, or (None, error_message) on failure.
    """
    cleaned_username = str(username or '').strip()
    if not cleaned_username:
        return None, "Username cannot be empty"

    # 1. First attempt to load existing details
    user_details = await get_user_details(cleaned_username)
    if user_details:
        return user_details, None

    # 2. Check if user exists in PEOPLE table
    conn = await get_async_connection()
    if not conn:
        return None, "Database connection failed"

    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                "SELECT SYSTEM_ID, FULL_NAME FROM PEOPLE WHERE UPPER(USER_ID) = UPPER(:username)",
                username=cleaned_username
            )
            people_row = await cursor.fetchone()
            if not people_row:
                return None, f"User '{cleaned_username}' is not registered in the organization directory (PEOPLE table)"

            people_system_id, full_name = people_row[0], people_row[1]

            # 3. Check if LKP_EDMS_USR_SECUR already has this user
            await cursor.execute(
                "SELECT SYSTEM_ID FROM LKP_EDMS_USR_SECUR WHERE USER_ID = :user_id",
                user_id=people_system_id
            )
            secur_row = await cursor.fetchone()

            if not secur_row:
                # Find minimal security level (Viewer)
                await cursor.execute("SELECT SYSTEM_ID FROM LKP_EDMS_SECURITY WHERE UPPER(NAME) = 'VIEWER'")
                viewer_row = await cursor.fetchone()
                if viewer_row:
                    sec_level_id = viewer_row[0]
                else:
                    await cursor.execute("SELECT SYSTEM_ID FROM LKP_EDMS_SECURITY ORDER BY SYSTEM_ID ASC FETCH FIRST 1 ROWS ONLY")
                    sec_row = await cursor.fetchone()
                    sec_level_id = sec_row[0] if sec_row else 0

                # Insert into LKP_EDMS_USR_SECUR with minimal privileges and defaults
                insert_secur_sql = """
                    INSERT INTO LKP_EDMS_USR_SECUR (
                        SYSTEM_ID, USER_ID, SECURITY_LEVEL_ID, LANG, THEME, VIEW_MODE, DISABLED, SECTION, LARGE_DOWNLOADS
                    ) VALUES (
                        (SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_EDMS_USR_SECUR),
                        :user_id,
                        :security_level_id,
                        'en',
                        'light',
                        'grid',
                        '0',
                        '0000',
                        '0'
                    )
                """
                try:
                    await cursor.execute(
                        insert_secur_sql,
                        user_id=people_system_id,
                        security_level_id=sec_level_id
                    )
                except oracledb.DatabaseError as db_err:
                    err_obj, = db_err.args
                    if hasattr(err_obj, 'code') and err_obj.code == 904:
                        # Fallback if VIEW_MODE or LARGE_DOWNLOADS column does not exist
                        fallback_sql = """
                            INSERT INTO LKP_EDMS_USR_SECUR (
                                SYSTEM_ID, USER_ID, SECURITY_LEVEL_ID, LANG, THEME, DISABLED, SECTION
                            ) VALUES (
                                (SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_EDMS_USR_SECUR),
                                :user_id,
                                :security_level_id,
                                'en',
                                'light',
                                '0',
                                '0000'
                            )
                        """
                        await cursor.execute(fallback_sql, user_id=people_system_id, security_level_id=sec_level_id)
                    else:
                        raise

                # Get the newly created EDMS User ID
                await cursor.execute("SELECT SYSTEM_ID FROM LKP_EDMS_USR_SECUR WHERE USER_ID = :1", [people_system_id])
                sec_rec = await cursor.fetchone()
                if sec_rec:
                    edms_user_id = sec_rec[0]
                    # Initialize Quota in LKP_EDMS_USR_DATA (1 GB default)
                    await cursor.execute("SELECT COUNT(*) FROM LKP_EDMS_USR_DATA WHERE USER_ID = :1", [edms_user_id])
                    quota_count = (await cursor.fetchone())[0]
                    if quota_count == 0:
                        await cursor.execute(
                            "INSERT INTO LKP_EDMS_USR_DATA (SYSTEM_ID, USER_ID, REMAINING_QUOTA, QUOTA, DISABLED) "
                            "VALUES ((SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_EDMS_USR_DATA), :1, 1073741824, 1073741824, '0')",
                            [edms_user_id]
                        )

                # Commit transaction — minimal levels, no tab permissions created
                await conn.commit()
                logging.info(f"Auto-provisioned new EDMS user '{cleaned_username}' (PEOPLE SYSTEM_ID: {people_system_id}) with minimal Viewer access.")

    except oracledb.IntegrityError:
        # If another concurrent request inserted the row simultaneously, ignore and retrieve
        if conn:
            await conn.rollback()
    except Exception as e:
        logging.error(f"Error auto-provisioning user '{cleaned_username}': {e}", exc_info=True)
        if conn:
            await conn.rollback()
        return None, f"Failed to auto-provision user: {e}"
    finally:
        if conn:
            await conn.close()

    # 4. Fetch the newly created or existing user details
    user_details = await get_user_details(cleaned_username)
    if user_details:
        return user_details, None

    return None, f"Could not retrieve user details for '{cleaned_username}' after provisioning"