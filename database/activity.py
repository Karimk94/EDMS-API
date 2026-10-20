import logging
import os
import db_connector
from database.connection import get_async_connection

# Cache for activity codes to avoid DB lookup every time
_ACTIVITY_CODES = {}

async def ensure_activity_types():
    """Ensure that 'Download' and 'View Shared Link' exist in ACTIVITY_DESCRIPTION and cache their codes."""
    global _ACTIVITY_CODES
    if 'Download' in _ACTIVITY_CODES and 'View Shared Link' in _ACTIVITY_CODES:
        return

    connection = None
    try:
        connection = await get_async_connection()
        with connection.cursor() as cursor:
            for activity_name in ['Download', 'View Shared Link']:
                await cursor.execute(
                    "SELECT CODE FROM ACTIVITY_DESCRIPTION WHERE UPPER(ACTIVITY_DESCRIPTION) = UPPER(:1)",
                    [activity_name]
                )
                row = await cursor.fetchone()
                if row:
                    _ACTIVITY_CODES[activity_name] = row[0]
                else:
                    # Insert new code
                    await cursor.execute("SELECT NVL(MAX(CODE), 0) + 1 FROM ACTIVITY_DESCRIPTION")
                    new_code = (await cursor.fetchone())[0]
                    await cursor.execute(
                        "INSERT INTO ACTIVITY_DESCRIPTION (CODE, ACTIVITY_DESCRIPTION) VALUES (:1, :2)",
                        [new_code, activity_name]
                    )
                    _ACTIVITY_CODES[activity_name] = new_code
            await connection.commit()
    except Exception as e:
        if connection:
            await connection.rollback()
        logging.error(f"Error ensuring activity types: {e}")
    finally:
        if connection:
            await connection.close()


async def get_or_create_viewer_person(viewer_email: str) -> int:
    """
    Returns the PEOPLE.SYSTEM_ID for the shared link viewer.
    The name is formatted as "<viewer_email> (via service account <DMS_USER>)"
    so the document history clearly shows both the service account used to
    access the DMS and the RTA user (email) that actually viewed/downloaded.
    If it doesn't exist, it is created.
    """
    connection = None
    try:
        dms_user = os.getenv('DMS_USER', 'okool_kaabdulwahed')
        
        connection = await get_async_connection()
        with connection.cursor() as cursor:
            # 0. Reuse an existing viewer entry for this email (any naming format)
            #    so history stays consistent across format changes.
            await cursor.execute(
                "SELECT SYSTEM_ID FROM PEOPLE WHERE UPPER(USER_ID) = UPPER(:1) AND ALLOW_LOGIN = 'N'",
                [viewer_email[:50]]
            )
            row = await cursor.fetchone()
            if row:
                return row[0]

            # 1. Get Full Name of DMS_USER from HR table (for context in the name)
            await cursor.execute(
                "SELECT FULLNAME_EN FROM LKP_HR_EMPLOYEES WHERE UPPER(LOGIN) = UPPER(:1) FETCH FIRST 1 ROW ONLY",
                [dms_user]
            )
            row = await cursor.fetchone()
            dms_full_name = row[0] if row else dms_user
            
            # Format the virtual user's full name so the history shows that the
            # document was accessed with the service account by the RTA user.
            virtual_full_name = f"{viewer_email} (via service account {dms_user} - {dms_full_name})"
            
            # 2. Check if this virtual person already exists in PEOPLE
            await cursor.execute(
                "SELECT SYSTEM_ID FROM PEOPLE WHERE FULL_NAME = :1",
                [virtual_full_name]
            )
            row = await cursor.fetchone()
            if row:
                return row[0]
                
            # 3. Insert new virtual person
            await cursor.execute("SELECT SEQSYSTEMKEY.NEXTVAL FROM dual")
            new_sys_id = (await cursor.fetchone())[0]
            
            # Use a dummy USER_ID, e.g. viewer_email (ensure it fits in the column, typically 32-50 chars)
            user_id_val = viewer_email[:50]
            
            profile_defaults = f"0;4;AUTHOR.USER_ID;DOCSADM.PEOPLE.SYSTEM_ID='{user_id_val}'"
            
            await cursor.execute("""
                INSERT INTO PEOPLE (
                    SYSTEM_ID, USER_ID, FULL_NAME, EMAIL_ADDRESS,
                    ALLOW_LOGIN, DISABLED, LAST_UPDATE, PROFILE_DEFAULTS
                ) VALUES (
                    :1, :2, :3, :4,
                    'N', 'Y', SYSDATE, :5
                )
            """, [new_sys_id, user_id_val, virtual_full_name, viewer_email, profile_defaults])
            
            await connection.commit()
            return new_sys_id
            
    except Exception as e:
        if connection:
            await connection.rollback()
        logging.error(f"Error getting/creating viewer person: {e}")
        return None
    finally:
        if connection:
            await connection.close()

async def log_activity(docnumber: int, activity_name: str, typist_system_id: int):
    """
    Inserts a record into ACTIVITYLOG.
    Requires ensuring activity types beforehand.
    """
    if not typist_system_id:
        logging.warning(f"Cannot log activity '{activity_name}' for doc {docnumber}: missing typist_system_id")
        return

    await ensure_activity_types()
    activity_code = _ACTIVITY_CODES.get(activity_name)
    if not activity_code:
        logging.error(f"Activity code for {activity_name} not found.")
        return

    connection = None
    try:
        connection = await get_async_connection()
        with connection.cursor() as cursor:
            await cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM ACTIVITYLOG")
            new_sys_id = (await cursor.fetchone())[0]
            
            # Default values seen in DB
            # START_DATE is sysdate
            # REF_LIBRARY = -1
            # TYPE_TIME, KEYSTROKES, PAGES, ELAPSED_TIME = 0
            
            await cursor.execute("""
                INSERT INTO ACTIVITYLOG (
                    SYSTEM_ID, DOCNUMBER, ACTIVITY_TYPE, TYPIST, AUTHOR,
                    START_DATE, REF_LIBRARY, TYPE_TIME, KEYSTROKES, PAGES, ELAPSED_TIME
                ) VALUES (
                    :1, :2, :3, :4, :5,
                    SYSDATE, -1, 0, 0, 0, 0
                )
            """, [new_sys_id, docnumber, activity_code, typist_system_id, typist_system_id])
            
            await connection.commit()
    except Exception as e:
        if connection:
            await connection.rollback()
        logging.error(f"Error inserting ACTIVITYLOG for {docnumber} ({activity_name}): {e}")
    finally:
        if connection:
            await connection.close()
