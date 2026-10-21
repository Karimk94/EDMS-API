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
    The name is formatted as "<viewer_email> (via service account <DMS_USER> - <DMS_FULL_NAME>)"
    so the document history in EDMS clearly shows both the viewer's email and the
    service account used to access the DMS — matching what EDMS displays in activity history.
    If the record doesn't exist, it is created. If it exists, its FULL_NAME is always
    refreshed to the current format so history stays accurate.
    """
    connection = None
    try:
        dms_user = os.getenv('DMS_USER')

        connection = await get_async_connection()
        with connection.cursor() as cursor:
            # 1. Get Full Name of DMS_USER from HR table (for context in the display name)
            await cursor.execute(
                "SELECT FULLNAME_EN FROM LKP_HR_EMPLOYEES WHERE UPPER(LOGIN) = UPPER(:1) FETCH FIRST 1 ROW ONLY",
                [dms_user]
            )
            row = await cursor.fetchone()
            dms_full_name = row[0] if row else dms_user

            # Build the canonical display name: "viewer@rta.ae (via service account SVC - Full Name)"
            # This is what appears next to every activity entry in the EDMS document history.
            virtual_full_name = f"{viewer_email} (via service account {dms_user} - {dms_full_name})"
            user_id_val = viewer_email[:50]

            # 2. Look up an existing virtual person for this email (ALLOW_LOGIN = 'N' marks
            #    middleware-created virtual persons so they are never confused with real users).
            await cursor.execute(
                "SELECT SYSTEM_ID FROM PEOPLE WHERE UPPER(USER_ID) = UPPER(:1) AND ALLOW_LOGIN = 'N'",
                [user_id_val]
            )
            row = await cursor.fetchone()
            if row:
                existing_sys_id = row[0]
                # Always refresh the FULL_NAME so the EDMS history shows the current
                # format (email + service account) even for records created before
                # this naming convention was introduced.
                await cursor.execute(
                    "UPDATE PEOPLE SET FULL_NAME = :1, EMAIL_ADDRESS = :2, LAST_UPDATE = SYSDATE WHERE SYSTEM_ID = :3",
                    [virtual_full_name, viewer_email, existing_sys_id]
                )
                await connection.commit()
                return existing_sys_id

            # 3. Also check by FULL_NAME in case the USER_ID lookup missed it
            await cursor.execute(
                "SELECT SYSTEM_ID FROM PEOPLE WHERE FULL_NAME = :1",
                [virtual_full_name]
            )
            row = await cursor.fetchone()
            if row:
                return row[0]

            # 4. Insert a new virtual person record
            await cursor.execute("SELECT SEQSYSTEMKEY.NEXTVAL FROM dual")
            new_sys_id = (await cursor.fetchone())[0]

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
