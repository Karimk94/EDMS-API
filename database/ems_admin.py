import oracledb
import logging
from datetime import datetime
from database.connection import get_async_connection

# --- Constants ---
SECTION_DEPTID = 8988  # Fixed DEPTID for companies/sections in LKP_SECTION

# --- Helper Functions for ID Generation ---

async def get_next_system_id_sequence():
    """Get the next SYSTEM_ID from Oracle sequence SEQSYSTEMKEY."""
    conn = await get_async_connection()
    if not conn:
        return None
    
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT SEQSYSTEMKEY.NEXTVAL FROM dual")
            result = await cursor.fetchone()
            return result[0] if result else None
    except oracledb.Error as ex:
        error, = ex.args
        logging.error(f"Error getting next SYSTEM_ID: {error.message}")
        return None
    finally:
        if conn:
            await conn.close()


async def get_next_secid():
    """Generate next SECID globally (pattern: A0001, A0002, ..., A9999, B0001, etc.).
    Includes a uniqueness verification loop to prevent duplicate SECIDs."""
    conn = await get_async_connection()
    if not conn:
        return None
    
    try:
        async with conn.cursor() as cursor:
            # Get the last SECID globally
            await cursor.execute(
                "SELECT SECID FROM LKP_SECTION ORDER BY SYSTEM_ID DESC FETCH NEXT 1 ROW ONLY"
            )
            result = await cursor.fetchone()
            
            if result and result[0]:
                last_secid = result[0]
                # Parse format like "A0001"
                if len(last_secid) == 5 and last_secid[0].isalpha() and last_secid[1:].isdigit():
                    prefix = last_secid[0].upper()
                    number = int(last_secid[1:])
                    
                    if number >= 9999:
                        # Move to next letter
                        prefix = chr(ord(prefix) + 1)
                        number = 1
                        if prefix > 'Z':
                            logging.warning("SECID sequence reached Z9999 limit")
                            return None
                    else:
                        number += 1
                    
                    new_secid = f"{prefix}{number:04d}"
                else:
                    new_secid = "A0001"
            else:
                # Start with A0001 if no records exist
                new_secid = "A0001"
            
            # Verify uniqueness - loop until we find an unused SECID
            while True:
                await cursor.execute(
                    "SELECT COUNT(*) FROM LKP_SECTION WHERE SECID = :secid",
                    secid=new_secid
                )
                count_result = await cursor.fetchone()
                if count_result[0] == 0:
                    return new_secid
                else:
                    # SECID already exists, increment and try again
                    current_prefix = new_secid[0].upper()
                    current_number = int(new_secid[1:])
                    
                    if current_number >= 9999:
                        current_number = 1
                        current_prefix = chr(ord(current_prefix) + 1)
                        if current_prefix > 'Z':
                            logging.warning("SECID sequence reached Z9999 limit during uniqueness check")
                            return None
                    else:
                        current_number += 1
                    new_secid = f"{current_prefix}{current_number:04d}"
    except oracledb.Error as ex:
        error, = ex.args
        logging.error(f"Error generating SECID: {error.message}")
        return None
    finally:
        if conn:
            await conn.close()


async def get_next_deptid():
    """Generate next DEPTID for departments (pattern: D0001, D0002, ..., D9999, E0001, etc.).
    Includes a uniqueness verification loop to prevent duplicate DEPTIDs."""
    conn = await get_async_connection()
    if not conn:
        return None
    
    try:
        async with conn.cursor() as cursor:
            # Get the last DEPTID
            await cursor.execute(
                "SELECT DEPTID FROM LKP_DEPT ORDER BY DEPTID DESC FETCH NEXT 1 ROW ONLY"
            )
            result = await cursor.fetchone()
            
            if result and result[0]:
                last_deptid = result[0]
                # Parse format like "D0001"
                if len(last_deptid) == 5 and last_deptid[0].isalpha() and last_deptid[1:].isdigit():
                    prefix = last_deptid[0].upper()
                    number = int(last_deptid[1:])
                    
                    if number >= 9999:
                        # Move to next letter
                        prefix = chr(ord(prefix) + 1)
                        number = 1
                        if prefix > 'Z':
                            logging.warning("DEPTID sequence reached Z9999 limit")
                            return None
                    else:
                        number += 1
                    
                    new_deptid = f"{prefix}{number:04d}"
                else:
                    new_deptid = "D0001"
            else:
                # Start with D0001 if no records exist
                new_deptid = "D0001"
            
            # Verify uniqueness - loop until we find an unused DEPTID
            while True:
                await cursor.execute(
                    "SELECT COUNT(*) FROM LKP_DEPT WHERE DEPTID = :deptid",
                    deptid=new_deptid
                )
                count_result = await cursor.fetchone()
                if count_result[0] == 0:
                    return new_deptid
                else:
                    # DEPTID already exists, increment and try again
                    current_prefix = new_deptid[0].upper()
                    current_number = int(new_deptid[1:])
                    
                    if current_number >= 9999:
                        current_number = 1
                        current_prefix = chr(ord(current_prefix) + 1)
                        if current_prefix > 'Z':
                            logging.warning("DEPTID sequence reached Z9999 limit during uniqueness check")
                            return None
                    else:
                        current_number += 1
                    new_deptid = f"{current_prefix}{current_number:04d}"
    except oracledb.Error as ex:
        error, = ex.args
        logging.error(f"Error generating DEPTID: {error.message}")
        return None
    finally:
        if conn:
            await conn.close()

# --- AGENCIES ---

async def get_agencies():
    """Get all active agencies from LKP_ORG_AGENCY."""
    conn = await get_async_connection()
    if not conn:
        return []

    agencies = []
    try:
        async with conn.cursor() as cursor:
            query = """
                SELECT SYSTEM_ID, NAME
                FROM LKP_ORG_AGENCY
                WHERE DISABLED = 'N'
                ORDER BY NAME
            """
            await cursor.execute(query)
            rows = await cursor.fetchall()
            
            for row in rows:
                agencies.append({
                    'SYSTEM_ID': row[0],
                    'NAME': row[1]
                })
    except oracledb.Error as ex:
        error, = ex.args
        logging.error(f"Error fetching agencies: {error.message}")
    finally:
        if conn:
            await conn.close()
    
    return agencies


# --- SECTIONS (Companies) ---

async def get_sections(name: str = "", disabled: str = None, page: int = 1, per_page: int = 10):
    """Get sections from LKP_SECTION with pagination and search. Only returns items from the last 2 days.
    Joins with LKP_DEPT on DEPTID = LKP_DEPT.SYSTEM_ID to ensure DEPTID references a valid department."""
    conn = await get_async_connection()
    if not conn:
        return {"sections": [], "total_records": 0, "total_pages": 0, "current_page": 1, "per_page": per_page}

    sections = []
    total = 0
    try:
        async with conn.cursor() as cursor:
            # Build search condition - DEPTID is a FK to LKP_DEPT.SYSTEM_ID, enforced via INNER JOIN
            search_condition = ""
            params = {}
            
            if name.strip():
                search_condition += " AND (UPPER(s.NAME) LIKE UPPER(:name) OR UPPER(s.SECID) LIKE UPPER(:name) OR UPPER(s.TRANSLATION) LIKE UPPER(:name))"
                params['name'] = f"%{name}%"
            else:
                search_condition += " AND s.LAST_UPDATE >= SYSDATE - 2"
            
            # Handle disabled filter - default is to show only enabled (DISABLED = 'N')
            if disabled == 'Y':
                search_condition += " AND s.DISABLED = 'Y'"
            else:
                # Default: only show enabled sections
                search_condition += " AND s.DISABLED = 'N'"
            
            # Count total - INNER JOIN ensures DEPTID references a valid LKP_DEPT.SYSTEM_ID
            count_query = f"""
                SELECT COUNT(*) 
                FROM LKP_SECTION s
                INNER JOIN LKP_DEPT d ON s.DEPTID = d.SYSTEM_ID
                WHERE 1=1 {search_condition}
            """
            await cursor.execute(count_query, params)
            count_result = await cursor.fetchone()
            total = count_result[0] if count_result else 0
            
            # Calculate offset
            offset = (page - 1) * per_page
            
            # Fetch paginated results - join with LKP_DEPT to validate DEPTID = SYSTEM_ID
            data_query = f"""
                SELECT * FROM (
                    SELECT 
                        s.SECID,
                        s.NAME,
                        s.TRANSLATION,
                        s.DISABLED,
                        s.LAST_UPDATE,
                        s.SYSTEM_ID,
                        s.DEPTID,
                        d.NAME AS DEPT_NAME,
                        ROW_NUMBER() OVER (ORDER BY s.SECID ASC) as rn
                    FROM LKP_SECTION s
                    INNER JOIN LKP_DEPT d ON s.DEPTID = d.SYSTEM_ID
                    WHERE 1=1 {search_condition}
                )
                WHERE rn > :offset AND rn <= :end_row
            """
            params['offset'] = offset
            params['end_row'] = offset + per_page
            
            await cursor.execute(data_query, params)
            rows = await cursor.fetchall()
            
            for row in rows:
                sections.append({
                    'SECID': row[0],
                    'NAME': row[1],
                    'TRANSLATION': row[2],
                    'DISABLED': row[3],
                    'LAST_UPDATE': row[4],
                    'SYSTEM_ID': row[5],
                    'DEPTID': row[6],
                    'DEPT_NAME': row[7]
                })
    except oracledb.Error as ex:
        error, = ex.args
        logging.error(f"Error fetching sections: {error.message}")
    finally:
        if conn:
            await conn.close()
    
    total_pages = (total + per_page - 1) // per_page
    return {
        "sections": sections,
        "total_records": total,
        "total_pages": total_pages,
        "current_page": page,
        "per_page": per_page
    }


async def is_section_name_duplicate(name: str) -> bool:
    """Check if a section name already exists in LKP_SECTION under the fixed department (DEPTID = SECTION_DEPTID)."""
    conn = await get_async_connection()
    if not conn:
        return False
    
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                "SELECT COUNT(*) FROM LKP_SECTION WHERE UPPER(NAME) = UPPER(:name) AND DEPTID = :deptid",
                name=name,
                deptid=SECTION_DEPTID
            )
            result = await cursor.fetchone()
            return result[0] > 0
    except oracledb.Error as ex:
        error, = ex.args
        logging.error(f"Error checking duplicate section name: {error.message}")
        return False
    finally:
        if conn:
            await conn.close()


async def add_section(name: str, translation: str):
    """Add a new section to LKP_SECTION. Checks for duplicate names first."""
    # Check for duplicate name before proceeding
    if await is_section_name_duplicate(name):
        return False, f'Company name "{name}" already exists. Please choose a different name.'
    
    conn = await get_async_connection()
    if not conn:
        return False, "Failed to connect to database"

    try:
        # Generate new SECID (alphanumeric like A0001)
        new_secid = await get_next_secid()
        if not new_secid:
            return False, "Failed to generate SECID"
        
        # Generate new SYSTEM_ID from sequence
        new_system_id = await get_next_system_id_sequence()
        if new_system_id is None:
            return False, "Failed to generate SYSTEM_ID"
        
        async with conn.cursor() as cursor:
            # Final existence check within the same connection to prevent race conditions
            await cursor.execute(
                "SELECT COUNT(*) FROM LKP_SECTION WHERE SECID = :secid",
                secid=new_secid
            )
            count_result = await cursor.fetchone()
            if count_result[0] > 0:
                return False, f'SECID "{new_secid}" already exists in LKP_SECTION. Please try again.'
            
            query = """
                INSERT INTO LKP_SECTION (SECID, SYSTEM_ID, NAME, TRANSLATION, DISABLED, DEPTID, LAST_UPDATE)
                VALUES (:secid, :system_id, :name, :translation, 'N', :deptid, SYSDATE)
            """
            await cursor.execute(
                query,
                secid=new_secid,
                system_id=new_system_id,
                name=name,
                translation=translation,
                deptid=SECTION_DEPTID
            )
            await conn.commit()
            return True, f"Section created with ID {new_secid}"
    except oracledb.Error as ex:
        error, = ex.args
        logging.error(f"Error adding section: {error.message}")
        return False, str(error.message)
    finally:
        if conn:
            await conn.close()


async def update_section(system_id: str, name: str, translation: str, disabled: str):
    """Update an existing section in LKP_SECTION."""
    conn = await get_async_connection()
    if not conn:
        return False, "Failed to connect to database"

    try:
        async with conn.cursor() as cursor:
            query = """
                UPDATE LKP_SECTION
                SET NAME = :name, TRANSLATION = :translation, DISABLED = :disabled, LAST_UPDATE = SYSDATE
                WHERE SYSTEM_ID = :system_id
            """
            await cursor.execute(query, system_id=system_id, name=name, translation=translation, disabled=disabled)
            await conn.commit()
            return True, "Section updated successfully"
    except oracledb.Error as ex:
        error, = ex.args
        logging.error(f"Error updating section: {error.message}")
        return False, str(error.message)
    finally:
        if conn:
            await conn.close()


# --- DEPARTMENTS ---

async def get_departments(name: str = "", agency_id: int = None, page: int = 1, per_page: int = 10, fetch_all: bool = False):
    """Get departments from LKP_DEPT with pagination and search. Joins with LKP_ORG_AGENCY to get agency name."""
    conn = await get_async_connection()
    if not conn:
        return {"departments": [], "total_records": 0, "total_pages": 0, "current_page": 1, "per_page": per_page}

    departments = []
    total = 0
    try:
        async with conn.cursor() as cursor:
            # Build search condition
            search_condition = ""
            params = {}
            
            if name.strip():
                search_condition += " AND (UPPER(d.NAME) LIKE UPPER(:name) OR UPPER(d.SHORT) LIKE UPPER(:name) OR UPPER(d.TRANSLATION) LIKE UPPER(:name) OR UPPER(d.DEPTID) LIKE UPPER(:name))"
                params['name'] = f"%{name.strip()}%"
            elif not fetch_all:
                search_condition += " AND d.LAST_UPDATE >= SYSDATE - 2"
            
            if agency_id is not None:
                search_condition += " AND d.AGENCYID = :agency_id"
                params['agency_id'] = agency_id
            
            # Count total
            count_query = f"""
                SELECT COUNT(*) 
                FROM LKP_DEPT d
                WHERE d.DISABLED = 'N' {search_condition}
            """
            await cursor.execute(count_query, params)
            count_result = await cursor.fetchone()
            total = count_result[0] if count_result else 0
            
            # Calculate offset
            offset = (page - 1) * per_page
            
            # Fetch paginated results with LEFT JOIN to LKP_ORG_AGENCY
            data_query = f"""
                SELECT * FROM (
                    SELECT 
                        d.DEPTID,
                        d.NAME,
                        d.TRANSLATION,
                        d.SHORT,
                        d.DISABLED,
                        d.LAST_UPDATE,
                        d.AGENCYID,
                        d.SYSTEM_ID,
                        a.NAME AS AGENCY_NAME,
                        ROW_NUMBER() OVER (ORDER BY d.NAME) as rn
                    FROM LKP_DEPT d
                    LEFT JOIN LKP_ORG_AGENCY a ON d.AGENCYID = a.SYSTEM_ID
                    WHERE d.DISABLED = 'N' {search_condition}
                )
                WHERE rn > :offset AND rn <= :end_row
            """
            params['offset'] = offset
            params['end_row'] = offset + per_page
            
            await cursor.execute(data_query, params)
            rows = await cursor.fetchall()
            
            for row in rows:
                departments.append({
                    'DEPTID': row[0],
                    'NAME': row[1],
                    'TRANSLATION': row[2],
                    'SHORT': row[3],
                    'DISABLED': row[4],
                    'LAST_UPDATE': row[5],
                    'AGENCYID': row[6],
                    'SYSTEM_ID': row[7],
                    'AGENCY_NAME': row[8] or ""
                })
    except oracledb.Error as ex:
        error, = ex.args
        logging.error(f"Error fetching departments: {error.message}")
    finally:
        if conn:
            await conn.close()
    
    total_pages = (total + per_page - 1) // per_page
    return {
        "departments": departments,
        "total_records": total,
        "total_pages": total_pages,
        "current_page": page,
        "per_page": per_page
    }


async def is_department_name_duplicate(name: str) -> bool:
    """Check if a department name already exists in LKP_DEPT."""
    conn = await get_async_connection()
    if not conn:
        return False
    
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                "SELECT COUNT(*) FROM LKP_DEPT WHERE UPPER(NAME) = UPPER(:name)",
                name=name
            )
            result = await cursor.fetchone()
            return result[0] > 0
    except oracledb.Error as ex:
        error, = ex.args
        logging.error(f"Error checking duplicate department name: {error.message}")
        return False
    finally:
        if conn:
            await conn.close()


async def is_department_short_duplicate(short: str) -> bool:
    """Check if a department SHORT (EMS Department ID) already exists in LKP_DEPT."""
    conn = await get_async_connection()
    if not conn:
        return False
    
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                "SELECT COUNT(*) FROM LKP_DEPT WHERE UPPER(SHORT) = UPPER(:short)",
                short=short
            )
            result = await cursor.fetchone()
            return result[0] > 0
    except oracledb.Error as ex:
        error, = ex.args
        logging.error(f"Error checking duplicate department SHORT: {error.message}")
        return False
    finally:
        if conn:
            await conn.close()


async def is_department_deptid_duplicate(deptid: str) -> bool:
    """Check if a DEPTID already exists in LKP_DEPT."""
    conn = await get_async_connection()
    if not conn:
        return False
    
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                "SELECT COUNT(*) FROM LKP_DEPT WHERE UPPER(DEPTID) = UPPER(:deptid)",
                deptid=deptid
            )
            result = await cursor.fetchone()
            return result[0] > 0
    except oracledb.Error as ex:
        error, = ex.args
        logging.error(f"Error checking duplicate department DEPTID: {error.message}")
        return False
    finally:
        if conn:
            await conn.close()


async def add_department(name: str, translation: str, short: str, agency_system_id: int, dept_id: str = None):
    """
    Add a new department to LKP_DEPT, along with:
    1. A security group in "GROUPS" named EMS_DPT_{SHORT}
    2. A default child section in LKP_SECTION named "N/A" (referencing department SYSTEM_ID)
    
    All operations are committed in a single atomic database transaction.
    """
    name = (name or "").strip()
    translation = (translation or "").strip()
    short = (short or "").strip().upper()
    dept_id = (dept_id or "").strip().upper() if dept_id else None

    if not name:
        return False, "Department name is required"
    if not translation:
        return False, "Arabic name / translation is required"
    if not short:
        return False, "EMS Department ID is required"
    if len(short) > 5:
        return False, "EMS Department ID cannot exceed 5 characters"
    if dept_id and len(dept_id) > 5:
        return False, "Department ID cannot exceed 5 characters"
    if not agency_system_id:
        return False, "Agency is required"

    # Check for duplicate name
    if await is_department_name_duplicate(name):
        return False, f'Department name "{name}" already exists. Please choose a different name.'
    
    # Check for duplicate SHORT (EMS Department ID)
    if await is_department_short_duplicate(short):
        return False, f'EMS Department ID "{short}" already exists. Please choose a different ID.'
    
    # Check if custom dept_id is supplied and duplicate
    if dept_id and await is_department_deptid_duplicate(dept_id):
        return False, f'Department ID "{dept_id}" already exists. Please choose a different ID.'

    conn = await get_async_connection()
    if not conn:
        return False, "Failed to connect to database"

    try:
        # Generate new DEPTID if not provided
        if not dept_id:
            dept_id = await get_next_deptid()
            if not dept_id:
                return False, "Failed to generate DEPTID"

        # Generate new SYSTEM_ID from sequence for department
        dept_system_id = await get_next_system_id_sequence()
        if dept_system_id is None:
            return False, "Failed to generate department SYSTEM_ID"

        # Generate new SYSTEM_ID from sequence for security group
        group_system_id = await get_next_system_id_sequence()
        if group_system_id is None:
            return False, "Failed to generate group SYSTEM_ID"

        # Generate new SYSTEM_ID from sequence for child N/A section
        sec_system_id = await get_next_system_id_sequence()
        if sec_system_id is None:
            return False, "Failed to generate section SYSTEM_ID"

        group_id_name = f"EMS_DPT_{short}"

        async with conn.cursor() as cursor:
            # 1. Final existence check for DEPTID in LKP_DEPT within same connection
            await cursor.execute(
                "SELECT COUNT(*) FROM LKP_DEPT WHERE UPPER(DEPTID) = UPPER(:deptid)",
                deptid=dept_id
            )
            count_result = await cursor.fetchone()
            if count_result and count_result[0] > 0:
                return False, f'DEPTID "{dept_id}" already exists in LKP_DEPT. Please try again.'
            
            # 2. Insert into LKP_DEPT (AGENCYID = agency_system_id, SYSTEM_ID = dept_system_id)
            dept_insert_query = """
                INSERT INTO LKP_DEPT (
                    DEPTID, NAME, TRANSLATION, DISABLED, AGENCYID, SYSTEM_ID, SHORT, LAST_UPDATE
                ) VALUES (
                    :deptid, :name, :translation, 'N', :agency_system_id, :dept_system_id, :short, SYSDATE
                )
            """
            await cursor.execute(
                dept_insert_query,
                deptid=dept_id,
                name=name,
                translation=translation,
                agency_system_id=agency_system_id,
                dept_system_id=dept_system_id,
                short=short
            )

            # 3. Insert Security Group into "GROUPS"
            groups_insert_query = """
                INSERT INTO "GROUPS" (
                    FORCE_CHECKIN, MBL_EDITCOPY, MBL_OVERWRITE, AUTOCLEAN, SHADOW_DOCS, SHADOW_PROFILES, 
                    SHADOW_SEC_DOCS, DISABLE_NATIVE, MAKE_READ_ONLY, REMOVE_READ_ONLY, MAKE_VER_READONLY, 
                    MAKE_VER_WRITABLE, PUBLISH_VERSION, UNPUBLISH_VERSION, DATE_FORMAT, TIME_FORMAT, 
                    DEFAULT_VIEWER, MANAGE_PRF, MANAGE_CYD, ALLOW_APPINT, ALLOW_USRSETTINGS, ALLOW_DOC_CREATE, 
                    CREATE_FOLDER, ROOT_FOLDER, CREATE_RELATION, SHOW_RELATED, REMOVE_RELATION, ALLOW_NOTIF, 
                    ALLOW_PREVIEW, WARN_UPDATE_AVAIL, ENABLE_WORKSPACE, GROUP_ID, NETWORK_ID, GROUP_NAME, 
                    DISABLED, ALLOW_LOGIN, DELETE_VERSIONS, EDIT_PREVIOUS_VER, NEW_VERSIONS, SAVE_TO_REM_LIB, 
                    PRECONNECT_LIBS, NV_AUTHOR_EDIT, NV_ENTERED_BY, NV_BILLABLE, DISPLAY_VER_LIST, 
                    MV_DOCS_IF_CHNG, CHECKOUT, OTHER_CHECKIN, CHECKIN_REMINDER, RESET_STATUS, COPY_IN_USE, 
                    TEMPLATE_MANAGER, MASS_UPD_PROFILES, AUTO_LOGIN, VIEW_UNSECURED, PROMPT_PAGES, 
                    NONBILL_PAGES, GET_EDIT_INFO, VISIT_AUTHOR_EDIT, VISIT_ENTERED_BY, LCP_RUN, EDIT_VTS, 
                    EDIT_LIBPARMS, EDIT_WS_PARAMS, EDIT_USER_DEFAULTS, MANAGE_GROUPS, DI_RUN, MBLINST_RUN, 
                    DI_MANAGE, CR_RUN, SM_RUN, DD_RUN, DB_EDIT, DBI_RUN, INDEXER_RUN, INTERCHANGE_RUN, 
                    PROFSEC, ALLOW_DOC_DELETE, ALLOW_CONTENT_DEL, ALLOW_QUEUE_DEL, ACL_DEFAULTS, 
                    REMOVE_MON_LIST, DISPLAY_MON_LIST, WARN_SECURE, ONLY_READONLY, MIN_DISKFREE, 
                    DEF_SHAD_RETENTION, DEFAULT_FT_INDEX, ITEM_MAX, PAGE_MAX, DPACKAGE, PROFILE_FORM, 
                    SYSTEM_ID, UNIV_ACCESS, MAX_VERSIONS, MAX_SUBVERSIONS, DEFAULT_PAGES, HITLIST_FORM, 
                    LAST_UPDATE, PROFILE_DEFAULTS, DECLARE_RECORD, UNDECLARE_RECORD, SU_CHNGE_PROF_EDIT, 
                    SU_CHNGE_PROF_SAVE, SU_SHOW_SEC_PANEL, SU_SHOW_LEFT_PANE, SU_USE_MODERN, SU_CHANGE_STYLE, 
                    SU_RECENT_COUNT, SU_USE_LAST_LOC, SU_BROWSE_TO_LOC, MANAGE_LOOKUP_SEC
                ) VALUES (
                    'D','D','D','D','D','D',
                    'D','D','D','D','D',
                    'D','D','D','D/M/Y','H:M',
                    'FULVIEW','D','D','D','D','D',
                    'D','D','D','D','D','D',
                    'D','D','D',:group_id,NULL,:group_name,
                    'N','D','D','D','D','D',
                    'D','D','D','D','D',
                    'D','D','D','D','D','D',
                    'D','D','D','D','D',
                    NULL,'D','D','D','D','D',
                    NULL,NULL,'D','D','D','D',
                    'D','D','D','D','D','D','D','D',
                    'D',NULL,'D','D','D',
                    'D','D','D','D',5,
                    30,0,25,20,NULL,0,
                    :group_system_id,0,99,0,0,0,
                    SYSDATE,NULL,'D','D','D',
                    'D','D','D','D','D',
                    10,'D','D',NULL
                )
            """
            await cursor.execute(
                groups_insert_query,
                group_id=group_id_name,
                group_name=group_id_name,
                group_system_id=group_system_id
            )

            # 4. Determine SECID for child "N/A" section
            # Check if dept_id is available as SECID in LKP_SECTION
            await cursor.execute(
                "SELECT COUNT(*) FROM LKP_SECTION WHERE UPPER(SECID) = UPPER(:secid)",
                secid=dept_id
            )
            sec_res = await cursor.fetchone()
            if sec_res and sec_res[0] == 0:
                na_secid = dept_id
            else:
                na_secid = await get_next_secid()
                if not na_secid:
                    na_secid = dept_id

            # 5. Insert child "N/A" section into LKP_SECTION (DEPTID = dept_system_id)
            section_insert_query = """
                INSERT INTO LKP_SECTION (
                    SECID, SYSTEM_ID, DISABLED, NAME, TRANSLATION, DEPTID, LAST_UPDATE
                ) VALUES (
                    :secid, :system_id, 'N', 'N/A', 'N/A', :parent_dept_system_id, SYSDATE
                )
            """
            await cursor.execute(
                section_insert_query,
                secid=na_secid,
                system_id=sec_system_id,
                parent_dept_system_id=dept_system_id
            )

            # Commit the atomic transaction
            await conn.commit()
            return True, f"Department created with ID {dept_id}"

    except oracledb.Error as ex:
        if conn:
            await conn.rollback()
        error, = ex.args
        logging.error(f"Error adding department: {error.message}")
        return False, str(error.message)
    except Exception as e:
        if conn:
            await conn.rollback()
        logging.error(f"Unexpected error adding department: {str(e)}")
        return False, str(e)
    finally:
        if conn:
            await conn.close()


async def update_department(system_id: int, name: str, translation: str):
    """Update an existing department in LKP_DEPT by SYSTEM_ID. Only name and translation can be updated."""
    name = (name or "").strip()
    translation = (translation or "").strip()
    if not name:
        return False, "Department name is required"
    if not translation:
        return False, "Arabic name / translation is required"

    conn = await get_async_connection()
    if not conn:
        return False, "Failed to connect to database"

    try:
        async with conn.cursor() as cursor:
            # Check if new name already exists on another department
            await cursor.execute(
                "SELECT COUNT(*) FROM LKP_DEPT WHERE UPPER(NAME) = UPPER(:name) AND SYSTEM_ID != :system_id",
                name=name,
                system_id=system_id
            )
            dup_result = await cursor.fetchone()
            if dup_result and dup_result[0] > 0:
                return False, f'Department name "{name}" is already used by another department.'

            query = """
                UPDATE LKP_DEPT
                SET NAME = :name, TRANSLATION = :translation, LAST_UPDATE = SYSDATE
                WHERE SYSTEM_ID = :system_id
            """
            await cursor.execute(query, system_id=system_id, name=name, translation=translation)
            await conn.commit()
            return True, "Department updated successfully"
    except oracledb.Error as ex:
        error, = ex.args
        logging.error(f"Error updating department: {error.message}")
        return False, str(error.message)
    finally:
        if conn:
            await conn.close()


# --- EMS SECTIONS (Hierarchical Sections under Departments) ---

async def get_departments_by_agency(agency_system_id: int):
    """Get departments filtered by agency."""
    conn = await get_async_connection()
    if not conn:
        return []

    departments = []
    try:
        async with conn.cursor() as cursor:
            query = """
                SELECT DEPTID, SYSTEM_ID, AGENCYID, NAME, SHORT
                FROM LKP_DEPT
                WHERE AGENCYID = :agency_system_id AND DISABLED = 'N'
                ORDER BY NAME
            """
            await cursor.execute(query, agency_system_id=agency_system_id)
            rows = await cursor.fetchall()
            
            for row in rows:
                departments.append({
                    'DEPTID': row[0],     # e.g. "D0001" (string code)
                    'SYSTEM_ID': row[1],  # integer PK — this is what LKP_SECTION.DEPTID stores
                    'AGENCYID': row[2],
                    'NAME': row[3],
                    'SHORT': row[4]
                })
    except oracledb.Error as ex:
        error, = ex.args
        logging.error(f"Error fetching departments by agency: {error.message}")
    finally:
        if conn:
            await conn.close()
    
    return departments


async def get_ems_sections(dept_system_id: int = None, name: str = "", page: int = 1, per_page: int = 10):
    """Get EMS sections (hierarchical) with pagination and search. Only returns items from the last 2 days."""
    conn = await get_async_connection()
    if not conn:
        return {"sections": [], "total_records": 0, "total_pages": 0, "current_page": 1, "per_page": per_page}

    sections = []
    total = 0
    try:
        async with conn.cursor() as cursor:
            # Build search condition
            search_condition = ""
            params = {}
            
            if dept_system_id:
                search_condition += " AND DEPTID = :dept_system_id"
                params['dept_system_id'] = dept_system_id
            
            if name.strip():
                search_condition += " AND (UPPER(NAME) LIKE UPPER(:name) OR UPPER(SECID) LIKE UPPER(:name) OR UPPER(TRANSLATION) LIKE UPPER(:name))"
                params['name'] = f"%{name}%"
            else:
                search_condition += " AND LAST_UPDATE >= SYSDATE - 2"
            
            # Count total
            count_query = f"SELECT COUNT(*) FROM LKP_SECTION WHERE 1=1 {search_condition}"
            await cursor.execute(count_query, params)
            count_result = await cursor.fetchone()
            total = count_result[0] if count_result else 0
            
            # Calculate offset
            offset = (page - 1) * per_page
            
            # Fetch paginated results
            data_query = f"""
                SELECT * FROM (
                    SELECT 
                        SECID,
                        NAME,
                        TRANSLATION,
                        DISABLED,
                        LAST_UPDATE,
                        SYSTEM_ID,
                        DEPTID,
                        ROW_NUMBER() OVER (ORDER BY SECID ASC) as rn
                    FROM LKP_SECTION
                    WHERE 1=1 {search_condition}
                )
                WHERE rn > :offset AND rn <= :end_row
            """
            params['offset'] = offset
            params['end_row'] = offset + per_page
            
            await cursor.execute(data_query, params)
            rows = await cursor.fetchall()
            
            for row in rows:
                sections.append({
                    'SECID': row[0],
                    'NAME': row[1],
                    'TRANSLATION': row[2],
                    'DISABLED': row[3],
                    'LAST_UPDATE': row[4],
                    'SYSTEM_ID': row[5],
                    'PARENT_DEPT_SYSTEM_ID': row[6]
                })
    except oracledb.Error as ex:
        error, = ex.args
        logging.error(f"Error fetching EMS sections: {error.message}")
    finally:
        if conn:
            await conn.close()
    
    total_pages = (total + per_page - 1) // per_page
    return {
        "sections": sections,
        "total_records": total,
        "total_pages": total_pages,
        "current_page": page,
        "per_page": per_page
    }


async def is_ems_section_duplicate_under_department(name: str, ems_sec_id: int) -> bool:
    """Check if an EMS section name already exists under the given department."""
    conn = await get_async_connection()
    if not conn:
        return False
    
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                "SELECT COUNT(*) FROM LKP_SECTION WHERE SECID = :ems_sec_id AND UPPER(NAME) = UPPER(:name)",
                ems_sec_id=ems_sec_id,
                name=name
            )
            result = await cursor.fetchone()
            return result[0] > 0
    except oracledb.Error as ex:
        error, = ex.args
        logging.error(f"Error checking duplicate EMS section: {error.message}")
        return False
    finally:
        if conn:
            await conn.close()


async def add_ems_section(name: str, translation: str, ems_code: str, dept_system_id: int):
    """Add a new EMS section within a department. Checks for duplicate name within the department first."""
    # Check for duplicate name within the same department
    if await is_ems_section_duplicate_under_department(name, ems_code):
        return False, f'Section name "{name}" already exists under this department. Please choose a different name.'
    
    conn = await get_async_connection()
    if not conn:
        return False, "Failed to connect to database"

    try:
        # Generate new SECID (numeric string)
        # new_secid = await get_next_secid()
        # if not new_secid:
        #     return False, "Failed to generate SECID"
        
        # Generate new SYSTEM_ID from sequence
        new_system_id = await get_next_system_id_sequence()
        if new_system_id is None:
            return False, "Failed to generate SYSTEM_ID"
        
        async with conn.cursor() as cursor:
            # Final existence check within the same connection to prevent race conditions
            await cursor.execute(
                "SELECT COUNT(*) FROM LKP_SECTION WHERE SECID = :secid",
                secid=ems_code   
            )
            count_result = await cursor.fetchone()
            if count_result[0] > 0:
                return False, f'SECID "{ems_code}" already exists in LKP_SECTION. Please try again.'
            
            query = """
                INSERT INTO LKP_SECTION (SECID, SYSTEM_ID, NAME, TRANSLATION, DISABLED, DEPTID, LAST_UPDATE)
                VALUES (:secid, :system_id, :name, :translation, 'N', :parent_dept_system_id, SYSDATE)
            """
            await cursor.execute(
                query,
                secid=ems_code,
                system_id=new_system_id,
                name=name,
                translation=translation,
                parent_dept_system_id=dept_system_id
            )
            await conn.commit()
            return True, f"EMS Section created with ID {ems_code}"
    except oracledb.Error as ex:
        error, = ex.args
        logging.error(f"Error adding EMS section: {error.message}")
        return False, str(error.message)
    finally:
        if conn:
            await conn.close()


async def update_ems_section(system_id: int, name: str, translation: str, disabled: str, parent_dept_system_id: int, new_ems_code: str = None):
    """Update an existing EMS section by SYSTEM_ID. Optionally updates SECID if new_ems_code is provided."""
    conn = await get_async_connection()
    if not conn:
        return False, "Failed to connect to database"

    try:
        async with conn.cursor() as cursor:
            # If a new EMS code is supplied, validate it first
            if new_ems_code:
                # Fetch the current SECID so we only check for duplicates when the code actually changed
                await cursor.execute(
                    "SELECT SECID FROM LKP_SECTION WHERE SYSTEM_ID = :system_id",
                    system_id=system_id
                )
                current_row = await cursor.fetchone()
                current_secid = current_row[0] if current_row else None

                if new_ems_code != current_secid:
                    # Check that the new code is not already taken by another record
                    await cursor.execute(
                        "SELECT COUNT(*) FROM LKP_SECTION WHERE SECID = :secid",
                        secid=new_ems_code
                    )
                    dup_result = await cursor.fetchone()
                    if dup_result[0] > 0:
                        return False, f'EMS Code "{new_ems_code}" already exists. Please choose a different code.'

                    query = """
                        UPDATE LKP_SECTION
                        SET SECID = :new_secid, NAME = :name, TRANSLATION = :translation, DISABLED = :disabled, LAST_UPDATE = SYSDATE, DEPTID = :parent_dept_system_id
                        WHERE SYSTEM_ID = :system_id
                    """
                    await cursor.execute(query, system_id=system_id, new_secid=new_ems_code, name=name, translation=translation, disabled=disabled, parent_dept_system_id=parent_dept_system_id)
                else:
                    # Code unchanged — skip SECID update
                    query = """
                        UPDATE LKP_SECTION
                        SET NAME = :name, TRANSLATION = :translation, DISABLED = :disabled, LAST_UPDATE = SYSDATE, DEPTID = :parent_dept_system_id
                        WHERE SYSTEM_ID = :system_id
                    """
                    await cursor.execute(query, system_id=system_id, name=name, translation=translation, disabled=disabled, parent_dept_system_id=parent_dept_system_id)
            else:
                query = """
                    UPDATE LKP_SECTION
                    SET NAME = :name, TRANSLATION = :translation, DISABLED = :disabled, LAST_UPDATE = SYSDATE, DEPTID = :parent_dept_system_id
                    WHERE SYSTEM_ID = :system_id
                """
                await cursor.execute(query, system_id=system_id, name=name, translation=translation, disabled=disabled, parent_dept_system_id=parent_dept_system_id)

            await conn.commit()
            return True, "EMS Section updated successfully"
    except oracledb.Error as ex:
        error, = ex.args
        logging.error(f"Error updating EMS section: {error.message}")
        return False, str(error.message)
    finally:
        if conn:
            await conn.close()

