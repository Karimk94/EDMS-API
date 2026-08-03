import oracledb
import logging
from datetime import datetime
from database.connection import get_async_connection

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


async def is_section_name_duplicate(name: str, dept_system_id: int) -> bool:
    """Check if a section name already exists in LKP_SECTION under the given department (DEPTID = LKP_DEPT.SYSTEM_ID)."""
    conn = await get_async_connection()
    if not conn:
        return False
    
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                "SELECT COUNT(*) FROM LKP_SECTION WHERE UPPER(NAME) = UPPER(:name) AND DEPTID = :dept_system_id",
                name=name,
                dept_system_id=dept_system_id
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


async def add_section(name: str, translation: str, dept_system_id: int):
    """Add a new section to LKP_SECTION under the given department. Checks for duplicate names first.
    DEPTID stores LKP_DEPT.SYSTEM_ID (the parent department's primary key)."""
    # Check for duplicate name before proceeding
    if await is_section_name_duplicate(name, dept_system_id):
        return False, f'Section name "{name}" already exists under this department. Please choose a different name.'
    
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
                VALUES (:secid, :system_id, :name, :translation, 'N', :dept_system_id, SYSDATE)
            """
            await cursor.execute(
                query,
                secid=new_secid,
                system_id=new_system_id,
                name=name,
                translation=translation,
                dept_system_id=dept_system_id
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

async def get_departments(name: str = "", agency_id: int = None, page: int = 1, per_page: int = 10):
    """Get departments from LKP_DEPT with pagination and search. Only returns items from the last 2 days."""
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
                search_condition += " AND (UPPER(NAME) LIKE UPPER(:name) OR UPPER(SHORT) LIKE UPPER(:name) OR UPPER(TRANSLATION) LIKE UPPER(:name))"
                params['name'] = f"%{name}%"
            else:
                search_condition += " AND LAST_UPDATE >= SYSDATE - 2"
            
            if agency_id is not None:
                search_condition += " AND AGENCYID = :agency_id"
                params['agency_id'] = agency_id
            
            # Count total
            count_query = f"SELECT COUNT(*) FROM LKP_DEPT WHERE DISABLED = 'N' {search_condition}"
            await cursor.execute(count_query, params)
            count_result = await cursor.fetchone()
            total = count_result[0] if count_result else 0
            
            # Calculate offset
            offset = (page - 1) * per_page
            
            # Fetch paginated results
            data_query = f"""
                SELECT * FROM (
                    SELECT 
                        DEPTID,
                        NAME,
                        SHORT,
                        DISABLED,
                        LAST_UPDATE,
                        AGENCYID,
                        SYSTEM_ID,
                        ROW_NUMBER() OVER (ORDER BY NAME) as rn
                    FROM LKP_DEPT
                    WHERE DISABLED = 'N' {search_condition}
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
                    'SHORT': row[2],
                    'DISABLED': row[3],
                    'LAST_UPDATE': row[4],
                    'AGENCYID': row[5],
                    'SYSTEM_ID': row[6]
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


async def add_department(name: str, translation: str, short: str, agency_system_id: int):
    """Add a new department to LKP_DEPT. Checks for duplicate name and SHORT first."""
    # Check for duplicate name
    if await is_department_name_duplicate(name):
        return False, f'Department name "{name}" already exists. Please choose a different name.'
    
    # Check for duplicate SHORT (EMS Department ID)
    if short and await is_department_short_duplicate(short):
        return False, f'EMS Department ID "{short}" already exists. Please choose a different ID.'
    
    conn = await get_async_connection()
    if not conn:
        return False, "Failed to connect to database"

    try:
        # Generate new DEPTID (alphanumeric like D0001)
        new_deptid = await get_next_deptid()
        if not new_deptid:
            return False, "Failed to generate DEPTID"
        
        # Note: AGENCYID is used as SYSTEM_ID in LKP_DEPT (as per legacy code)
        # The SHORT column has a max of 5 characters (validated in frontend)
        
        async with conn.cursor() as cursor:
            # Final existence check within the same connection to prevent race conditions
            await cursor.execute(
                "SELECT COUNT(*) FROM LKP_DEPT WHERE DEPTID = :deptid",
                deptid=new_deptid
            )
            count_result = await cursor.fetchone()
            if count_result[0] > 0:
                return False, f'DEPTID "{new_deptid}" already exists in LKP_DEPT. Please try again.'
            
            query = """
                INSERT INTO LKP_DEPT (DEPTID, NAME, TRANSLATION, SHORT, DISABLED, AGENCYID, LAST_UPDATE)
                VALUES (:deptid, :name, :translation, :short, 'N', :agency_system_id, SYSDATE)
            """
            await cursor.execute(
                query,
                deptid=new_deptid,
                name=name,
                translation=translation,
                short=short,
                agency_system_id=agency_system_id
            )
            await conn.commit()
            return True, f"Department created with ID {new_deptid}"
    except oracledb.Error as ex:
        error, = ex.args
        logging.error(f"Error adding department: {error.message}")
        return False, str(error.message)
    finally:
        if conn:
            await conn.close()


async def update_department(system_id: int, name: str, translation: str):
    """Update an existing department in LKP_DEPT by SYSTEM_ID."""
    conn = await get_async_connection()
    if not conn:
        return False, "Failed to connect to database"

    try:
        async with conn.cursor() as cursor:
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

