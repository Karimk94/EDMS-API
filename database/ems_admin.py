import oracledb
import logging
from datetime import datetime
from database.connection import get_async_connection

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
    """Get sections (companies) from LKP_SECTION with pagination and search. Only returns items from the last 2 days."""
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
            
            if name.strip():
                search_condition += " AND (UPPER(NAME) LIKE UPPER(:name) OR UPPER(SECID) LIKE UPPER(:name) OR UPPER(TRANSLATION) LIKE UPPER(:name))"
                params['name'] = f"%{name}%"
            else:
                search_condition += " AND LAST_UPDATE >= SYSDATE - 2"
            
            if disabled == 'Y':
                search_condition += " AND DISABLED = 'Y'"
            elif disabled != 'Y':
                search_condition += " AND DISABLED = 'N'"
            
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
                        ROW_NUMBER() OVER (ORDER BY NAME) as rn
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
                    'SYSTEM_ID': row[5]
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


async def add_section(name: str, translation: str):
    """Add a new section to LKP_SECTION."""
    conn = await get_async_connection()
    if not conn:
        return False, "Failed to connect to database"

    try:
        async with conn.cursor() as cursor:
            # Get new SECID
            await cursor.execute("SELECT MAX(TO_NUMBER(SECID)) FROM LKP_SECTION WHERE SECID LIKE '%'")
            max_id = await cursor.fetchone()
            new_id = str(int(max_id[0] or 0) + 1) if max_id[0] else "1"
            
            query = """
                INSERT INTO LKP_SECTION (SECID, NAME, TRANSLATION, DISABLED, LAST_UPDATE)
                VALUES (:secid, :name, :translation, 'N', SYSDATE)
            """
            await cursor.execute(query, secid=new_id, name=name, translation=translation)
            await conn.commit()
            return True, f"Section created with ID {new_id}"
    except oracledb.Error as ex:
        error, = ex.args
        logging.error(f"Error adding section: {error.message}")
        return False, str(error.message)
    finally:
        if conn:
            await conn.close()


async def update_section(secid: str, name: str, translation: str, disabled: str):
    """Update an existing section in LKP_SECTION."""
    conn = await get_async_connection()
    if not conn:
        return False, "Failed to connect to database"

    try:
        async with conn.cursor() as cursor:
            query = """
                UPDATE LKP_SECTION
                SET NAME = :name, TRANSLATION = :translation, DISABLED = :disabled, LAST_UPDATE = SYSDATE
                WHERE SECID = :secid
            """
            await cursor.execute(query, secid=secid, name=name, translation=translation, disabled=disabled)
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

async def get_departments(name: str = "", page: int = 1, per_page: int = 10):
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
                    'SYSTEM_ID': row[5] # Keep dictionary key as SYSTEM_ID for frontend compatibility, but it holds AGENCYID
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


async def add_department(name: str, translation: str, short: str, agency_system_id: int):
    """Add a new department to LKP_DEPT."""
    conn = await get_async_connection()
    if not conn:
        return False, "Failed to connect to database"

    try:
        async with conn.cursor() as cursor:
            # Get new DEPTID
            await cursor.execute("SELECT MAX(TO_NUMBER(DEPTID)) FROM LKP_DEPT WHERE DEPTID LIKE '%'")
            max_id = await cursor.fetchone()
            new_id = str(int(max_id[0] or 0) + 1) if max_id[0] else "1"
            
            query = """
                INSERT INTO LKP_DEPT (DEPTID, NAME, TRANSLATION, SHORT, DISABLED, LAST_UPDATE, AGENCYID)
                VALUES (:deptid, :name, :translation, :short, 'N', SYSDATE, :agency_system_id)
            """
            await cursor.execute(query, deptid=new_id, name=name, translation=translation, short=short, agency_system_id=agency_system_id)
            await conn.commit()
            return True, f"Department created with ID {new_id}"
    except oracledb.Error as ex:
        error, = ex.args
        logging.error(f"Error adding department: {error.message}")
        return False, str(error.message)
    finally:
        if conn:
            await conn.close()


async def update_department(deptid: str, name: str, translation: str):
    """Update an existing department in LKP_DEPT."""
    conn = await get_async_connection()
    if not conn:
        return False, "Failed to connect to database"

    try:
        async with conn.cursor() as cursor:
            query = """
                UPDATE LKP_DEPT
                SET NAME = :name, TRANSLATION = :translation, LAST_UPDATE = SYSDATE
                WHERE DEPTID = :deptid
            """
            await cursor.execute(query, deptid=deptid, name=name, translation=translation)
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
                SELECT DEPTID, AGENCYID, NAME, SHORT
                FROM LKP_DEPT
                WHERE AGENCYID = :agency_system_id AND DISABLED = 'N'
                ORDER BY NAME
            """
            await cursor.execute(query, agency_system_id=agency_system_id)
            rows = await cursor.fetchall()
            
            for row in rows:
                departments.append({
                    'DEPTID': row[0],
                    'SYSTEM_ID': row[1],
                    'NAME': row[2],
                    'SHORT': row[3]
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
                        ROW_NUMBER() OVER (ORDER BY NAME) as rn
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


async def add_ems_section(ems_code: str, name: str, translation: str, dept_system_id: int):
    """Add a new EMS section within a department."""
    conn = await get_async_connection()
    if not conn:
        return False, "Failed to connect to database"

    try:
        async with conn.cursor() as cursor:
            query = """
                INSERT INTO LKP_SECTION (SECID, NAME, TRANSLATION, DISABLED, LAST_UPDATE, DEPTID)
                VALUES (:secid, :name, :translation, 'N', SYSDATE, :parent_dept_system_id)
            """
            await cursor.execute(query, secid=ems_code, name=name, translation=translation, parent_dept_system_id=dept_system_id)
            await conn.commit()
            return True, f"EMS Section created with ID {ems_code}"
    except oracledb.Error as ex:
        error, = ex.args
        logging.error(f"Error adding EMS section: {error.message}")
        return False, str(error.message)
    finally:
        if conn:
            await conn.close()


async def update_ems_section(secid: str, name: str, translation: str, disabled: str, parent_dept_system_id: int):
    """Update an existing EMS section."""
    conn = await get_async_connection()
    if not conn:
        return False, "Failed to connect to database"

    try:
        async with conn.cursor() as cursor:
            query = """
                UPDATE LKP_SECTION
                SET NAME = :name, TRANSLATION = :translation, DISABLED = :disabled, LAST_UPDATE = SYSDATE, DEPTID = :parent_dept_system_id
                WHERE SECID = :secid
            """
            await cursor.execute(query, secid=secid, name=name, translation=translation, disabled=disabled, parent_dept_system_id=parent_dept_system_id)
            await conn.commit()
            return True, "EMS Section updated successfully"
    except oracledb.Error as ex:
        error, = ex.args
        logging.error(f"Error updating EMS section: {error.message}")
        return False, str(error.message)
    finally:
        if conn:
            await conn.close()
