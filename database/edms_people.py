import logging
from typing import Dict, Any, List, Optional
import db_connector
import hashlib
import base64

# --- Helper functions ---
def hash_password(password: str) -> str:
    """Hash password according to EDMS format. Defaulting to MD5 Base64."""
    if not password:
        return ""
    return base64.b64encode(hashlib.md5(password.encode('utf-8')).digest()).decode()


async def get_edms_people(search: str = "", page: int = 1, limit: int = 20) -> Dict[str, Any]:
    """Get paginated users from PEOPLE table along with their HR and group details."""
    connection = None
    try:
        connection = await db_connector.get_async_connection()
        start = (page - 1) * limit
        
        query = """
            SELECT * FROM (
                SELECT 
                    p.SYSTEM_ID, p.USER_ID, p.FULL_NAME, p.EMAIL_ADDRESS, 
                    p.DISABLED, p.ALLOW_LOGIN, p.PRIMARY_GROUP, p.SECID,
                    g.GROUP_NAME as PRIMARY_GROUP_NAME,
                    hr.AGENCYID as HR_AGENCYID, 
                    hr.DEPARTMENTID as HR_DEPARTMENTID, 
                    hr.SECTIONID as HR_SECTIONID,
                    COUNT(DISTINCT pg.GROUPS_SYSTEM_ID) as GROUPS_COUNT,
                    ROW_NUMBER() OVER (ORDER BY p.SYSTEM_ID DESC) as rn,
                    COUNT(*) OVER () as total_count
                FROM PEOPLE p
                LEFT JOIN GROUPS g ON p.PRIMARY_GROUP = g.SYSTEM_ID
                LEFT JOIN (
                    SELECT LOGIN, MAX(AGENCYID) as AGENCYID, MAX(DEPARTMENTID) as DEPARTMENTID, MAX(SECTIONID) as SECTIONID
                    FROM LKP_HR_EMPLOYEES
                    GROUP BY LOGIN
                ) hr ON UPPER(p.USER_ID) = UPPER(hr.LOGIN)
                LEFT JOIN PEOPLEGROUPS pg ON p.SYSTEM_ID = pg.PEOPLE_SYSTEM_ID
                WHERE (:search IS NULL OR UPPER(p.USER_ID) LIKE UPPER('%' || :search || '%')
                       OR UPPER(p.FULL_NAME) LIKE UPPER('%' || :search || '%')
                       OR UPPER(p.EMAIL_ADDRESS) LIKE UPPER('%' || :search || '%'))
                GROUP BY p.SYSTEM_ID, p.USER_ID, p.FULL_NAME, p.EMAIL_ADDRESS, 
                         p.DISABLED, p.ALLOW_LOGIN, p.PRIMARY_GROUP, p.SECID, g.GROUP_NAME,
                         hr.AGENCYID, hr.DEPARTMENTID, hr.SECTIONID
            )
            WHERE rn > :start_idx AND rn <= :end_idx
        """
        
        params = {
            "search": search if search else None,
            "start_idx": start,
            "end_idx": start + limit
        }
        
        with connection.cursor() as cursor:
            await cursor.execute(query, params)
            columns = [col[0].lower() for col in cursor.description]
            rows = await cursor.fetchall()

            if not rows:
                return {"users": [], "total": 0, "has_more": False}

            total_count = rows[0][columns.index('total_count')]
            users = [dict(zip(columns, row)) for row in rows]

            return {
                "users": users,
                "total": total_count,
                "has_more": (start + limit) < total_count
            }

    except Exception as e:
        logging.error(f"Error fetching EDMS people: {str(e)}")
        return {"users": [], "total": 0, "has_more": False}
    finally:
        if connection:
            await connection.close()


async def search_hr_employees(search: str = "") -> List[Dict[str, Any]]:
    """Search LKP_HR_EMPLOYEES for users not yet marked as EDMS users by login, name, email, or EMPNO."""
    connection = None
    try:
        connection = await db_connector.get_async_connection()
        # Get max 50 for search capability
        query = """
            SELECT * FROM (
                SELECT 
                    SYSTEM_ID, LOGIN, FULLNAME_EN, FULLNAME_AR, EMAIL,
                    AGENCY, DEPARTEMENT as DEPARTMENT, SECTION_ORG as SECTION, EMPNO,
                    AGENCYID, DEPARTMENTID, SECTIONID, IS_EDMS_USR
                FROM LKP_HR_EMPLOYEES
                WHERE (IS_EDMS_USR != 'Y' OR IS_EDMS_USR IS NULL)
            """
            
        params = {}
        if search:
            query += """ AND (UPPER(LOGIN) LIKE UPPER('%' || :search || '%')
                         OR UPPER(FULLNAME_EN) LIKE UPPER('%' || :search || '%')
                         OR UPPER(EMAIL) LIKE UPPER('%' || :search || '%')
                         OR UPPER(EMPNO) LIKE UPPER('%' || :search || '%'))
                     """
            params['search'] = search
            
        query += " ORDER BY FULLNAME_EN ) WHERE ROWNUM <= 50"

        with connection.cursor() as cursor:
            await cursor.execute(query, params)
            columns = [col[0].lower() for col in cursor.description]
            rows = await cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]

    except Exception as e:
        logging.error(f"Error searching HR employees: {e}")
        return []
    finally:
        if connection:
            await connection.close()

async def get_hr_agencies() -> List[Dict[str, Any]]:
    """Get all agencies from LKP_ORG_AGENCY."""
    connection = None
    try:
        connection = await db_connector.get_async_connection()
        query = "SELECT SYSTEM_ID, NAME FROM LKP_ORG_AGENCY WHERE DISABLED = 'N' ORDER BY NAME"
        with connection.cursor() as cursor:
            await cursor.execute(query)
            cols = [col[0].lower() for col in cursor.description]
            return [dict(zip(cols, row)) for row in await cursor.fetchall()]
    except Exception as e:
        logging.error(f"Error fetching HR agencies: {e}")
        return []
    finally:
        if connection:
            await connection.close()

async def get_hr_departments(agency_id: int) -> List[Dict[str, Any]]:
    """Get all departments for a given agency from LKP_DEPT."""
    connection = None
    try:
        connection = await db_connector.get_async_connection()
        query = "SELECT DEPTID as SYSTEM_ID, NAME FROM LKP_DEPT WHERE AGENCYID = :aid AND DISABLED = 'N' ORDER BY NAME"
        with connection.cursor() as cursor:
            await cursor.execute(query, {'aid': agency_id})
            cols = [col[0].lower() for col in cursor.description]
            return [dict(zip(cols, row)) for row in await cursor.fetchall()]
    except Exception as e:
        logging.error(f"Error fetching HR departments: {e}")
        return []
    finally:
        if connection:
            await connection.close()

async def get_hr_sections(dept_id: int) -> List[Dict[str, Any]]:
    """Get all sections for a given department from LKP_SECTION."""
    connection = None
    try:
        connection = await db_connector.get_async_connection()
        query = "SELECT SECID as SYSTEM_ID, NAME FROM LKP_SECTION WHERE DEPTID = :did AND DISABLED = 'N' ORDER BY NAME"
        with connection.cursor() as cursor:
            await cursor.execute(query, {'did': dept_id})
            cols = [col[0].lower() for col in cursor.description]
            return [dict(zip(cols, row)) for row in await cursor.fetchall()]
    except Exception as e:
        logging.error(f"Error fetching HR sections: {e}")
        return []
    finally:
        if connection:
            await connection.close()

async def get_all_groups() -> List[Dict[str, Any]]:
    """Get all EDMS groups from GROUPS table."""
    connection = None
    try:
        connection = await db_connector.get_async_connection()
        query = """
            SELECT SYSTEM_ID, GROUP_ID, GROUP_NAME 
            FROM GROUPS
            ORDER BY GROUP_NAME
        """
        with connection.cursor() as cursor:
            await cursor.execute(query)
            columns = [col[0].lower() for col in cursor.description]
            rows = await cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logging.error(f"Error fetching groups: {e}")
        return []
    finally:
        if connection:
            await connection.close()


async def get_person_details(people_system_id: int) -> Dict[str, Any]:
    """Get groups, aliases, and HR data for a specific person."""
    connection = None
    try:
        connection = await db_connector.get_async_connection()
        
        person_query = """
            SELECT p.USER_ID, p.FULL_NAME
            FROM PEOPLE p
            WHERE p.SYSTEM_ID = :people_id
        """
        
        groups_query = """
            SELECT pg.GROUPS_SYSTEM_ID as SYSTEM_ID, g.GROUP_ID, g.GROUP_NAME
            FROM PEOPLEGROUPS pg
            JOIN GROUPS g ON pg.GROUPS_SYSTEM_ID = g.SYSTEM_ID
            WHERE pg.PEOPLE_SYSTEM_ID = :people_id
        """
        
        aliases_query = """
            SELECT NETWORK_ID, NETWORK_TYPE 
            FROM NETWORK_ALIASES 
            WHERE PERSONORGROUP = :people_id
        """
        
        hr_query = """
            SELECT hr.AGENCYID, hr.AGENCY, hr.DEPARTMENTID, hr.DEPARTEMENT as DEPARTMENT, hr.SECTIONID, hr.SECTION_ORG as SECTION
            FROM LKP_HR_EMPLOYEES hr
            WHERE UPPER(hr.LOGIN) = UPPER(:user_id)
            ORDER BY hr.AGENCYID DESC
            FETCH FIRST 1 ROW ONLY
        """

        result = {"groups": [], "aliases": [], "agency": "", "department": "", "section": ""}
        user_id = None
        
        with connection.cursor() as cursor:
            # Fetch person's user_id first
            await cursor.execute(person_query, {"people_id": people_system_id})
            if cursor.description:
                p_cols = [c[0].lower() for c in cursor.description]
                p_data = await cursor.fetchone()
                if p_data:
                    person_dict = dict(zip(p_cols, p_data))
                    user_id = person_dict.get("user_id")
            
            # Fetch groups
            await cursor.execute(groups_query, {"people_id": people_system_id})
            g_cols = [c[0].lower() for c in cursor.description]
            result["groups"] = [dict(zip(g_cols, r)) for r in await cursor.fetchall()]
            
            # Fetch aliases
            await cursor.execute(aliases_query, {"people_id": people_system_id})
            if cursor.description:
                a_cols = [c[0].lower() for c in cursor.description]
                result["aliases"] = [dict(zip(a_cols, r)) for r in await cursor.fetchall()]
            
            # Fetch HR data if user_id found
            if user_id:
                await cursor.execute(hr_query, {"user_id": user_id})
                if cursor.description:
                    hr_cols = [c[0].lower() for c in cursor.description]
                    hr_data = await cursor.fetchone()
                    if hr_data:
                        hr_dict = dict(zip(hr_cols, hr_data))
                        result["agency"] = hr_dict.get("agency", "") or ""
                        result["department"] = hr_dict.get("department", "") or ""
                        result["section"] = hr_dict.get("section", "") or ""

        return result
    except Exception as e:
        logging.error(f"Error fetching person details: {e}")
        return {"groups": [], "aliases": [], "agency": "", "department": "", "section": ""}
    finally:
        if connection:
            await connection.close()


async def add_edms_person(
    user_id: str,
    full_name: str,
    email: str,
    password_plain: str,
    primary_group: int,
    allow_login: str,
    disabled: str,
    secid: int,
    additional_groups: List[int],
    network_aliases: List[str],
    hr_login: Optional[str] = None,
    hr_empno: Optional[str] = None
) -> tuple[bool, str]:
    """Add a new user to EDMS (PEOPLE, PEOPLEGROUPS, NETWORK_ALIASES)."""
    connection = None
    try:
        connection = await db_connector.get_async_connection()
        
        user_id = user_id.upper().strip()
        pwd_hash = hash_password(password_plain)

        with connection.cursor() as cursor:
            # Check if USER_ID already exists
            logging.info(f"[add_edms_person] Checking if user {user_id} exists")
            await cursor.execute("SELECT 1 FROM PEOPLE WHERE UPPER(USER_ID) = :1", [user_id])
            if await cursor.fetchone():
                return False, f"Username {user_id} already exists in EDMS."

            # Pre-fetch sequence value
            logging.info(f"[add_edms_person] Fetching next SYSTEM_ID from sequence")
            await cursor.execute("SELECT SEQSYSTEMKEY.NEXTVAL FROM dual")
            row = await cursor.fetchone()
            new_sys_id = row[0]
            logging.info(f"[add_edms_person] Got new_sys_id={new_sys_id}")

            # Insert PEOPLE row using positional bind variables
            profile_defaults = f"0;4;AUTHOR.USER_ID;DOCSADM.PEOPLE.SYSTEM_ID='{user_id}'"
            logging.info(f"[add_edms_person] Inserting into PEOPLE")
            await cursor.execute("""
                INSERT INTO PEOPLE (
                    SYSTEM_ID, USER_ID, FULL_NAME, EMAIL_ADDRESS, USER_PASSWORD,
                    ALLOW_LOGIN, DISABLED, PRIMARY_GROUP, SECID,
                    PROFILE_DEFAULTS, LAST_UPDATE
                ) VALUES (
                    :1, :2, :3, :4, :5,
                    :6, :7, :8, :9,
                    :10, SYSDATE
                )
            """, [new_sys_id, user_id, full_name, email, pwd_hash,
                  allow_login, disabled, primary_group, secid,
                  profile_defaults])

            # Add groups
            all_groups = set(additional_groups)
            if primary_group:
                all_groups.add(primary_group)
            
            logging.info(f"[add_edms_person] Inserting groups: {all_groups}")
            for gid in all_groups:
                await cursor.execute("""
                    INSERT INTO PEOPLEGROUPS (PEOPLE_SYSTEM_ID, GROUPS_SYSTEM_ID, LAST_UPDATE)
                    VALUES (:1, :2, SYSDATE)
                """, [new_sys_id, gid])
                
            # Add Network Aliases
            logging.info(f"[add_edms_person] Inserting network aliases: {network_aliases}")
            for net_alias in network_aliases:
                if net_alias.strip():
                    await cursor.execute("""
                        INSERT INTO NETWORK_ALIASES (
                            SYSTEM_ID, NETWORK_ID, NETWORK_TYPE, PERSONORGROUP, LAST_UPDATE
                        )
                        SELECT SEQSYSTEMKEY.NEXTVAL, :1, 8, :2, SYSDATE FROM dual
                        WHERE NOT EXISTS (
                            SELECT 1 FROM NETWORK_ALIASES WHERE UPPER(NETWORK_ID) = UPPER(:3)
                        )
                    """, [net_alias.strip(), new_sys_id, net_alias.strip()])
                    
            # Update HR record
            if hr_login:
                logging.info(f"[add_edms_person] Marking HR employee as EDMS user: {hr_login}")
                await cursor.execute("""
                    UPDATE LKP_HR_EMPLOYEES SET IS_EDMS_USR = 'Y'
                    WHERE UPPER(LOGIN) = UPPER(:1)
                """, [hr_login])
                
        await connection.commit()
        return True, "User successfully created in EDMS."
    except Exception as e:
        if connection:
            await connection.rollback()
        logging.error(f"Error creating EDMS user: {e}")
        return False, f"Database error: {str(e)}"
    finally:
        if connection:
            await connection.close()


async def update_edms_person(
    system_id: int,
    full_name: str,
    email: str,
    password_plain: str,
    primary_group: int,
    allow_login: str,
    disabled: str,
    secid: int,
    additional_groups: List[int],
    network_aliases: List[str]
) -> tuple[bool, str]:
    """Update an existing user in EDMS (PEOPLE, PEOPLEGROUPS, NETWORK_ALIASES)."""
    connection = None
    try:
        connection = await db_connector.get_async_connection()

        with connection.cursor() as cursor:
            if password_plain:
                await cursor.execute("""
                    UPDATE PEOPLE SET
                        FULL_NAME = :1, EMAIL_ADDRESS = :2, USER_PASSWORD = :3,
                        ALLOW_LOGIN = :4, DISABLED = :5, PRIMARY_GROUP = :6,
                        SECID = :7, LAST_UPDATE = SYSDATE
                    WHERE SYSTEM_ID = :8
                """, [full_name, email, hash_password(password_plain),
                      allow_login, disabled, primary_group, secid, system_id])
            else:
                await cursor.execute("""
                    UPDATE PEOPLE SET
                        FULL_NAME = :1, EMAIL_ADDRESS = :2,
                        ALLOW_LOGIN = :3, DISABLED = :4, PRIMARY_GROUP = :5,
                        SECID = :6, LAST_UPDATE = SYSDATE
                    WHERE SYSTEM_ID = :7
                """, [full_name, email, allow_login, disabled, primary_group, secid, system_id])

            # Rebuild groups
            await cursor.execute("DELETE FROM PEOPLEGROUPS WHERE PEOPLE_SYSTEM_ID = :1", [system_id])
            all_groups = set(additional_groups)
            if primary_group:
                all_groups.add(primary_group)
            
            for gid in all_groups:
                await cursor.execute("""
                    INSERT INTO PEOPLEGROUPS (PEOPLE_SYSTEM_ID, GROUPS_SYSTEM_ID, LAST_UPDATE)
                    VALUES (:1, :2, SYSDATE)
                """, [system_id, gid])
                
            # Rebuild Aliases
            await cursor.execute("DELETE FROM NETWORK_ALIASES WHERE PERSONORGROUP = :1", [system_id])
            for net_alias in network_aliases:
                if net_alias.strip():
                    await cursor.execute("""
                        INSERT INTO NETWORK_ALIASES (
                            SYSTEM_ID, NETWORK_ID, NETWORK_TYPE, PERSONORGROUP, LAST_UPDATE
                        ) VALUES (
                            SEQSYSTEMKEY.NEXTVAL, :1, 8, :2, SYSDATE
                        )
                    """, [net_alias.strip(), system_id])

        await connection.commit()
        return True, "User successfully updated."
    except Exception as e:
        if connection:
            await connection.rollback()
        logging.error(f"Error updating EDMS user: {e}")
        return False, f"Database error: {str(e)}"
    finally:
        if connection:
            await connection.close()
