import logging
import oracledb
from database.connection import get_async_connection
from datetime import datetime

async def fetch_search_scopes(user_id):
    """
    Returns distinct search scopes from LKP_MAIN_FIELDS,
    filtered by the user's group permissions.
    Each scope is derived from the SEARCH_FORM column.
    """
    conn = await get_async_connection()
    if not conn:
        return _get_fallback_scopes()

    try:
        async with conn.cursor() as cursor:
            sql = """
                SELECT DISTINCT SEARCH_FORM, FORM
                FROM LKP_MAIN_FIELDS
                WHERE (DISABLED <> 'Y')
                  AND UPPER(FORM) IN (
                    SELECT DISTINCT UPPER(FORMS.FORM_NAME) AS UFORM
                    FROM SEARCH_FORM sf, FORMS, PEOPLEGROUPS, PEOPLE, GROUPS
                    WHERE sf.FORM_ID = FORMS.SYSTEM_ID
                      AND sf.GROUP_ID = GROUPS.SYSTEM_ID
                      AND PEOPLEGROUPS.PEOPLE_SYSTEM_ID = PEOPLE.SYSTEM_ID
                      AND PEOPLEGROUPS.GROUPS_SYSTEM_ID = GROUPS.SYSTEM_ID
                      AND PEOPLE.USER_ID = UPPER(:user_id)
                  )
                ORDER BY 1
            """
            await cursor.execute(sql, {'user_id': user_id.strip()})
            rows = await cursor.fetchall()

            if not rows:
                return _get_fallback_scopes()

            # Build unique scopes from SEARCH_FORM values
            seen = set()
            scopes = []
            for row in rows:
                search_form = str(row[0]).strip() if row[0] else ""
                form_name = str(row[1]).strip() if row[1] else ""
                # Use SEARCH_FORM as scope key; skip empty/duplicate
                key = search_form or form_name
                if not key or key in seen:
                    continue
                seen.add(key)
                scopes.append({
                    "label": key,
                    "value": key
                })

            return scopes

    except Exception as e:
        logging.error(f"Error fetching search scopes: {e}")
        return _get_fallback_scopes()
    finally:
        await conn.close()


def _get_fallback_scopes():
    """Fallback scopes derived from hardcoded types."""
    return [
        {"label": "All", "value": "0"},
        {"label": "Vehicles & General (3799)", "value": "3799"},
        {"label": "Files (2572)", "value": "2572"},
        {"label": "Projects / Ref (4239)", "value": "4239"},
    ]


async def fetch_search_types(user_id, scope=None):
    """
    Returns a list of search types dynamically from LKP_MAIN_FIELDS,
    filtered by the user's group permissions.
    Replicates the legacy Researcher (Home.aspx.cs) GetDoc_types() method.
    Falls back to hardcoded types if the dynamic query fails.
    """
    conn = await get_async_connection()
    if not conn:
        return _get_fallback_types()

    try:
        async with conn.cursor() as cursor:
            # This is the exact same SQL logic from the legacy Home.aspx.cs (lines 33-38)
            # It queries LKP_MAIN_FIELDS filtered by forms the user has access to
            # via their group memberships.
            sql = """
                SELECT TYPE_NAME, FIELD_NAME, FORM, FORM_SRCH_FIELD,
                       TYPE_NAME_EN, system_id, SEARCH_FORM, DISPLAY, exact
                FROM LKP_MAIN_FIELDS
                WHERE (DISABLED <> 'Y')
                  AND UPPER(FORM) IN (
                    SELECT DISTINCT UPPER(FORMS.FORM_NAME) AS UFORM
                    FROM SEARCH_FORM sf, FORMS, PEOPLEGROUPS, PEOPLE, GROUPS
                    WHERE sf.FORM_ID = FORMS.SYSTEM_ID
                      AND sf.GROUP_ID = GROUPS.SYSTEM_ID
                      AND PEOPLEGROUPS.PEOPLE_SYSTEM_ID = PEOPLE.SYSTEM_ID
                      AND PEOPLEGROUPS.GROUPS_SYSTEM_ID = GROUPS.SYSTEM_ID
                      AND PEOPLE.USER_ID = UPPER(:user_id)
                  )
            """
            bind_params = {'user_id': user_id.strip()}

            # Filter by scope if provided
            if scope and scope != '0':
                sql += "  AND (UPPER(SEARCH_FORM) = UPPER(:scope) OR UPPER(FORM) = UPPER(:scope))\n"
                bind_params['scope'] = scope.strip()

            sql += "                ORDER BY 1"

            await cursor.execute(sql, bind_params)
            rows = await cursor.fetchall()

            if not rows:
                logging.warning(f"No search types found for user '{user_id}' from LKP_MAIN_FIELDS. Falling back to hardcoded types.")
                return _get_fallback_types()

            types = []
            for row in rows:
                type_name_ar = str(row[0]) if row[0] else ""
                field_name = str(row[1]) if row[1] else ""
                form = str(row[2]) if row[2] else ""
                form_srch_field = str(row[3]) if row[3] else ""
                type_name_en = str(row[4]) if row[4] else ""
                system_id = str(row[5]) if row[5] else ""
                search_form = str(row[6]) if row[6] else ""
                display = str(row[7]) if row[7] else ""
                exact = str(row[8]) if row[8] else ""

                # Label matches legacy: "Arabic Name   (English Name)"
                label = f"{type_name_ar}   ({type_name_en})" if type_name_ar else type_name_en

                types.append({
                    "label": label,
                    "value": {
                        "field_name": field_name,
                        "form": form,
                        "search_field": form_srch_field,
                        "type_name_en": type_name_en,
                        "type_name_ar": type_name_ar,
                        "system_id": system_id,
                        "search_form": search_form,
                        "display_field": display,
                        "exact": exact
                    }
                })

            # logging.info(f"Loaded {len(types)} search types from LKP_MAIN_FIELDS for user '{user_id}'.")
            return types

    except Exception as e:
        logging.error(f"Error querying LKP_MAIN_FIELDS for search types: {e}. Falling back to hardcoded types.")
        return _get_fallback_types()
    finally:
        await conn.close()


def _get_fallback_types():
    """
    Returns hardcoded search types as a fallback if the LKP_MAIN_FIELDS
    table is not accessible.
    """
    hardcoded_types = [
        ("Document Number", "DOCNUMBER", "F", "0"),
        ("Subject / Title", "DOCNAME", "F", "0"),
        ("File Number", "FILE_NUMBER", "T", "2572"),
        ("App Number", "APP_NUMBER", "T", "22010818"),
        ("Trade License No", "TRADE_LICENSE_NO", "T", "21994731"),
        ("Plate Number", "PLATE_NO", "T", "9092020"),
        ("Chassis Number", "CHASSIS_NUMBER", "T", "3799"),
        ("Staff ID", "STAFF_ID", "T", "0"),
        ("Contract Number", "RTA_CONTRACTNUM", "T", "2616"),
        ("Project", "LKP_PROJECT", "F", "4239"),
        ("Email Sender", "PD_ORIGINATOR", "F", "2442"),
        ("Email Recipient", "PD_ADDRESSEE", "F", "2442"),
        ("Reference Number", "RTAREFNUM", "T", "4239"),
        ("External Ref Num", "RTA_EXTREFNUM", "T", "3799"),
    ]

    types = []
    system_id_counter = 1
    for label, field, exact_match, form_id in hardcoded_types:
        types.append({
            "label": label,
            "value": {
                "field_name": field,
                "form": form_id,
                "search_field": field,
                "type_name_en": label,
                "system_id": system_id_counter,
                "search_form": "0",
                "display_field": field,
                "exact": exact_match
            }
        })
        system_id_counter += 1

    return types

async def search_documents(
    user_id, 
    form_name, 
    field_name, 
    keyword, 
    search_form, 
    search_field, 
    match_type='like',
    date_from=None, 
    date_to=None, 
    display_field=None,
    page=1, 
    page_size=20
):
    """
    Performs the dynamic search based on the selected Form/Field type.
    Replicates the legacy Researcher (Home.aspx.cs) General_Search method.
    
    Parameters mapping from legacy:
    - form_name: FORM from LKP_MAIN_FIELDS (e.g., 'SEARCH_S354', 'DEF_QBE')
                 This is the search profile/object name in legacy PCDSearch.
    - field_name: FIELD_NAME from LKP_MAIN_FIELDS (e.g., 'DOCNAME', 'NUM:ID_NO,STR:RTA_PERSON1')
    - search_form: SEARCH_FORM from LKP_MAIN_FIELDS (e.g., 'S354', 'G001', 'S043,S045')
                   These are the form names whose SYSTEM_IDs are used to filter PROFILE.FORM.
    - search_field: FORM_SRCH_FIELD from LKP_MAIN_FIELDS (e.g., 'FORM_NAME', 'FORM1')
    - display_field: DISPLAY from LKP_MAIN_FIELDS (e.g., 'DOCNAME', 'RTA_TEXT1')
    """
    import re

    conn = await get_async_connection()
    if not conn:
        return [], 0

    try:
        async with conn.cursor() as cursor:
            # --- Step 1: Resolve the field name ---
            # Legacy logic (Home.aspx.cs lines 568-609):
            # If field_name contains "NUM:" and "STR:" parts separated by comma,
            # pick the NUM field if keyword is numeric, STR field if keyword is text.
            clean_field_name = field_name
            
            if ',' in field_name and ':' in field_name:
                # Compound field like "NUM:ID_NO,STR:RTA_PERSON1"
                is_numeric = keyword and keyword.strip().replace('*', '').isdigit() if keyword else False
                parts = field_name.split(',')
                for p in parts:
                    p = p.strip()
                    if is_numeric and p.upper().startswith('NUM:'):
                        clean_field_name = p.split(':')[1]
                        break
                    elif not is_numeric and p.upper().startswith('STR:'):
                        clean_field_name = p.split(':')[1]
                        break
                else:
                    # Fallback: take the first field
                    first = parts[0].strip()
                    clean_field_name = first.split(':')[1] if ':' in first else first
            elif ':' in field_name:
                # Single prefixed field like "NUM:RTA_REF2"
                clean_field_name = field_name.split(':')[1]

            # --- Step 2: Handle special fields ---
            # FULLTEXT_CONTENT is a DM full-text search feature, not a PROFILE column.
            # Fall back to searching DOCNAME + ABSTRACT instead.
            is_fulltext = clean_field_name.upper() == 'FULLTEXT_CONTENT'
            
            # Validate the resolved field name (must be a safe column name)
            if not is_fulltext and not re.match(r'^[A-Z0-9_]+$', clean_field_name.upper()):
                logging.warning(f"Invalid resolved field_name: {clean_field_name} (original: {field_name})")
                return [], 0

            # --- Step 3: Resolve form IDs ---
            # Two sources of form filtering:
            # 1. search_form from LKP_MAIN_FIELDS: comma-separated form NAMES like "S043,S045,S046,S084"
            # 2. form_name from searchScope dropdown: can be a numeric form ID like "3799" or 
            #    a search profile name like "SEARCH_S354"
            # 3. "0" or "ALL" means no form filter
            target_form_ids = []
            
            # Determine which source to use for form filtering
            form_source = search_form if search_form and search_form != '0' else None
            form_name_source = form_name if form_name and form_name != '0' and form_name.upper() != 'ALL' else None
            
            # If form_name is a pure numeric ID (from searchScope dropdown), use it directly
            if form_name_source and form_name_source.isdigit():
                target_form_ids = [int(form_name_source)]
            elif form_source:
                # search_form has comma-separated form names — look up their IDs
                form_names_list = [f.strip() for f in form_source.split(',') if f.strip()]
                if form_names_list:
                    placeholders = ', '.join([f':fn{i}' for i in range(len(form_names_list))])
                    lookup_sql = f"SELECT SYSTEM_ID FROM FORMS WHERE UPPER(FORM_NAME) IN ({placeholders})"
                    lookup_params = {f'fn{i}': name.upper() for i, name in enumerate(form_names_list)}
                    
                    await cursor.execute(lookup_sql, lookup_params)
                    form_rows = await cursor.fetchall()
                    target_form_ids = [row[0] for row in form_rows]
            elif form_name_source:
                # form_name is a text name like "SEARCH_S354" or "DEF_QBE" — look up its ID
                lookup_sql = "SELECT SYSTEM_ID FROM FORMS WHERE UPPER(FORM_NAME) = UPPER(:fn)"
                await cursor.execute(lookup_sql, {'fn': form_name_source})
                form_row = await cursor.fetchone()
                if form_row:
                    target_form_ids = [form_row[0]]

            # --- Step 4: Build the WHERE clause ---
            where_clauses = []
            params = {}

            # Filter by form IDs if we resolved any
            if target_form_ids:
                if len(target_form_ids) == 1:
                    where_clauses.append("p.FORM = :form_id")
                    params['form_id'] = target_form_ids[0]
                else:
                    form_placeholders = ', '.join([f':fid{i}' for i in range(len(target_form_ids))])
                    where_clauses.append(f"p.FORM IN ({form_placeholders})")
                    for i, fid in enumerate(target_form_ids):
                        params[f'fid{i}'] = fid

            # Apply keyword filter
            if keyword:
                # Legacy exact pattern handling (Home.aspx.cs line 558):
                # The 'exact' field from LKP_MAIN_FIELDS contains patterns like:
                #   "T"    -> exact match (just the keyword)
                #   "*T*"  -> contains (wrap in wildcards)
                #   "T*"   -> starts with (wildcard at end)
                # The legacy code does: search_text = exact.Replace("T", search_text)
                # But in our API, the match_type is determined from the 'exact' field.
                
                check_keyword = keyword.strip().upper()
                
                # Strip wildcards if keyword is numeric (legacy line 563)
                try:
                    int(check_keyword.replace('*', '').replace('%', ''))
                    check_keyword = check_keyword.replace('*', '').replace('%', '')
                except ValueError:
                    pass
                
                if is_fulltext:
                    # Full-text: search across DOCNAME and ABSTRACT
                    where_clauses.append(
                        "(UPPER(p.DOCNAME) LIKE :keyword OR UPPER(p.ABSTRACT) LIKE :keyword)"
                    )
                    params['keyword'] = f"%{check_keyword}%"
                else:
                    if match_type == 'exact':
                        where_clauses.append(f"UPPER(p.{clean_field_name}) = :keyword")
                        params['keyword'] = check_keyword
                    elif match_type == 'startsWith':
                        where_clauses.append(f"UPPER(p.{clean_field_name}) LIKE :keyword")
                        params['keyword'] = f"{check_keyword}%"
                    else:  # 'like' / contains
                        if '%' not in check_keyword:
                            check_keyword = f"%{check_keyword}%"
                        where_clauses.append(f"UPPER(p.{clean_field_name}) LIKE :keyword")
                        params['keyword'] = check_keyword

            # Date range filter
            if date_from and date_to:
                where_clauses.append("p.CREATION_DATE BETWEEN TO_DATE(:d_from, 'YYYY-MM-DD') AND TO_DATE(:d_to, 'YYYY-MM-DD')")
                params['d_from'] = date_from
                params['d_to'] = date_to
            elif date_from:
                where_clauses.append("p.CREATION_DATE >= TO_DATE(:d_from, 'YYYY-MM-DD')")
                params['d_from'] = date_from
            elif date_to:
                where_clauses.append("p.CREATION_DATE <= TO_DATE(:d_to, 'YYYY-MM-DD')")
                params['d_to'] = date_to

            # Must have at least one filter
            if not where_clauses:
                logging.warning("No search criteria provided, returning empty results.")
                return [], 0

            # --- Step 5: Resolve display field ---
            clean_display_field = 'DOCNAME'
            if display_field and re.match(r'^[A-Z0-9_]+$', display_field.upper()):
                clean_display_field = display_field

            # --- Step 6: Execute the query ---
            offset = (page - 1) * page_size

            def build_sql(disp_field):
                return f"""
                SELECT /*+ FIRST_ROWS({page_size}) */
                    p.DOCNUMBER, 
                    p.ABSTRACT, 
                    p.DOCNAME, 
                    p.CREATION_DATE, 
                    p.TYPIST,
                    a.DEFAULT_EXTENSION,
                    p.{disp_field} as DISPLAY_VAL
                FROM PROFILE p
                LEFT JOIN APPS a ON p.APPLICATION = a.SYSTEM_ID
                WHERE {' AND '.join(where_clauses)}
                ORDER BY p.CREATION_DATE DESC
                OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY
                """
            
            params['offset'] = offset
            params['page_size'] = page_size
            
            # Try with the requested display field, fall back to DOCNAME if column doesn't exist
            # Also handles case where the search field in WHERE clause doesn't exist
            try:
                sql = build_sql(clean_display_field)
                await cursor.execute(sql, params)
            except oracledb.Error as col_err:
                err_msg = str(col_err)
                if 'ORA-00904' in err_msg:
                    # Identify which column is invalid and retry with fallbacks
                    logging.warning(f"Column error in search query: {col_err}. Retrying with DOCNAME fallbacks.")
                    
                    # Reset to safe columns
                    clean_display_field = 'DOCNAME'
                    
                    # If the search field itself is invalid, rebuild WHERE with DOCNAME
                    if not is_fulltext and clean_field_name.upper() != 'DOCNAME':
                        # Rebuild where_clauses with DOCNAME instead of the problematic field
                        new_where = []
                        new_params = {}
                        for key, val in params.items():
                            if key not in ('keyword', 'offset', 'page_size'):
                                new_params[key] = val
                        
                        # Keep form filter clauses
                        for clause in where_clauses:
                            if clean_field_name not in clause:
                                new_where.append(clause)
                                
                        # Re-add keyword with DOCNAME
                        if keyword and 'keyword' in params:
                            new_where.append("UPPER(p.DOCNAME) LIKE :keyword")
                            kw = keyword.strip().upper()
                            new_params['keyword'] = f"%{kw}%" if '%' not in kw else kw
                        
                        new_params['offset'] = offset
                        new_params['page_size'] = page_size
                        
                        if new_where:
                            where_clauses = new_where
                            params = new_params
                    
                    sql = build_sql('DOCNAME')
                    await cursor.execute(sql, params)
                else:
                    raise

            rows = await cursor.fetchall()
            
            documents = []
            for row in rows:
                doc_id, abstract, docname, date, typist, ext, display_val = row
                
                date_str = date.strftime('%Y-%m-%d') if date else ""
                
                media_type = 'unknown'
                ext_str = (str(ext) if ext else "").lower().replace('.', '')
                if ext_str in ['pdf']: media_type = 'pdf'
                elif ext_str in ['doc', 'docx']: media_type = 'word'
                elif ext_str in ['xls', 'xlsx']: media_type = 'excel'
                elif ext_str in ['ppt', 'pptx']: media_type = 'powerpoint'
                elif ext_str in ['zip', 'rar', '7z', 'tar', 'gz']: media_type = 'zip'
                elif ext_str in ['jpg', 'png', 'jpeg', 'tif', 'tiff']: media_type = 'image'
                elif ext_str in ['msg', 'eml']: media_type = 'email'
                elif ext_str in ['mp4', 'avi']: media_type = 'video'

                documents.append({
                    "doc_id": doc_id,
                    "title": abstract or docname, 
                    "docname": docname,
                    "date": date_str,
                    "typist": typist,
                    "media_type": media_type,
                    "extension": ext_str,
                    "display_value": display_val,
                    "thumbnail_url": f"temp_thumbnail/{doc_id}" if media_type in ['image', 'video', 'pdf'] else ""
                })

            # Pagination estimation (skip expensive COUNT)
            fetched_count = len(documents)
            if fetched_count == page_size:
                 total_rows = (page * page_size) + 20 
            else:
                 total_rows = (page - 1) * page_size + fetched_count
            
            return documents, total_rows

    except oracledb.Error as e:
        logging.error(f"Oracle error in search_documents: {e}")
        return [], 0
    finally:
        await conn.close()


async def search_documents_multi(
    user_id,
    scope,
    criteria,
    date_from=None,
    date_to=None,
    page=1,
    page_size=20
):
    """
    Multi-criteria search: each item in criteria list adds an AND condition.
    criteria = [{ field_name, keyword, match_type, search_form, search_field, display_field }]
    scope = form ID or search_form name used to filter PROFILE.FORM
    """
    import re

    if not criteria:
        return [], 0

    conn = await get_async_connection()
    if not conn:
        return [], 0

    try:
        async with conn.cursor() as cursor:
            where_clauses = []
            params = {}

            # --- Resolve scope to form IDs ---
            target_form_ids = []
            if scope and scope != '0':
                if scope.isdigit():
                    target_form_ids = [int(scope)]
                else:
                    # Look up form names from scope (could be comma-separated)
                    form_names_list = [f.strip() for f in scope.split(',') if f.strip()]
                    if form_names_list:
                        placeholders = ', '.join([f':sfn{i}' for i in range(len(form_names_list))])
                        lookup_sql = f"SELECT SYSTEM_ID FROM FORMS WHERE UPPER(FORM_NAME) IN ({placeholders})"
                        lookup_params = {f'sfn{i}': name.upper() for i, name in enumerate(form_names_list)}
                        await cursor.execute(lookup_sql, lookup_params)
                        form_rows = await cursor.fetchall()
                        target_form_ids = [row[0] for row in form_rows]

            if target_form_ids:
                if len(target_form_ids) == 1:
                    where_clauses.append("p.FORM = :form_id")
                    params['form_id'] = target_form_ids[0]
                else:
                    fp = ', '.join([f':fid{i}' for i in range(len(target_form_ids))])
                    where_clauses.append(f"p.FORM IN ({fp})")
                    for i, fid in enumerate(target_form_ids):
                        params[f'fid{i}'] = fid

            # --- Process each criterion as an AND condition ---
            display_field_to_use = 'DOCNAME'
            for idx, crit in enumerate(criteria):
                field_name = crit.get('field_name', '')
                keyword = crit.get('keyword', '')
                match_type = crit.get('match_type', 'like')
                disp = crit.get('display_field', '')

                if idx == 0 and disp and re.match(r'^[A-Z0-9_]+$', disp.upper()):
                    display_field_to_use = disp

                # Resolve compound field names
                clean_field = field_name
                if ',' in field_name and ':' in field_name:
                    is_numeric = keyword and keyword.strip().replace('*', '').isdigit()
                    parts = field_name.split(',')
                    for p in parts:
                        p = p.strip()
                        if is_numeric and p.upper().startswith('NUM:'):
                            clean_field = p.split(':')[1]
                            break
                        elif not is_numeric and p.upper().startswith('STR:'):
                            clean_field = p.split(':')[1]
                            break
                    else:
                        first = parts[0].strip()
                        clean_field = first.split(':')[1] if ':' in first else first
                elif ':' in field_name:
                    clean_field = field_name.split(':')[1]

                is_fulltext = clean_field.upper() == 'FULLTEXT_CONTENT'
                if not is_fulltext and not re.match(r'^[A-Z0-9_]+$', clean_field.upper()):
                    continue  # Skip invalid field names

                if keyword:
                    kw_param = f'kw{idx}'
                    check_kw = keyword.strip().upper()
                    try:
                        int(check_kw.replace('*', '').replace('%', ''))
                        check_kw = check_kw.replace('*', '').replace('%', '')
                    except ValueError:
                        pass

                    if is_fulltext:
                        where_clauses.append(
                            f"(UPPER(p.DOCNAME) LIKE :{kw_param} OR UPPER(p.ABSTRACT) LIKE :{kw_param})"
                        )
                        params[kw_param] = f"%{check_kw}%"
                    elif match_type == 'exact':
                        where_clauses.append(f"UPPER(p.{clean_field}) = :{kw_param}")
                        params[kw_param] = check_kw
                    elif match_type == 'startsWith':
                        where_clauses.append(f"UPPER(p.{clean_field}) LIKE :{kw_param}")
                        params[kw_param] = f"{check_kw}%"
                    else:
                        if '%' not in check_kw:
                            check_kw = f"%{check_kw}%"
                        where_clauses.append(f"UPPER(p.{clean_field}) LIKE :{kw_param}")
                        params[kw_param] = check_kw

            # --- Date range (global) ---
            if date_from and date_to:
                where_clauses.append("p.CREATION_DATE BETWEEN TO_DATE(:d_from, 'YYYY-MM-DD') AND TO_DATE(:d_to, 'YYYY-MM-DD')")
                params['d_from'] = date_from
                params['d_to'] = date_to
            elif date_from:
                where_clauses.append("p.CREATION_DATE >= TO_DATE(:d_from, 'YYYY-MM-DD')")
                params['d_from'] = date_from
            elif date_to:
                where_clauses.append("p.CREATION_DATE <= TO_DATE(:d_to, 'YYYY-MM-DD')")
                params['d_to'] = date_to

            if not where_clauses:
                return [], 0

            # --- Execute ---
            offset = (page - 1) * page_size
            params['offset'] = offset
            params['page_size'] = page_size

            sql = f"""
            SELECT /*+ FIRST_ROWS({page_size}) */
                p.DOCNUMBER, p.ABSTRACT, p.DOCNAME, p.CREATION_DATE, p.TYPIST,
                a.DEFAULT_EXTENSION, p.{display_field_to_use} as DISPLAY_VAL
            FROM PROFILE p
            LEFT JOIN APPS a ON p.APPLICATION = a.SYSTEM_ID
            WHERE {' AND '.join(where_clauses)}
            ORDER BY p.CREATION_DATE DESC
            OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY
            """

            try:
                await cursor.execute(sql, params)
            except oracledb.Error as col_err:
                if 'ORA-00904' in str(col_err):
                    logging.warning(f"Column error in multi-search: {col_err}. Retrying with DOCNAME.")
                    sql = sql.replace(f"p.{display_field_to_use} as DISPLAY_VAL", "p.DOCNAME as DISPLAY_VAL")
                    display_field_to_use = 'DOCNAME'
                    await cursor.execute(sql, params)
                else:
                    raise

            rows = await cursor.fetchall()

            documents = []
            for row in rows:
                doc_id, abstract, docname, date, typist, ext, display_val = row
                date_str = date.strftime('%Y-%m-%d') if date else ""
                media_type = 'unknown'
                ext_str = (str(ext) if ext else "").lower().replace('.', '')
                if ext_str in ['pdf']: media_type = 'pdf'
                elif ext_str in ['doc', 'docx']: media_type = 'word'
                elif ext_str in ['xls', 'xlsx']: media_type = 'excel'
                elif ext_str in ['ppt', 'pptx']: media_type = 'powerpoint'
                elif ext_str in ['zip', 'rar', '7z', 'tar', 'gz']: media_type = 'zip'
                elif ext_str in ['jpg', 'png', 'jpeg', 'tif', 'tiff']: media_type = 'image'
                elif ext_str in ['msg', 'eml']: media_type = 'email'
                elif ext_str in ['mp4', 'avi']: media_type = 'video'

                documents.append({
                    "doc_id": doc_id,
                    "title": abstract or docname,
                    "docname": docname,
                    "date": date_str,
                    "typist": typist,
                    "media_type": media_type,
                    "extension": ext_str,
                    "display_value": display_val,
                    "thumbnail_url": f"temp_thumbnail/{doc_id}" if media_type in ['image', 'video', 'pdf'] else ""
                })

            fetched_count = len(documents)
            if fetched_count == page_size:
                total_rows = (page * page_size) + 20
            else:
                total_rows = (page - 1) * page_size + fetched_count

            return documents, total_rows

    except oracledb.Error as e:
        logging.error(f"Oracle error in search_documents_multi: {e}")
        return [], 0
    finally:
        await conn.close()
