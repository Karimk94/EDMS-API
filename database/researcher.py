import logging
import oracledb
from database.connection import get_async_connection
from datetime import datetime

async def fetch_search_types(user_id):
    """
    Returns a list of search types based on available PROFILE columns.
    Hardcoded as LKP_MAIN_FIELDS table is missing.
    """
    # Mapping of Label -> Column Name
    # Based on user-provided PROFILE table schema
    # The legacy app uses FORM_NAME from FORMS table as the 'FORM' parameter in DDL.
    # We need to ensure we use a valid FORM ID or Name that the PROFILE table recognizes.
    # In search_documents, we filter by p.FORM = :form_id. 
    # If 2740 is the ID for 'DEF_CORR_OUT', we are good.
    # Let's add a dynamic lookup for the default form 'DEF_CORR_OUT' or use the most common one.
    
    conn = await get_async_connection()
    if not conn:
        return []
        
    default_form_id = "2740" # Verified: Contains "metro" documents (legacy target)
    try:
        async with conn.cursor() as cursor:
            # Try to get ID for DEF_CORR_OUT just in case
            await cursor.execute("SELECT SYSTEM_ID FROM FORMS WHERE UPPER(FORM_NAME) = 'DEF_CORR_OUT'")
            row = await cursor.fetchone()
            if row:
                # If DEF_CORR_OUT is found, use it, otherwise fallback to 2740
                default_form_id = str(row[0])
    except Exception:
        pass
    finally:
        await conn.close()

    hardcoded_types = [
        # (Label, Field, Exact Match, Form ID)
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
    
    # We'll use a dummy ID counter
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
    Optimized for performance:
    - Skips explicit COUNT(*) for keyword searches (too slow on 7M+ rows).
    - Uses FIRST_ROWS hint.
    - Limits max results.
    """

    conn = await get_async_connection()
    if not conn:
        return [], 0

    try:
        async with conn.cursor() as cursor:
            # Default to Form 2740 (standard form with "metro" docs) or lookup if needed.
            target_form_id = 2740
            
            # Try to look up form_name if provided
            if form_name:
                 if str(form_name) == '0' or str(form_name).upper() == 'ALL':
                     target_form_id = None
                 else:
                     form_id_sql = "SELECT SYSTEM_ID FROM FORMS WHERE UPPER(FORM_NAME) = UPPER(:form_name)"
                     await cursor.execute(form_id_sql, {'form_name': form_name})
                     form_row = await cursor.fetchone()
                     if form_row:
                         target_form_id = form_row[0]
            
            # Sanitize field_name to prevent injection
            import re
            if not re.match(r'^[A-Z0-9_]+$', field_name.upper()) and field_name != '*':
                 logging.warning(f"Invalid field_name detected: {field_name}")
                 return [], 0

            # Handling "NUM:" or "STR:" prefixes if they exist
            clean_field_name = field_name
            if ',' in field_name:
                 parts = field_name.split(',')
                 for p in parts:
                     if ':' in p:
                         clean_field_name = p.split(':')[1]
                         break
                     else:
                         clean_field_name = p
                         break
            elif ':' in field_name:
                clean_field_name = field_name.split(':')[1]

            where_clauses = []
            params = {}
            if target_form_id is not None:
                where_clauses.append("p.FORM = :form_id")
                params['form_id'] = target_form_id

            if keyword:
                check_keyword = keyword.replace('*', '%')
                
                if match_type == 'exact':
                    clean_keyword = check_keyword.replace('%', '')
                    where_clauses.append(f"UPPER(p.{clean_field_name}) = :keyword")
                    params['keyword'] = clean_keyword.upper()
                elif match_type == 'startsWith':
                    clean_keyword = check_keyword.replace('%', '')
                    where_clauses.append(f"UPPER(p.{clean_field_name}) LIKE :keyword")
                    params['keyword'] = f"{clean_keyword.upper()}%"
                else: # 'like'
                    # Always wrap in wildcards for "Contains" if they aren't already there
                    text_fields = {'DOCNAME', 'ABSTRACT', 'PD_ORIGINATOR', 'PD_ADDRESSEE'}
                    
                    if '%' not in check_keyword:
                        check_keyword = f"%{check_keyword}%"
                        
                    where_clauses.append(f"UPPER(p.{clean_field_name}) LIKE :keyword")
                    params['keyword'] = check_keyword.upper()

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

            # Pagination
            offset = (page - 1) * page_size
            
            clean_display_field = display_field if display_field and re.match(r'^[A-Z0-9_]+$', display_field.upper()) else 'DOCNAME'
            
            # Data query - Optimized
            # We skip total count. It's too slow.
            # We return a dummy count (e.g., page * page_size + 1 if rows found, or just enough to enable 'Next')
            
            # Use CREATION_DATE for ordering as it is indexed (PROFILE00).
            # RTADOCDATE was not indexed, causing full table scans for sorting.
            
            sql = f"""
            SELECT /*+ FIRST_ROWS({page_size}) */
                p.DOCNUMBER, 
                p.ABSTRACT, 
                p.DOCNAME, 
                p.CREATION_DATE, 
                p.TYPIST,
                a.DEFAULT_EXTENSION,
                p.{clean_display_field} as DISPLAY_VAL
            FROM PROFILE p
            LEFT JOIN APPS a ON p.APPLICATION = a.SYSTEM_ID
            WHERE {' AND '.join(where_clauses)}
            ORDER BY p.CREATION_DATE DESC
            OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY
            """
            
            params['offset'] = offset
            params['page_size'] = page_size
            
            await cursor.execute(sql, params)
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

            # Create a fake total to enable pagination (current page items + more if full page)
            # If we got a full page, assume there's at least one more page.
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

