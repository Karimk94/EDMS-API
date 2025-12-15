import oracledb
import logging
import re
from database.connection import get_connection, BLOCKLIST

try:
    import vector_client
except ImportError:
    vector_client = None

def add_person_to_lkp(person_name_english, person_name_arabic=None):
    """Adds a new person to the LKP_PERSON lookup table."""
    conn = get_connection()
    if not conn: return False, "Could not connect to the database."
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(SYSTEM_ID) FROM LKP_PERSON WHERE NAME_ENGLISH = :1", [person_name_english])
            if cursor.fetchone()[0] > 0:
                return True, f"'{person_name_english}' already exists in LKP_PERSON."

            insert_query = """
                INSERT INTO LKP_PERSON (NAME_ENGLISH, NAME_ARABIC, LAST_UPDATE, DISABLED, SYSTEM_ID)
                VALUES (:1, :2, SYSDATE, 0, (SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_PERSON))
            """
            cursor.execute(insert_query, [person_name_english, person_name_arabic])
            conn.commit()
            return True, f"Successfully added '{person_name_english}' to LKP_PERSON."
    except oracledb.Error as e:
        return False, f"Database error: {e}"
    finally:
        if conn:
            conn.close()

def fetch_lkp_persons(page=1, page_size=20, search='', lang='en'):
    """Fetches a paginated list of people from the LKP_PERSON table."""
    conn = get_connection()
    if not conn: return [], 0

    offset = (page - 1) * page_size
    persons = []
    total_rows = 0

    search_term_upper = f"%{search.upper()}%"
    search_term_normal = f"%{search}%"

    # Search both English and Arabic columns regardless of lang
    search_clause = "WHERE (UPPER(NAME_ENGLISH) LIKE :search_upper OR NAME_ARABIC LIKE :search_normal)"
    params = {'search_upper': search_term_upper, 'search_normal': search_term_normal}

    count_query = f"SELECT COUNT(SYSTEM_ID) FROM LKP_PERSON {search_clause}"

    # Determine sort order
    order_by_column = "NAME_ENGLISH" if lang == 'en' else "NAME_ARABIC"

    fetch_query = f"""
        SELECT SYSTEM_ID, NAME_ENGLISH, NVL(NAME_ARABIC, '')
        FROM LKP_PERSON
        {search_clause}
        ORDER BY {order_by_column}
        OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY
    """

    params_paginated = params.copy()
    params_paginated['offset'] = offset
    params_paginated['page_size'] = page_size

    try:
        with conn.cursor() as cursor:
            cursor.execute(count_query, params)
            total_rows = cursor.fetchone()[0]

            cursor.execute(fetch_query, params_paginated)
            for row in cursor:
                # The frontend will construct the label. Send all data.
                persons.append({"id": row[0], "name_english": row[1], "name_arabic": row[2]})
    except oracledb.Error as e:
        print(f"❌ Oracle Database error in fetch_lkp_persons: {e}")
    finally:
        if conn:
            conn.close()
    return persons, total_rows

def fetch_all_tags(lang='en', security_level='Editor', app_source='unknown'):
    """Fetches all unique tags (keywords and persons) considering security level and app source visibility."""
    conn = get_connection()
    if not conn: return []

    # --- Dynamic Filtering Logic (Same as fetch_documents) ---
    range_start = 19677386
    range_end = 19679115
    smart_edms_floor = 19662092

    # Default filter (fallback)
    doc_filter_sql = f"p.DOCNUMBER >= {range_start}"

    if app_source == 'edms-media':
        doc_filter_sql = f"p.DOCNUMBER BETWEEN {range_start} AND {range_end}"
    elif app_source == 'smart-edms':
        doc_filter_sql = f"p.DOCNUMBER >= {smart_edms_floor} AND (p.DOCNUMBER < {range_start} OR p.DOCNUMBER > {range_end})"

    # -------------------------------

    tags = set()
    try:
        with conn.cursor() as cursor:
            keyword_column = "k.DESCRIPTION" if lang == 'ar' else "k.KEYWORD_ID"
            person_column = "p.NAME_ARABIC" if lang == 'ar' else "p.NAME_ENGLISH"

            shortlist_clause = "AND k.SHORTLISTED = '1'" if security_level == 'Viewer' else ""

            keyword_query = f"""
                SELECT DISTINCT {keyword_column} 
                FROM KEYWORD k 
                JOIN LKP_DOCUMENT_TAGS ldt ON ldt.TAG_ID = k.SYSTEM_ID 
                JOIN PROFILE p ON ldt.DOCNUMBER = p.DOCNUMBER
                WHERE {keyword_column} IS NOT NULL 
                {shortlist_clause}
                AND p.FORM = 2740
                AND {doc_filter_sql}
            """
            cursor.execute(keyword_query)
            for row in cursor:
                if row[0]:
                    tags.add(row[0].strip())

            person_query = f"""
                SELECT DISTINCT {person_column} 
                FROM LKP_PERSON p 
                WHERE {person_column} IS NOT NULL
                AND EXISTS (
                    SELECT 1 FROM PROFILE pr
                    WHERE pr.FORM = 2740
                    AND {doc_filter_sql.replace('p.', 'pr.')}
                    AND (
                        UPPER(pr.ABSTRACT) LIKE '%' || UPPER(p.NAME_ENGLISH) || '%'
                        OR 
                        (p.NAME_ARABIC IS NOT NULL AND pr.ABSTRACT LIKE '%' || p.NAME_ARABIC || '%')
                    )
                )
            """
            cursor.execute(person_query)
            for row in cursor:
                if row[0]:
                    tags.add(row[0].strip())
    except oracledb.Error as e:
        print(f"❌ Oracle Database error in fetch_all_tags: {e}")
    finally:
        if conn:
            conn.close()

    return sorted(list(tags))

def fetch_tags_for_document(doc_id, lang='en', security_level='Editor'):
    """
    Fetches all keyword and person tags for a single document.
    Returns a list of dictionaries: {'text': 'TagName', 'shortlisted': 1/0, 'type': 'keyword'/'person'}
    """
    conn = get_connection()
    if not conn:
        return []

    doc_tags = []
    seen_tags = set()

    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT ABSTRACT FROM PROFILE WHERE DOCNUMBER = :doc_id", {'doc_id': doc_id})
            result = cursor.fetchone()
            abstract = result[0] if result else None

            keyword_column = "k.DESCRIPTION" if lang == 'ar' else "k.KEYWORD_ID"
            person_column = "p.NAME_ARABIC" if lang == 'ar' else "p.NAME_ENGLISH"

            shortlist_clause = "AND k.SHORTLISTED = '1'" if security_level == 'Viewer' else ""

            tag_query = f"""
                SELECT {keyword_column}, k.SHORTLISTED
                FROM LKP_DOCUMENT_TAGS ldt
                JOIN KEYWORD k ON ldt.TAG_ID = k.SYSTEM_ID
                WHERE ldt.DOCNUMBER = :doc_id AND {keyword_column} IS NOT NULL {shortlist_clause}
            """
            cursor.execute(tag_query, {'doc_id': doc_id})
            for row in cursor:
                tag_text = row[0].strip() if row[0] else ""
                is_shortlisted = row[1] if row[1] else '0'

                if tag_text and tag_text not in seen_tags:
                    seen_tags.add(tag_text)
                    doc_tags.append({
                        'text': tag_text,
                        'shortlisted': 1 if str(is_shortlisted) == '1' else 0,
                        'type': 'keyword'
                    })

            if abstract:
                person_query = f"""
                    SELECT {person_column}
                    FROM LKP_PERSON p
                    WHERE :abstract LIKE '%' || UPPER(NAME_ENGLISH) || '%' AND {person_column} IS NOT NULL
                """
                cursor.execute(person_query, {'abstract': abstract.upper()})
                for row in cursor:
                    person_name = row[0].strip() if row[0] else ""
                    if person_name and person_name not in seen_tags:
                        seen_tags.add(person_name)
                        doc_tags.append({
                            'text': person_name,
                            'shortlisted': 0,
                            'type': 'person'
                        })

    except oracledb.Error as e:
        print(f"❌ Oracle Database error in fetch_tags_for_document for doc_id {doc_id}: {e}")
    finally:
        if conn:
            conn.close()

    # Sort by text
    return sorted(doc_tags, key=lambda x: x['text'].lower())

def toggle_tag_shortlist(tag, lang='en'):
    """
    Toggles the SHORTLISTED status of a keyword (0 -> 1 or 1 -> 0).
    Identifies the keyword by name (English or Arabic).
    """
    conn = get_connection()
    if not conn: return False, "Database connection failed."

    try:
        with conn.cursor() as cursor:
            # 1. Find the Keyword ID and current status
            # Check both English ID and Arabic Description
            cursor.execute("""
                           SELECT SYSTEM_ID, SHORTLISTED
                           FROM KEYWORD
                           WHERE UPPER(KEYWORD_ID) = :tag_upper
                              OR DESCRIPTION = :tag_normal
                           """, tag_upper=tag.upper(), tag_normal=tag)

            result = cursor.fetchone()
            if not result:
                return False, "Tag not found in keywords (cannot shortlist Persons)."

            keyword_id = result[0]
            current_status = str(result[1]) if result[1] else '0'

            # Toggle status
            new_status = '0' if current_status == '1' else '1'

            # 2. Update the keyword
            cursor.execute("UPDATE KEYWORD SET SHORTLISTED = :new_status WHERE SYSTEM_ID = :id",
                           new_status=new_status, id=keyword_id)
            conn.commit()

            return True, {"new_status": int(new_status)}

    except oracledb.Error as e:
        if conn: conn.rollback()
        logging.error(f"Database error toggling shortlist: {e}", exc_info=True)
        return False, f"Database error: {e}"
    finally:
        if conn: conn.close()

def insert_keywords_and_tags(docnumber, keywords):
    """
    Inserts keywords and links them to a document. Handles duplicates gracefully.
    'keywords' is a list of dictionaries, with each dictionary having 'english' and 'arabic' keys.
    """
    conn = get_connection()
    if not conn:
        logging.error(f"DB_KEYWORD_FAILURE: Could not get a database connection for docnumber {docnumber}.")
        return

    try:
        with conn.cursor() as cursor:
            processed_keywords = set()

            for keyword in keywords:
                english_keyword_orig = keyword.get('english')
                arabic_keyword = keyword.get('arabic')

                if not english_keyword_orig or not arabic_keyword:
                    continue

                # --- Keyword Validation ---
                if len(english_keyword_orig.strip()) < 2:
                    logging.warning(f"Skipping short keyword '{english_keyword_orig}' for docnumber {docnumber}.")
                    continue

                if ' ' not in english_keyword_orig and english_keyword_orig.lower() in BLOCKLIST.get('meaningless',
                                                                                                     set()):
                    logging.warning(f"Skipping meaningless keyword '{english_keyword_orig}' for docnumber {docnumber}.")
                    continue

                english_keyword = english_keyword_orig.lower()

                if english_keyword in processed_keywords:
                    continue

                if len(english_keyword) > 30:
                    logging.warning(
                        f"Skipping keyword '{english_keyword_orig}' for docnumber {docnumber} because its length ({len(english_keyword_orig)}) exceeds the 30-character limit.")
                    continue

                keyword_system_id = None

                cursor.execute("SELECT SYSTEM_ID FROM KEYWORD WHERE UPPER(KEYWORD_ID) = UPPER(:keyword_id)",
                               keyword_id=english_keyword)
                result = cursor.fetchone()

                if result:
                    keyword_system_id = result[0]
                else:
                    try:
                        cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM KEYWORD")
                        keyword_system_id = cursor.fetchone()[0]

                        cursor.execute("""
                            INSERT INTO KEYWORD (KEYWORD_ID, DESCRIPTION, SYSTEM_ID)
                            VALUES (:keyword_id, :description, :system_id)
                        """, keyword_id=english_keyword, description=arabic_keyword,
                                       system_id=keyword_system_id)

                    except oracledb.IntegrityError as ie:
                        error, = ie.args
                        if "ORA-00001" in error.message:
                            logging.warning(
                                f"Keyword '{english_keyword}' was inserted by another process. Fetching existing ID.")
                            cursor.execute("SELECT SYSTEM_ID FROM KEYWORD WHERE KEYWORD_ID = :keyword_id",
                                           keyword_id=english_keyword)
                            result = cursor.fetchone()
                            if result:
                                keyword_system_id = result[0]
                            else:
                                logging.error(
                                    f"Failed to fetch SYSTEM_ID for '{english_keyword}' after integrity error.")
                                continue
                        else:
                            raise

                if keyword_system_id:
                    cursor.execute(
                        "SELECT COUNT(*) FROM LKP_DOCUMENT_TAGS WHERE DOCNUMBER = :docnumber AND TAG_ID = :tag_id",
                        docnumber=docnumber, tag_id=keyword_system_id)

                    if cursor.fetchone()[0] == 0:
                        cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_DOCUMENT_TAGS")
                        lkp_system_id = cursor.fetchone()[0]
                        cursor.execute("""
                            INSERT INTO LKP_DOCUMENT_TAGS (DOCNUMBER, TAG_ID, SYSTEM_ID, LAST_UPDATE, DISABLED)
                            VALUES (:docnumber, :tag_id, :system_id, SYSDATE, 0)
                        """, docnumber=docnumber, tag_id=keyword_system_id, system_id=lkp_system_id)

                processed_keywords.add(english_keyword)

            conn.commit()
            # logging.info(f"DB_KEYWORD_SUCCESS: Successfully processed keywords for docnumber {docnumber}.")

    except oracledb.Error as e:
        logging.error(f"DB_KEYWORD_ERROR: Oracle error while processing keywords for docnumber {docnumber}: {e}",
                      exc_info=True)
        try:
            conn.rollback()
            # logging.info(f"DB_ROLLBACK: Transaction for docnumber {docnumber} keywords was rolled back.")
        except oracledb.Error as rb_e:
            logging.error(
                f"DB_ROLLBACK_ERROR: Failed to rollback transaction for docnumber {docnumber} keywords: {rb_e}",
                exc_info=True)

    finally:
        if conn:
            conn.close()

def add_tag_to_document(doc_id, tag):
    """Adds a new tag to a document, handling existing keywords and validating the tag."""
    # --- Tag Validation ---
    if not tag or len(tag.strip()) < 2:
        return False, "Tag cannot be empty or less than 2 characters."

    # If the tag is a single word, check if it's in the blocklist
    if ' ' not in tag and tag.lower() in BLOCKLIST.get('meaningless', set()):
        return False, f"Tag '{tag}' is a meaningless word and cannot be added."

    conn = get_connection()
    if not conn:
        return False, "Could not connect to the database."
    try:
        with conn.cursor() as cursor:
            # Check if the keyword already exists
            cursor.execute("SELECT SYSTEM_ID FROM KEYWORD WHERE KEYWORD_ID = :1", [tag.lower()])
            result = cursor.fetchone()
            if result:
                keyword_id = result[0]
            else:
                # Insert new keyword if it doesn't exist
                cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM KEYWORD")
                keyword_id = cursor.fetchone()[0]
                cursor.execute("INSERT INTO KEYWORD (KEYWORD_ID, SYSTEM_ID) VALUES (:1, :2)",
                               [tag.lower(), keyword_id])

            # Check if the document is already tagged with this keyword
            cursor.execute("SELECT COUNT(*) FROM LKP_DOCUMENT_TAGS WHERE DOCNUMBER = :1 AND TAG_ID = :2",
                           [doc_id, keyword_id])
            if cursor.fetchone()[0] > 0:
                return True, "Tag already exists on this document."

            # Link the keyword to the document
            cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_DOCUMENT_TAGS")
            lkp_system_id = cursor.fetchone()[0]
            cursor.execute("""
                INSERT INTO LKP_DOCUMENT_TAGS (DOCNUMBER, TAG_ID, SYSTEM_ID, LAST_UPDATE, DISABLED)
                VALUES (:docnumber, :tag_id, :system_id, SYSDATE, 0)
            """, docnumber=doc_id, tag_id=keyword_id, system_id=lkp_system_id)
            conn.commit()
            return True, "Tag added successfully."
    except oracledb.Error as e:
        conn.rollback()
        return False, f"Database error: {e}"
    finally:
        if conn:
            conn.close()

def update_tag_for_document(doc_id, old_tag, new_tag):
    """Updates a tag for a document."""
    # --- Tag Validation ---
    if not new_tag or len(new_tag.strip()) < 2:
        return False, "New tag cannot be empty or less than 2 characters."

    if ' ' not in new_tag and new_tag.lower() in BLOCKLIST.get('meaningless', set()):
        return False, f"Tag '{new_tag}' is a meaningless word and cannot be added."

    conn = get_connection()
    if not conn:
        return False, "Could not connect to the database."
    try:
        with conn.cursor() as cursor:
            # Find the old keyword ID
            cursor.execute("SELECT SYSTEM_ID FROM KEYWORD WHERE KEYWORD_ID = :1", [old_tag.lower()])
            result = cursor.fetchone()
            if not result:
                return False, "Old tag not found."
            old_keyword_id = result[0]

            # Check if the new keyword exists
            cursor.execute("SELECT SYSTEM_ID FROM KEYWORD WHERE KEYWORD_ID = :1", [new_tag.lower()])
            result = cursor.fetchone()
            if result:
                new_keyword_id = result[0]
            else:
                # Insert new keyword
                cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM KEYWORD")
                new_keyword_id = cursor.fetchone()[0]
                cursor.execute("INSERT INTO KEYWORD (KEYWORD_ID, SYSTEM_ID) VALUES (:1, :2)",
                               [new_tag.lower(), new_keyword_id])

            # Update the link in LKP_DOCUMENT_TAGS
            cursor.execute("""
                UPDATE LKP_DOCUMENT_TAGS
                SET TAG_ID = :new_tag_id
                WHERE DOCNUMBER = :doc_id AND TAG_ID = :old_tag_id
            """, new_tag_id=new_keyword_id, doc_id=doc_id, old_tag_id=old_keyword_id)
            conn.commit()
            return True, "Tag updated successfully."
    except oracledb.Error as e:
        return False, f"Database error: {e}"
    finally:
        if conn:
            conn.close()

def delete_tag_from_document(doc_id, tag):
    """Deletes a tag from a document. The tag can be a keyword or a person's name."""
    conn = get_connection()
    if not conn:
        return False, "Could not connect to the database."

    try:
        with conn.cursor() as cursor:
            # --- 1. Check if the tag is a KEYWORD (English or Arabic) ---
            # Checks KEYWORD_ID (English, case-insensitive) OR DESCRIPTION (Arabic, case-sensitive)
            cursor.execute("""
                SELECT SYSTEM_ID 
                FROM KEYWORD 
                WHERE UPPER(KEYWORD_ID) = :tag_upper OR DESCRIPTION = :tag_normal
            """, tag_upper=tag.upper(), tag_normal=tag)

            keyword_result = cursor.fetchone()

            if keyword_result:
                keyword_id = keyword_result[0]
                # Delete the link from LKP_DOCUMENT_TAGS
                cursor.execute("""
                    DELETE FROM LKP_DOCUMENT_TAGS
                    WHERE DOCNUMBER = :doc_id AND TAG_ID = :tag_id
                """, doc_id=doc_id, tag_id=keyword_id)

                # Check if any rows were deleted
                if cursor.rowcount > 0:
                    conn.commit()
                    return True, "Tag deleted successfully."

            # --- 2. If not a keyword, check if it's a PERSON (English or Arabic) ---
            # Checks NAME_ENGLISH (case-insensitive) OR NAME_ARABIC (case-sensitive)
            cursor.execute("""
                SELECT NAME_ENGLISH 
                FROM LKP_PERSON 
                WHERE UPPER(NAME_ENGLISH) = :tag_upper OR NAME_ARABIC = :tag_normal
            """, tag_upper=tag.upper(), tag_normal=tag)

            person_result = cursor.fetchone()

            if person_result:
                # It's a person, so we need to modify the abstract.
                cursor.execute("SELECT ABSTRACT FROM PROFILE WHERE DOCNUMBER = :1", [doc_id])
                abstract_result = cursor.fetchone()
                if not abstract_result or not abstract_result[0]:
                    return False, "Document abstract not found or is empty."

                current_abstract = abstract_result[0]
                # Find the VIPs section
                vips_match = re.search(r'VIPs\s*:\s*(.*)', current_abstract, re.IGNORECASE)
                if vips_match:
                    vips_str = vips_match.group(1)
                    vips_list = [name.strip() for name in vips_str.split(',')]

                    # Remove the person from the list (case-insensitive for both English and Arabic)
                    # .upper() works for Arabic (returns the same string) and English
                    original_len = len(vips_list)
                    tag_upper = tag.upper()
                    vips_list = [name for name in vips_list if name.upper() != tag_upper]

                    if len(vips_list) < original_len:
                        # Reconstruct the abstract
                        if vips_list:
                            new_vips_str = "VIPs: " + ", ".join(vips_list)
                            # Use regex sub to replace the old vips_match group 0
                            new_abstract = re.sub(re.escape(vips_match.group(0)), new_vips_str, current_abstract,
                                                  flags=re.IGNORECASE)
                        else:
                            # If no VIPs are left, remove the entire VIPs line
                            new_abstract = re.sub(r'\s*\n*VIPs\s*:.*', '', current_abstract,
                                                  flags=re.IGNORECASE).strip()

                        cursor.execute("UPDATE PROFILE SET ABSTRACT = :1 WHERE DOCNUMBER = :2", [new_abstract, doc_id])
                        conn.commit()

                        if vector_client:
                            try:
                                vector_client.add_or_update_document(doc_id, new_abstract)
                            except Exception as e:
                                logging.error(
                                    f"Failed to update vector index for doc_id {doc_id} after tag delete: {e}",
                                    exc_info=True)

                        return True, "Person tag removed from abstract successfully."

            # --- 3. If not found as keyword or person ---
            conn.commit()
            return False, f"Tag '{tag}' not found for this document."

    except oracledb.Error as e:
        conn.rollback()
        return False, f"Database error: {e}"
    finally:
        if conn:
            conn.close()