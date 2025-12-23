import oracledb
import logging
import re
from database.connection import get_async_connection, BLOCKLIST

try:
    import vector_client
except ImportError:
    vector_client = None

async def add_person_to_lkp(person_name_english, person_name_arabic=None):
    """Adds a new person to the LKP_PERSON lookup table."""
    conn = await get_async_connection()
    if not conn: return False, "Could not connect to the database."
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT COUNT(SYSTEM_ID) FROM LKP_PERSON WHERE NAME_ENGLISH = :1", [person_name_english])
            res = await cursor.fetchone()
            if res[0] > 0:
                return True, f"'{person_name_english}' already exists in LKP_PERSON."

            insert_query = """
                INSERT INTO LKP_PERSON (NAME_ENGLISH, NAME_ARABIC, LAST_UPDATE, DISABLED, SYSTEM_ID)
                VALUES (:1, :2, SYSDATE, 0, (SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_PERSON))
            """
            await cursor.execute(insert_query, [person_name_english, person_name_arabic])
            await conn.commit()
            return True, f"Successfully added '{person_name_english}' to LKP_PERSON."
    except oracledb.Error as e:
        if conn: await conn.rollback()
        return False, f"Database error: {e}"
    finally:
        if conn:
            await conn.close()

async def fetch_lkp_persons(page=1, page_size=20, search='', lang='en'):
    """Fetches a paginated list of people from the LKP_PERSON table."""
    conn = await get_async_connection()
    if not conn: return [], 0

    offset = (page - 1) * page_size
    persons = []
    total_rows = 0

    search_term_upper = f"%{search.upper()}%"
    search_term_normal = f"%{search}%"

    search_clause = "WHERE (UPPER(NAME_ENGLISH) LIKE :search_upper OR NAME_ARABIC LIKE :search_normal)"
    params = {'search_upper': search_term_upper, 'search_normal': search_term_normal}

    count_query = f"SELECT COUNT(SYSTEM_ID) FROM LKP_PERSON {search_clause}"
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
        async with conn.cursor() as cursor:
            await cursor.execute(count_query, params)
            count_res = await cursor.fetchone()
            total_rows = count_res[0]

            await cursor.execute(fetch_query, params_paginated)
            async for row in cursor:
                persons.append({"id": row[0], "name_english": row[1], "name_arabic": row[2]})
    except oracledb.Error as e:
        print(f"❌ Oracle Database error in fetch_lkp_persons: {e}")
    finally:
        if conn:
            await conn.close()
    return persons, total_rows

async def fetch_all_tags(lang='en', security_level='Editor', app_source='unknown'):
    """Fetches all unique tags (keywords and persons) considering security level and app source visibility."""
    conn = await get_async_connection()
    if not conn: return []

    doc_filter_sql = "p.RTA_TEXT1 = 'edms-media'"

    if app_source == 'edms-media':
        doc_filter_sql = "p.RTA_TEXT1 = 'edms-media'"
    elif app_source == 'smart-edms':
        smart_edms_floor = 19662092
        doc_filter_sql = f"p.DOCNUMBER >= {smart_edms_floor} AND (p.RTA_TEXT1 IS NULL OR p.RTA_TEXT1 != 'edms-media')"

    tags = set()
    try:
        async with conn.cursor() as cursor:
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
            await cursor.execute(keyword_query)
            async for row in cursor:
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
            await cursor.execute(person_query)
            async for row in cursor:
                if row[0]:
                    tags.add(row[0].strip())
    except oracledb.Error as e:
        print(f"❌ Oracle Database error in fetch_all_tags: {e}")
    finally:
        if conn:
            await conn.close()

    return sorted(list(tags))

async def fetch_tags_for_document(doc_id, lang='en', security_level='Editor'):
    """Fetches all keyword and person tags for a single document."""
    conn = await get_async_connection()
    if not conn: return []

    doc_tags = []
    seen_tags = set()

    try:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT ABSTRACT FROM PROFILE WHERE DOCNUMBER = :doc_id", {'doc_id': doc_id})
            result = await cursor.fetchone()
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
            await cursor.execute(tag_query, {'doc_id': doc_id})
            async for row in cursor:
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
                await cursor.execute(person_query, {'abstract': abstract.upper()})
                async for row in cursor:
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
            await conn.close()

    return sorted(doc_tags, key=lambda x: x['text'].lower())

async def toggle_tag_shortlist(tag, lang='en'):
    """Toggles the SHORTLISTED status of a keyword."""
    conn = await get_async_connection()
    if not conn: return False, "Database connection failed."

    try:
        async with conn.cursor() as cursor:
            await cursor.execute("""
                           SELECT SYSTEM_ID, SHORTLISTED
                           FROM KEYWORD
                           WHERE UPPER(KEYWORD_ID) = :tag_upper
                              OR DESCRIPTION = :tag_normal
                           """, tag_upper=tag.upper(), tag_normal=tag)

            result = await cursor.fetchone()
            if not result:
                return False, "Tag not found in keywords (cannot shortlist Persons)."

            keyword_id = result[0]
            current_status = str(result[1]) if result[1] else '0'
            new_status = '0' if current_status == '1' else '1'

            await cursor.execute("UPDATE KEYWORD SET SHORTLISTED = :new_status WHERE SYSTEM_ID = :id",
                           new_status=new_status, id=keyword_id)
            await conn.commit()

            return True, {"new_status": int(new_status)}

    except oracledb.Error as e:
        if conn: await conn.rollback()
        logging.error(f"Database error toggling shortlist: {e}", exc_info=True)
        return False, f"Database error: {e}"
    finally:
        if conn: await conn.close()

async def insert_keywords_and_tags(docnumber, keywords):
    """Inserts keywords and links them to a document."""
    conn = await get_async_connection()
    if not conn:
        logging.error(f"DB_KEYWORD_FAILURE: Could not get a database connection for docnumber {docnumber}.")
        return

    try:
        async with conn.cursor() as cursor:
            processed_keywords = set()

            for keyword in keywords:
                english_keyword_orig = keyword.get('english')
                arabic_keyword = keyword.get('arabic')

                if not english_keyword_orig or not arabic_keyword: continue
                if len(english_keyword_orig.strip()) < 2: continue
                if ' ' not in english_keyword_orig and english_keyword_orig.lower() in BLOCKLIST.get('meaningless', set()): continue

                english_keyword = english_keyword_orig.lower()
                if english_keyword in processed_keywords: continue
                if len(english_keyword) > 30: continue

                keyword_system_id = None
                await cursor.execute("SELECT SYSTEM_ID FROM KEYWORD WHERE UPPER(KEYWORD_ID) = UPPER(:keyword_id)",
                               keyword_id=english_keyword)
                result = await cursor.fetchone()

                if result:
                    keyword_system_id = result[0]
                else:
                    try:
                        await cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM KEYWORD")
                        row = await cursor.fetchone()
                        keyword_system_id = row[0]
                        await cursor.execute("""
                            INSERT INTO KEYWORD (KEYWORD_ID, DESCRIPTION, SYSTEM_ID)
                            VALUES (:keyword_id, :description, :system_id)
                        """, keyword_id=english_keyword, description=arabic_keyword, system_id=keyword_system_id)
                    except oracledb.IntegrityError:
                        await cursor.execute("SELECT SYSTEM_ID FROM KEYWORD WHERE KEYWORD_ID = :keyword_id", keyword_id=english_keyword)
                        result = await cursor.fetchone()
                        if result: keyword_system_id = result[0]

                if keyword_system_id:
                    await cursor.execute(
                        "SELECT COUNT(*) FROM LKP_DOCUMENT_TAGS WHERE DOCNUMBER = :docnumber AND TAG_ID = :tag_id",
                        docnumber=docnumber, tag_id=keyword_system_id)
                    cnt_res = await cursor.fetchone()
                    if cnt_res[0] == 0:
                        await cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_DOCUMENT_TAGS")
                        lkp_row = await cursor.fetchone()
                        lkp_system_id = lkp_row[0]
                        await cursor.execute("""
                            INSERT INTO LKP_DOCUMENT_TAGS (DOCNUMBER, TAG_ID, SYSTEM_ID, LAST_UPDATE, DISABLED)
                            VALUES (:docnumber, :tag_id, :system_id, SYSDATE, 0)
                        """, docnumber=docnumber, tag_id=keyword_system_id, system_id=lkp_system_id)

                processed_keywords.add(english_keyword)
            await conn.commit()

    except oracledb.Error as e:
        logging.error(f"DB_KEYWORD_ERROR: {e}", exc_info=True)
        if conn: await conn.rollback()
    finally:
        if conn:
            await conn.close()

async def add_tag_to_document(doc_id, tag):
    """Adds a new tag to a document."""
    if not tag or len(tag.strip()) < 2: return False, "Tag cannot be empty or less than 2 characters."
    if ' ' not in tag and tag.lower() in BLOCKLIST.get('meaningless', set()):
        return False, f"Tag '{tag}' is a meaningless word and cannot be added."

    conn = await get_async_connection()
    if not conn: return False, "Could not connect to the database."
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT SYSTEM_ID FROM KEYWORD WHERE KEYWORD_ID = :1", [tag.lower()])
            result = await cursor.fetchone()
            if result:
                keyword_id = result[0]
            else:
                await cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM KEYWORD")
                row = await cursor.fetchone()
                keyword_id = row[0]
                await cursor.execute("INSERT INTO KEYWORD (KEYWORD_ID, SYSTEM_ID) VALUES (:1, :2)", [tag.lower(), keyword_id])

            await cursor.execute("SELECT COUNT(*) FROM LKP_DOCUMENT_TAGS WHERE DOCNUMBER = :1 AND TAG_ID = :2", [doc_id, keyword_id])
            res = await cursor.fetchone()
            if res[0] > 0: return True, "Tag already exists on this document."

            await cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM LKP_DOCUMENT_TAGS")
            row = await cursor.fetchone()
            lkp_system_id = row[0]
            await cursor.execute("""
                INSERT INTO LKP_DOCUMENT_TAGS (DOCNUMBER, TAG_ID, SYSTEM_ID, LAST_UPDATE, DISABLED)
                VALUES (:docnumber, :tag_id, :system_id, SYSDATE, 0)
            """, docnumber=doc_id, tag_id=keyword_id, system_id=lkp_system_id)
            await conn.commit()
            return True, "Tag added successfully."
    except oracledb.Error as e:
        if conn: await conn.rollback()
        return False, f"Database error: {e}"
    finally:
        if conn: await conn.close()

async def update_tag_for_document(doc_id, old_tag, new_tag):
    """Updates a tag for a document."""
    if not new_tag or len(new_tag.strip()) < 2: return False, "New tag cannot be empty or less than 2 characters."
    if ' ' not in new_tag and new_tag.lower() in BLOCKLIST.get('meaningless', set()):
        return False, f"Tag '{new_tag}' is a meaningless word."

    conn = await get_async_connection()
    if not conn: return False, "Could not connect to the database."
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT SYSTEM_ID FROM KEYWORD WHERE KEYWORD_ID = :1", [old_tag.lower()])
            result = await cursor.fetchone()
            if not result: return False, "Old tag not found."
            old_keyword_id = result[0]

            await cursor.execute("SELECT SYSTEM_ID FROM KEYWORD WHERE KEYWORD_ID = :1", [new_tag.lower()])
            result = await cursor.fetchone()
            if result:
                new_keyword_id = result[0]
            else:
                await cursor.execute("SELECT NVL(MAX(SYSTEM_ID), 0) + 1 FROM KEYWORD")
                row = await cursor.fetchone()
                new_keyword_id = row[0]
                await cursor.execute("INSERT INTO KEYWORD (KEYWORD_ID, SYSTEM_ID) VALUES (:1, :2)", [new_tag.lower(), new_keyword_id])

            await cursor.execute("""
                UPDATE LKP_DOCUMENT_TAGS SET TAG_ID = :new_tag_id
                WHERE DOCNUMBER = :doc_id AND TAG_ID = :old_tag_id
            """, new_tag_id=new_keyword_id, doc_id=doc_id, old_tag_id=old_keyword_id)
            await conn.commit()
            return True, "Tag updated successfully."
    except oracledb.Error as e:
        if conn: await conn.rollback()
        return False, f"Database error: {e}"
    finally:
        if conn: await conn.close()

async def delete_tag_from_document(doc_id, tag):
    """Deletes a tag from a document."""
    conn = await get_async_connection()
    if not conn: return False, "Could not connect to the database."

    try:
        async with conn.cursor() as cursor:
            await cursor.execute("""
                SELECT SYSTEM_ID FROM KEYWORD 
                WHERE UPPER(KEYWORD_ID) = :tag_upper OR DESCRIPTION = :tag_normal
            """, tag_upper=tag.upper(), tag_normal=tag)

            keyword_result = await cursor.fetchone()

            if keyword_result:
                keyword_id = keyword_result[0]
                await cursor.execute("""
                    DELETE FROM LKP_DOCUMENT_TAGS
                    WHERE DOCNUMBER = :doc_id AND TAG_ID = :tag_id
                """, doc_id=doc_id, tag_id=keyword_id)

                if cursor.rowcount > 0:
                    await conn.commit()
                    return True, "Tag deleted successfully."

            await cursor.execute("""
                SELECT NAME_ENGLISH FROM LKP_PERSON 
                WHERE UPPER(NAME_ENGLISH) = :tag_upper OR NAME_ARABIC = :tag_normal
            """, tag_upper=tag.upper(), tag_normal=tag)

            person_result = await cursor.fetchone()

            if person_result:
                await cursor.execute("SELECT ABSTRACT FROM PROFILE WHERE DOCNUMBER = :1", [doc_id])
                abstract_result = await cursor.fetchone()
                if not abstract_result or not abstract_result[0]:
                    return False, "Document abstract not found or is empty."

                current_abstract = abstract_result[0]
                vips_match = re.search(r'VIPs\s*:\s*(.*)', current_abstract, re.IGNORECASE)
                if vips_match:
                    vips_str = vips_match.group(1)
                    vips_list = [name.strip() for name in vips_str.split(',')]
                    original_len = len(vips_list)
                    tag_upper = tag.upper()
                    vips_list = [name for name in vips_list if name.upper() != tag_upper]

                    if len(vips_list) < original_len:
                        if vips_list:
                            new_vips_str = "VIPs: " + ", ".join(vips_list)
                            new_abstract = re.sub(re.escape(vips_match.group(0)), new_vips_str, current_abstract, flags=re.IGNORECASE)
                        else:
                            new_abstract = re.sub(r'\s*\n*VIPs\s*:.*', '', current_abstract, flags=re.IGNORECASE).strip()

                        await cursor.execute("UPDATE PROFILE SET ABSTRACT = :1 WHERE DOCNUMBER = :2", [new_abstract, doc_id])
                        await conn.commit()

                        if vector_client:
                            try:
                                vector_client.add_or_update_document(doc_id, new_abstract)
                            except Exception as e:
                                logging.error(f"Failed to update vector index: {e}")

                        return True, "Person tag removed from abstract successfully."

            await conn.commit()
            return False, f"Tag '{tag}' not found for this document."

    except oracledb.Error as e:
        if conn: await conn.rollback()
        return False, f"Database error: {e}"
    finally:
        if conn: await conn.close()