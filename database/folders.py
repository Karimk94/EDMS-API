from .connection import get_async_connection
from .media import resolve_media_types_from_db
import logging
import oracledb

NODE_TYPE_FOLDER = 'F'
NODE_TYPE_DOCUMENT = 'D'

async def get_folder_by_docnumber(docnumber: str) -> dict | None:
    """
    Retrieves folder information by DOCNUMBER (PROFILE.DOCNUMBER).
    This is used when looking up shared folders since we share by DOCNUMBER.
    """
    conn = await get_async_connection()
    if not conn:
        return None

    try:
        async with conn.cursor() as cursor:
            # First get the folder info from FOLDER_ITEM by DOCNUMBER
            # Also get the folder name from PROFILE or DISPLAYNAME
            await cursor.execute(
                """SELECT fi.SYSTEM_ID, fi.DOCNUMBER, fi.DISPLAYNAME, fi.PARENT, p.DOCNAME, p.ABSTRACT
                   FROM FOLDER_ITEM fi
                   LEFT JOIN PROFILE p ON fi.DOCNUMBER = p.DOCNUMBER
                   WHERE fi.DOCNUMBER = :docnumber 
                   AND fi.NODE_TYPE = :node_type""",
                {'docnumber': docnumber, 'node_type': NODE_TYPE_FOLDER}
            )
            row = await cursor.fetchone()

            if row:
                system_id, doc_num, display_name, parent_system_id, docname, abstract = row
                # Prefer DISPLAYNAME, then DOCNAME from PROFILE
                folder_name = display_name or docname or f'Folder {doc_num}'

                return {
                    'system_id': system_id,
                    'docnumber': doc_num,
                    'id': str(doc_num),  # Use DOCNUMBER as the ID
                    'name': folder_name,
                    'parent_system_id': parent_system_id,
                    'node_type': NODE_TYPE_FOLDER
                }
            return None
    except Exception as e:
        logging.error(f"Error getting folder by docnumber: {e}")
        return None
    finally:
        await conn.close()

async def get_folder_system_id(docnumber: str) -> str | None:
    """
    Gets the SYSTEM_ID from FOLDER_ITEM for a given DOCNUMBER.
    """
    conn = await get_async_connection()
    if not conn:
        return None

    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                """SELECT SYSTEM_ID FROM FOLDER_ITEM 
                   WHERE DOCNUMBER = :docnumber AND NODE_TYPE = :node_type""",
                {'docnumber': docnumber, 'node_type': NODE_TYPE_FOLDER}
            )
            row = await cursor.fetchone()
            return str(row[0]) if row and row[0] else None
    except Exception as e:
        logging.error(f"Error getting folder system_id: {e}")
        return None
    finally:
        await conn.close()

async def get_folder_parent_docnumber(docnumber: str) -> str | None:
    """
    Gets the parent folder's DOCNUMBER for a given folder DOCNUMBER.
    PARENT column in FOLDER_ITEM contains the parent's DOCNUMBER directly.
    """
    conn = await get_async_connection()
    if not conn:
        return None

    try:
        async with conn.cursor() as cursor:
            # PARENT column contains the parent folder's DOCNUMBER
            await cursor.execute(
                """SELECT PARENT 
                   FROM FOLDER_ITEM
                   WHERE DOCNUMBER = :docnumber AND NODE_TYPE = :node_type""",
                {'docnumber': docnumber, 'node_type': NODE_TYPE_FOLDER}
            )
            row = await cursor.fetchone()
            return str(row[0]) if row and row[0] else None
    except Exception as e:
        logging.error(f"Error getting folder parent docnumber: {e}")
        return None
    finally:
        await conn.close()

async def get_folder_children(folder_docnumber: str) -> list:
    """
    Gets all child folders of a given folder.
    folder_docnumber is the DOCNUMBER of the parent folder.

    Note: PARENT column in FOLDER_ITEM contains the parent's DOCNUMBER, not SYSTEM_ID.
    """
    conn = await get_async_connection()
    if not conn:
        return []

    try:
        async with conn.cursor() as cursor:
            # Query child folders where PARENT = folder_docnumber
            # PARENT stores the DOCNUMBER of the parent folder
            await cursor.execute(
                """SELECT fi.SYSTEM_ID, fi.DOCNUMBER, fi.DISPLAYNAME, p.DOCNAME
                   FROM FOLDER_ITEM fi
                   LEFT JOIN PROFILE p ON fi.DOCNUMBER = p.DOCNUMBER
                   WHERE fi.PARENT = :parent_docnumber 
                   AND fi.NODE_TYPE = :node_type
                   ORDER BY NVL(fi.DISPLAYNAME, p.DOCNAME)""",
                {'parent_docnumber': folder_docnumber, 'node_type': NODE_TYPE_FOLDER}
            )
            rows = await cursor.fetchall()

            return [
                {
                    'id': str(row[1]),  # DOCNUMBER as ID
                    'system_id': str(row[0]),
                    'name': row[2] or row[3] or f'Folder {row[1]}',
                    'type': 'folder',
                    'media_type': 'folder'
                }
                for row in rows
            ]
    except Exception as e:
        logging.error(f"Error getting folder children: {e}")
        return []
    finally:
        await conn.close()

async def get_folder_files(folder_docnumber: str) -> list:
    """
    Gets all files/documents in a given folder.
    folder_docnumber is the DOCNUMBER of the folder.
    Uses resolve_media_types_from_db for accurate media type detection.

    Note: PARENT column in FOLDER_ITEM contains the parent's DOCNUMBER, not SYSTEM_ID.
    """
    conn = await get_async_connection()
    if not conn:
        return []

    try:
        async with conn.cursor() as cursor:
            # Query files where PARENT = folder_docnumber
            # PARENT stores the DOCNUMBER of the parent folder
            await cursor.execute(
                """SELECT fi.SYSTEM_ID, fi.DOCNUMBER, fi.DISPLAYNAME, p.DOCNAME
                   FROM FOLDER_ITEM fi
                   LEFT JOIN PROFILE p ON fi.DOCNUMBER = p.DOCNUMBER
                   WHERE fi.PARENT = :parent_docnumber 
                   AND fi.NODE_TYPE = :node_type
                   ORDER BY NVL(fi.DISPLAYNAME, p.DOCNAME)""",
                {'parent_docnumber': folder_docnumber, 'node_type': NODE_TYPE_DOCUMENT}
            )
            rows = await cursor.fetchall()

            if not rows:
                return []

            # Collect all DOCNUMBERs for media type resolution
            doc_ids = [str(row[1]) for row in rows if row[1]]

            # Resolve media types using the existing function from media.py
            media_types_map = {}
            if doc_ids:
                try:
                    media_types_map = await resolve_media_types_from_db(doc_ids)
                except Exception as e:
                    logging.error(f"Error resolving media types: {e}")

            files = []
            for row in rows:
                system_id, docnumber, display_name, docname = row
                file_name = display_name or docname or f'Document {docnumber}'

                # Get media type from resolved map, default to 'file'
                media_type = media_types_map.get(str(docnumber), 'file')

                files.append({
                    'id': str(docnumber),  # DOCNUMBER as ID for downloads
                    'system_id': str(system_id),
                    'name': file_name,
                    'type': 'file',
                    'media_type': media_type
                })

            return files
    except Exception as e:
        logging.error(f"Error getting folder files: {e}")
        return []
    finally:
        await conn.close()

async def get_folder_processing_summary(folder_docnumber: str) -> dict:
    """
    Returns a processing summary for all direct file children of a folder.

    Counts how many documents are still being processed (queued or in-progress)
    versus how many are done (TAGGING_QUEUE.STATUS = 3).

    Returns:
        {
            'folder_id': str,
            'total': int,        # total direct file children
            'processing': int,   # children still being processed
            'done': int,         # children with STATUS = 3
            'is_complete': bool, # True when processing == 0 and total > 0
            'processing_doc_ids': [str, ...]  # doc IDs still processing
        }
    """
    result = {
        'folder_id': str(folder_docnumber),
        'total': 0,
        'processing': 0,
        'done': 0,
        'is_complete': False,
        'processing_doc_ids': []
    }

    conn = await get_async_connection()
    if not conn:
        return result

    try:
        async with conn.cursor() as cursor:
            # LEFT JOIN TAGGING_QUEUE so that documents with no queue row
            # (just uploaded, not yet picked up) are counted as "processing".
            await cursor.execute(
                """SELECT fi.DOCNUMBER, NVL(q.STATUS, 0) AS QUEUE_STATUS
                   FROM FOLDER_ITEM fi
                   LEFT JOIN TAGGING_QUEUE q ON fi.DOCNUMBER = q.DOCNUMBER
                   WHERE fi.PARENT = :parent_docnumber
                   AND fi.NODE_TYPE = :node_type""",
                {'parent_docnumber': folder_docnumber, 'node_type': NODE_TYPE_DOCUMENT}
            )
            rows = await cursor.fetchall()

            if not rows:
                return result

            total = len(rows)
            done = 0
            processing_doc_ids = []

            for row in rows:
                docnumber, queue_status = row
                if queue_status == 3:
                    done += 1
                else:
                    processing_doc_ids.append(str(docnumber))

            processing = total - done
            result = {
                'folder_id': str(folder_docnumber),
                'total': total,
                'processing': processing,
                'done': done,
                'is_complete': processing == 0 and total > 0,
                'processing_doc_ids': processing_doc_ids
            }
            return result
    except Exception as e:
        logging.error(f"Error getting folder processing summary: {e}")
        return result
    finally:
        await conn.close()


async def get_folders_processing_summary_batch(folder_docnumbers: list) -> dict:
    """
    Returns a processing summary for multiple folders in a single DB round-trip.

    Args:
        folder_docnumbers: list of folder DOCNUMBERs (strings or ints)

    Returns:
        { folder_id: { total, processing, done, is_complete, processing_doc_ids }, ... }
    """
    result = {}
    if not folder_docnumbers:
        return result

    # Normalize to strings
    folder_ids_str = [str(f) for f in folder_docnumbers]
    for fid in folder_ids_str:
        result[fid] = {
            'folder_id': fid,
            'total': 0,
            'processing': 0,
            'done': 0,
            'is_complete': False,
            'processing_doc_ids': []
        }

    conn = await get_async_connection()
    if not conn:
        return result

    try:
        async with conn.cursor() as cursor:
            int_folder_ids = [int(f) for f in folder_ids_str if str(f).isdigit()]
            if not int_folder_ids:
                return result

            bind_names = [f':f{i}' for i in range(len(int_folder_ids))]
            bind_vars = {f'f{i}': val for i, val in enumerate(int_folder_ids)}

            # Get all direct file children of the requested folders with their
            # TAGGING_QUEUE status. LEFT JOIN so docs without a queue row count
            # as "processing" (not yet picked up by the worker).
            sql = f"""
                SELECT fi.PARENT AS FOLDER_ID, fi.DOCNUMBER, NVL(q.STATUS, 0) AS QUEUE_STATUS
                FROM FOLDER_ITEM fi
                LEFT JOIN TAGGING_QUEUE q ON fi.DOCNUMBER = q.DOCNUMBER
                WHERE fi.PARENT IN ({','.join(bind_names)})
                AND fi.NODE_TYPE = :node_type
            """
            bind_vars['node_type'] = NODE_TYPE_DOCUMENT
            await cursor.execute(sql, bind_vars)
            rows = await cursor.fetchall()

            # Aggregate per folder
            for row in rows:
                folder_id_raw, docnumber, queue_status = row
                fid = str(folder_id_raw)
                if fid not in result:
                    result[fid] = {
                        'folder_id': fid,
                        'total': 0,
                        'processing': 0,
                        'done': 0,
                        'is_complete': False,
                        'processing_doc_ids': []
                    }
                entry = result[fid]
                entry['total'] += 1
                if queue_status == 3:
                    entry['done'] += 1
                else:
                    entry['processing'] += 1
                    entry['processing_doc_ids'].append(str(docnumber))

            # Set is_complete flags
            for fid, entry in result.items():
                entry['is_complete'] = entry['processing'] == 0 and entry['total'] > 0

            return result
    except Exception as e:
        logging.error(f"Error in get_folders_processing_summary_batch: {e}")
        return result
    finally:
        await conn.close()


async def get_folder_preview_ids(folder_docnumber: str, limit: int = 4) -> list:
    """
    Gets up to `limit` document IDs from the folder that are images, videos, or PDFs
    to be used as folder thumbnail previews.
    """
    conn = await get_async_connection()
    if not conn:
        return []

    try:
        async with conn.cursor() as cursor:
            # Query files where PARENT = folder_docnumber and have correct extensions
            # DOSEXTENSION in PROFILE can be used, or just fetch all and resolve
            # We fetch up to a reasonable number to check extensions
            query_sql = """
                SELECT fi.DOCNUMBER, a.DEFAULT_EXTENSION, p.DOCNAME
                FROM FOLDER_ITEM fi
                LEFT JOIN PROFILE p ON fi.DOCNUMBER = p.DOCNUMBER
                LEFT JOIN APPS a ON p.APPLICATION = a.SYSTEM_ID
                WHERE fi.PARENT = :parent_docnumber 
                AND fi.NODE_TYPE = :node_type
                ORDER BY fi.DOCNUMBER DESC
            """
            try:
                await cursor.execute(
                    query_sql,
                    {'parent_docnumber': folder_docnumber, 'node_type': NODE_TYPE_DOCUMENT}
                )
            except oracledb.DatabaseError as db_err:
                if 'ORA-00904' in str(db_err):
                    fallback_sql = """
                        SELECT fi.DOCNUMBER, a.DEFAULT_EXTENSION, p.DOCNAME
                        FROM FOLDER_ITEM fi
                        LEFT JOIN PROFILE p ON fi.DOCNUMBER = p.DOCNUMBER
                        LEFT JOIN APPS a ON p.APPLICATION = a.APPLICATION
                        WHERE fi.PARENT = :parent_docnumber 
                        AND fi.NODE_TYPE = :node_type
                        ORDER BY fi.DOCNUMBER DESC
                    """
                    await cursor.execute(
                        fallback_sql,
                        {'parent_docnumber': folder_docnumber, 'node_type': NODE_TYPE_DOCUMENT}
                    )
                else:
                    raise
            rows = await cursor.fetchall()

            if not rows:
                return []

            preview_ids = []
            
            # Extensions that can generate a thumbnail
            valid_exts = {'jpg', 'jpeg', 'png', 'gif', 'bmp', 'tif', 'tiff', 'webp', 
                          'mp4', 'mov', 'avi', 'wmv', 'mkv', 'flv', 'webm', '3gp',
                          'pdf'}

            for row in rows:
                docnumber, extension, docname = row
                
                # Check extension from DOSEXTENSION or DOCNAME
                ext = None
                if extension:
                    ext = str(extension).lower().strip('.')
                elif docname and '.' in docname:
                    ext = str(docname).split('.')[-1].lower()
                
                if ext in valid_exts:
                    preview_ids.append(str(docnumber))
                    if len(preview_ids) >= limit:
                        break

            return preview_ids
    except Exception as e:
        logging.error(f"Error getting folder preview IDs: {e}")
        return []
    finally:
        await conn.close()

async def get_folder_contents(folder_docnumber: str) -> list:
    """
    Gets all contents (subfolders and files) of a folder.
    Returns folders first, then files, both sorted alphabetically.
    """
    folders = await get_folder_children(folder_docnumber)
    files = await get_folder_files(folder_docnumber)

    return folders + files


async def get_folder_files_recursive(folder_docnumber: str, prefix: str = '', max_depth: int = 20) -> list:
    """
    Recursively collects all files from a folder and its subfolders.
    Returns a list of dicts with 'id', 'name', and 'archive_path' (relative path preserving folder structure).
    """
    if max_depth <= 0:
        return []

    all_files = []

    # Get direct files in this folder
    direct_files = await get_folder_files(folder_docnumber)
    for f in direct_files:
        archive_path = f'{prefix}{f["name"]}' if prefix else f['name']
        all_files.append({
            'id': f['id'],
            'name': f['name'],
            'archive_path': archive_path
        })

    # Recurse into subfolders
    subfolders = await get_folder_children(folder_docnumber)
    for subfolder in subfolders:
        sub_prefix = f'{prefix}{subfolder["name"]}/' if prefix else f'{subfolder["name"]}/'
        sub_files = await get_folder_files_recursive(subfolder['id'], sub_prefix, max_depth - 1)
        all_files.extend(sub_files)

    return all_files

async def get_folder_by_id(folder_id: str) -> dict | None:
    """
    Alias for get_folder_by_docnumber for backward compatibility.
    """
    return await get_folder_by_docnumber(folder_id)

async def verify_folder_in_hierarchy(root_folder_docnumber: str, target_folder_docnumber: str) -> bool:
    """
    Verifies that target_folder is within the hierarchy of root_folder.
    Both parameters are DOCNUMBERs.
    """
    if str(root_folder_docnumber) == str(target_folder_docnumber):
        return True

    current_docnumber = target_folder_docnumber
    max_depth = 50  # Prevent infinite loops

    for _ in range(max_depth):
        parent_docnumber = await get_folder_parent_docnumber(current_docnumber)
        if not parent_docnumber:
            return False

        if str(parent_docnumber) == str(root_folder_docnumber):
            return True

        current_docnumber = parent_docnumber

    return False

async def verify_document_in_folder(root_folder_docnumber: str, doc_id: str) -> bool:
    """
    Verifies that a document belongs to the shared folder or its subfolders.
    root_folder_docnumber is the DOCNUMBER of the shared folder.
    doc_id is the DOCNUMBER of the document.

    PARENT column contains the parent folder's DOCNUMBER directly.
    """
    conn = await get_async_connection()
    if not conn:
        return False

    try:
        async with conn.cursor() as cursor:
            # Get the document's parent folder DOCNUMBER from FOLDER_ITEM
            # PARENT column contains the parent's DOCNUMBER
            await cursor.execute(
                """SELECT PARENT FROM FOLDER_ITEM 
                   WHERE DOCNUMBER = :doc_id 
                   AND NODE_TYPE = :node_type""",
                {'doc_id': doc_id, 'node_type': NODE_TYPE_DOCUMENT}
            )
            doc_row = await cursor.fetchone()
            if not doc_row or not doc_row[0]:
                return False

            doc_parent_docnumber = str(doc_row[0])

            # Check if document's parent is the root folder
            if doc_parent_docnumber == str(root_folder_docnumber):
                return True

            # Check if document's folder is in the hierarchy
            return await verify_folder_in_hierarchy(root_folder_docnumber, doc_parent_docnumber)
    except Exception as e:
        logging.error(f"Error verifying document in folder: {e}")
        return False
    finally:
        await conn.close()

async def build_breadcrumb_path(root_folder_docnumber: str, current_folder_docnumber: str) -> list:
    """
    Builds the breadcrumb path from root to current folder efficiently.
    Uses a single database connection for all lookups instead of N separate connections.
    Both parameters are DOCNUMBERs.
    """
    if str(root_folder_docnumber) == str(current_folder_docnumber):
        folder_info = await get_folder_by_docnumber(current_folder_docnumber)
        return [{
            'id': current_folder_docnumber,
            'name': folder_info.get('name', 'Shared Folder') if folder_info else 'Shared Folder'
        }]

    # Collect all docnumbers in the path first (child → parent order, reversed later)
    docnumbers = [current_folder_docnumber]
    current_docnumber = current_folder_docnumber
    max_depth = 50

    conn = await get_async_connection()
    if not conn:
        # Fallback to old method if connection fails
        return await _build_breadcrumb_path_fallback(root_folder_docnumber, current_folder_docnumber)

    try:
        async with conn.cursor() as cursor:
            for _ in range(max_depth):
                # Get parent docnumber for current folder
                await cursor.execute(
                    """SELECT PARENT FROM FOLDER_ITEM
                       WHERE DOCNUMBER = :docnumber AND NODE_TYPE = :node_type""",
                    {'docnumber': current_docnumber, 'node_type': NODE_TYPE_FOLDER}
                )
                row = await cursor.fetchone()
                if not row or not row[0]:
                    break

                parent_docnumber = str(row[0])

                if str(parent_docnumber) == str(root_folder_docnumber):
                    # Add root and stop — don't duplicate
                    docnumbers.append(parent_docnumber)
                    break

                if parent_docnumber not in docnumbers:
                    docnumbers.append(parent_docnumber)

                current_docnumber = parent_docnumber

            # Reverse: we collected child→parent, but need parent→child (root first)
            docnumbers.reverse()

            # Now batch-fetch all folder info in one query
            if not docnumbers:
                return [{'id': root_folder_docnumber, 'name': 'Shared Folder'}]

            placeholders = ','.join([f":{i}" for i in range(len(docnumbers))])
            query_params = {str(i): docnumber for i, docnumber in enumerate(docnumbers)}

            await cursor.execute(
                f"""SELECT fi.DOCNUMBER, fi.DISPLAYNAME, p.DOCNAME
                    FROM FOLDER_ITEM fi
                    LEFT JOIN PROFILE p ON fi.DOCNUMBER = p.DOCNUMBER
                    WHERE fi.DOCNUMBER IN ({placeholders}) AND fi.NODE_TYPE = :node_type""",
                {**query_params, 'node_type': NODE_TYPE_FOLDER}
            )
            rows = await cursor.fetchall()

            # Build a map of docnumber -> folder info
            folder_map = {}
            for row in rows:
                docnumber, display_name, docname = row
                folder_map[str(docnumber)] = display_name or docname or f'Folder {docnumber}'

            # Build path in correct order
            path = []
            for docnumber in docnumbers:
                path.append({
                    'id': str(docnumber),
                    'name': folder_map.get(str(docnumber), 'Folder')
                })

            return path if path else [{'id': root_folder_docnumber, 'name': 'Shared Folder'}]

    except Exception as e:
        logging.error(f"Error building breadcrumb path: {e}")
        return [{'id': root_folder_docnumber, 'name': 'Shared Folder'}]
    finally:
        await conn.close()


async def _build_breadcrumb_path_fallback(root_folder_docnumber: str, current_folder_docnumber: str) -> list:
    """
    Fallback method for build_breadcrumb_path (old method, slower but reliable).
    """
    path = []
    current_docnumber = current_folder_docnumber
    max_depth = 50

    for _ in range(max_depth):
        folder_info = await get_folder_by_docnumber(current_docnumber)
        if folder_info:
            path.insert(0, {
                'id': current_docnumber,
                'name': folder_info.get('name', 'Folder')
            })

        if str(current_docnumber) == str(root_folder_docnumber):
            break

        parent_docnumber = await get_folder_parent_docnumber(current_docnumber)
        if not parent_docnumber:
            break

        current_docnumber = parent_docnumber

    return path if path else [{'id': root_folder_docnumber, 'name': 'Shared Folder'}]