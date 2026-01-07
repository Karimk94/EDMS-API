from .connection import get_async_connection
from .media import resolve_media_types_from_db
import logging

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

async def get_folder_contents(folder_docnumber: str) -> list:
    """
    Gets all contents (subfolders and files) of a folder.
    Returns folders first, then files, both sorted alphabetically.
    """
    folders = await get_folder_children(folder_docnumber)
    files = await get_folder_files(folder_docnumber)

    return folders + files

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
    Builds the breadcrumb path from root to current folder.
    Both parameters are DOCNUMBERs.
    """
    if str(root_folder_docnumber) == str(current_folder_docnumber):
        folder_info = await get_folder_by_docnumber(current_folder_docnumber)
        return [{
            'id': current_folder_docnumber,
            'name': folder_info.get('name', 'Shared Folder') if folder_info else 'Shared Folder'
        }]

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