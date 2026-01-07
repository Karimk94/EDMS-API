from .connection import get_async_connection
import logging

# Node type constants
NODE_TYPE_FOLDER = 'F'
NODE_TYPE_DOCUMENT = 'D'

async def get_folder_by_id(folder_id: str) -> dict | None:
    """
    Retrieves folder information by SYSTEM_ID from FOLDER_ITEM.
    """
    conn = await get_async_connection()
    if not conn:
        return None

    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                """SELECT SYSTEM_ID, DISPLAYNAME, PARENT, NODE_TYPE 
                   FROM FOLDER_ITEM 
                   WHERE SYSTEM_ID = :folder_id 
                   AND NODE_TYPE = :node_type""",
                {'folder_id': folder_id, 'node_type': NODE_TYPE_FOLDER}
            )
            row = await cursor.fetchone()

            if row:
                return {
                    'system_id': row[0],
                    'id': str(row[0]),
                    'name': row[1] or 'Folder',
                    'parent_id': str(row[2]) if row[2] else None,
                    'node_type': row[3]
                }
            return None
    except Exception as e:
        logging.error(f"Error getting folder by ID: {e}")
        return None
    finally:
        await conn.close()

async def get_folder_parent(folder_id: str) -> str | None:
    """
    Gets the parent folder SYSTEM_ID for a given folder.
    """
    conn = await get_async_connection()
    if not conn:
        return None

    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                """SELECT PARENT FROM FOLDER_ITEM 
                   WHERE SYSTEM_ID = :folder_id""",
                {'folder_id': folder_id}
            )
            row = await cursor.fetchone()
            return str(row[0]) if row and row[0] else None
    except Exception as e:
        logging.error(f"Error getting folder parent: {e}")
        return None
    finally:
        await conn.close()

async def get_folder_children(folder_id: str) -> list:
    """
    Gets all child folders of a given folder from FOLDER_ITEM.
    """
    conn = await get_async_connection()
    if not conn:
        return []

    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                """SELECT SYSTEM_ID, DISPLAYNAME, PARENT 
                   FROM FOLDER_ITEM 
                   WHERE PARENT = :folder_id 
                   AND NODE_TYPE = :node_type
                   ORDER BY DISPLAYNAME""",
                {'folder_id': folder_id, 'node_type': NODE_TYPE_FOLDER}
            )
            rows = await cursor.fetchall()

            return [
                {
                    'id': str(row[0]),
                    'name': row[1] or 'Folder',
                    'parent_id': str(row[2]) if row[2] else None,
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

async def get_folder_files(folder_id: str) -> list:
    """
    Gets all files/documents in a given folder.
    Joins FOLDER_ITEM with PROFILE to get document details.
    """
    conn = await get_async_connection()
    if not conn:
        return []

    try:
        async with conn.cursor() as cursor:
            # Get documents that are in this folder
            # NODE_TYPE = 'D' for documents, join with PROFILE for metadata
            await cursor.execute(
                """SELECT fi.SYSTEM_ID, fi.DOCNUMBER, fi.DISPLAYNAME, p.DOCNAME, p.ABSTRACT
                   FROM FOLDER_ITEM fi
                   LEFT JOIN PROFILE p ON fi.DOCNUMBER = p.DOCNUMBER
                   WHERE fi.PARENT = :folder_id 
                   AND fi.NODE_TYPE = :node_type
                   ORDER BY NVL(fi.DISPLAYNAME, p.DOCNAME)""",
                {'folder_id': folder_id, 'node_type': NODE_TYPE_DOCUMENT}
            )
            rows = await cursor.fetchall()

            files = []
            for row in rows:
                system_id, docnumber, display_name, docname, abstract = row

                # Determine file name (prefer DISPLAYNAME, fallback to DOCNAME)
                file_name = display_name or docname or f'Document {docnumber}'

                # Determine media type from file extension
                media_type = get_media_type_from_filename(file_name)

                files.append({
                    'id': str(docnumber),  # Use DOCNUMBER as the ID for downloads
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

def get_media_type_from_filename(filename: str) -> str:
    """
    Determines media type from filename extension.
    """
    if not filename:
        return 'file'

    filename_lower = filename.lower()

    image_exts = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tif', '.tiff', '.webp', '.heic', '.ico', '.jfif'}
    video_exts = {'.mp4', '.mov', '.avi', '.mkv', '.wmv', '.flv', '.webm', '.m4v', '.3gp', '.ts', '.mts', '.3g2'}
    pdf_exts = {'.pdf'}
    excel_exts = {'.xls', '.xlsx', '.xlsm', '.csv'}
    powerpoint_exts = {'.ppt', '.pptx'}
    text_exts = {'.txt', '.rtf', '.json', '.xml', '.log', '.md'}

    for ext in image_exts:
        if filename_lower.endswith(ext):
            return 'image'

    for ext in video_exts:
        if filename_lower.endswith(ext):
            return 'video'

    for ext in pdf_exts:
        if filename_lower.endswith(ext):
            return 'pdf'

    for ext in excel_exts:
        if filename_lower.endswith(ext):
            return 'excel'

    for ext in powerpoint_exts:
        if filename_lower.endswith(ext):
            return 'powerpoint'

    for ext in text_exts:
        if filename_lower.endswith(ext):
            return 'text'

    return 'file'

async def get_folder_contents(folder_id: str) -> list:
    """
    Gets all contents (subfolders and files) of a folder.
    Returns folders first, then files, both sorted alphabetically.
    """
    folders = await get_folder_children(folder_id)
    files = await get_folder_files(folder_id)

    return folders + files

async def verify_folder_in_hierarchy(root_folder_id: str, target_folder_id: str) -> bool:
    """
    Verifies that target_folder_id is within the hierarchy of root_folder_id.
    Walks up the tree from target to see if we reach root.
    """
    if str(root_folder_id) == str(target_folder_id):
        return True

    current_id = target_folder_id
    max_depth = 50  # Prevent infinite loops

    for _ in range(max_depth):
        parent_id = await get_folder_parent(current_id)
        if not parent_id:
            return False

        if str(parent_id) == str(root_folder_id):
            return True

        current_id = parent_id

    return False

async def verify_document_in_folder(root_folder_id: str, doc_id: str) -> bool:
    """
    Verifies that a document belongs to the shared folder or its subfolders.
    Checks the FOLDER_ITEM table to find which folder contains the document.
    """
    conn = await get_async_connection()
    if not conn:
        return False

    try:
        async with conn.cursor() as cursor:
            # Get the document's parent folder from FOLDER_ITEM
            await cursor.execute(
                """SELECT PARENT FROM FOLDER_ITEM 
                   WHERE DOCNUMBER = :doc_id 
                   AND NODE_TYPE = :node_type""",
                {'doc_id': doc_id, 'node_type': NODE_TYPE_DOCUMENT}
            )
            row = await cursor.fetchone()
            if not row or not row[0]:
                return False

            doc_folder_id = str(row[0])

            # Check if document's folder is the root or in the hierarchy
            if doc_folder_id == str(root_folder_id):
                return True

            return await verify_folder_in_hierarchy(root_folder_id, doc_folder_id)
    except Exception as e:
        logging.error(f"Error verifying document in folder: {e}")
        return False
    finally:
        await conn.close()

async def build_breadcrumb_path(root_folder_id: str, current_folder_id: str) -> list:
    """
    Builds the breadcrumb path from root to current folder.
    """
    if str(root_folder_id) == str(current_folder_id):
        folder_info = await get_folder_by_id(current_folder_id)
        return [{
            'id': current_folder_id,
            'name': folder_info.get('name', 'Shared Folder') if folder_info else 'Shared Folder'
        }]

    path = []
    current_id = current_folder_id
    max_depth = 50

    for _ in range(max_depth):
        folder_info = await get_folder_by_id(current_id)
        if folder_info:
            path.insert(0, {
                'id': current_id,
                'name': folder_info.get('name', 'Folder')
            })

        if str(current_id) == str(root_folder_id):
            break

        parent_id = await get_folder_parent(current_id)
        if not parent_id:
            break

        current_id = parent_id

    return path if path else [{'id': root_folder_id, 'name': 'Shared Folder'}]