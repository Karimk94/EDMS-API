import os
import logging
import re
import zlib
import struct
from zeep import Client, Settings, xsd
from zeep.exceptions import Fault
from dotenv import load_dotenv

load_dotenv()

WSDL_URL = os.getenv("WSDL_URL")
DMS_USER = os.getenv("DMS_USER")
DMS_PASSWORD = os.getenv("DMS_PASSWORD")

# --- CONSTANTS ---
SMART_EDMS_ROOT_ID = '19685837'

def get_soap_client(service_name=None):
    settings = Settings(strict=False, xml_huge_tree=True)
    if service_name:
        return Client(WSDL_URL, port_name=service_name, settings=settings)
    return Client(WSDL_URL, settings=settings)

def find_client_with_operation(operation_name):
    try:
        base_client = Client(WSDL_URL, settings=Settings(strict=False, xml_huge_tree=True))
        for service in base_client.wsdl.services.values():
            for port in service.ports.values():
                try:
                    if operation_name in port.binding.port_type.operations:
                        return Client(WSDL_URL, port_name=port.name,
                                      settings=Settings(strict=False, xml_huge_tree=True))
                except Exception:
                    continue
        return None
    except Exception:
        return None

def dms_system_login():
    try:
        client = get_soap_client()
        if not hasattr(client.service, 'LoginSvr5'):
            client = find_client_with_operation('LoginSvr5')
        if not client: return None

        login_info_type = client.get_type(
            '{http://schemas.datacontract.org/2004/07/OpenText.DMSvr.Serializable}DMSvrLoginInfo')
        login_info_instance = login_info_type(network=0, loginContext='RTA_MAIN', username=DMS_USER,
                                              password=DMS_PASSWORD)
        array_type = client.get_type(
            '{http://schemas.datacontract.org/2004/07/OpenText.DMSvr.Serializable}ArrayOfDMSvrLoginInfo')
        login_info_array_instance = array_type(DMSvrLoginInfo=[login_info_instance])

        response = client.service.LoginSvr5(call={'loginInfo': login_info_array_instance, 'authen': 1, 'dstIn': ''})
        if response and response.resultCode == 0 and response.DSTOut:
            return response.DSTOut
        return None
    except Exception as e:
        logging.info(f"Login failed: {e}")
        return None

def get_doc_version_info(dst, doc_number):
    try:
        svc_client = get_soap_client('BasicHttpBinding_IDMSvc')
        get_doc_call = {
            'call': {
                'dstIn': dst,
                'criteria': {
                    'criteriaCount': 2,
                    'criteriaNames': {'string': ['%TARGET_LIBRARY', '%DOCUMENT_NUMBER']},
                    'criteriaValues': {'string': ['RTA_MAIN', str(doc_number)]}
                }
            }
        }
        doc_reply = svc_client.service.GetDocSvr3(**get_doc_call)
        if doc_reply and doc_reply.resultCode == 0 and doc_reply.docProperties:
            prop_names = doc_reply.docProperties.propertyNames.string
            prop_values = doc_reply.docProperties.propertyValues.anyType
            if '%VERSION_ID' in prop_names:
                return prop_values[prop_names.index('%VERSION_ID')]
            elif 'VERSION_ID' in prop_names:
                return prop_values[prop_names.index('VERSION_ID')]
    except Exception:
        pass
    return "0"

def create_dms_folder(dst, folder_name, description="", parent_id=None, user_id=None):
    target_parent_id = parent_id if parent_id and str(parent_id).strip() else SMART_EDMS_ROOT_ID
    try:
        svc_client = get_soap_client('BasicHttpBinding_IDMSvc')
        string_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}string')
        string_array_type = svc_client.get_type(
            '{http://schemas.microsoft.com/2003/10/Serialization/Arrays}ArrayOfstring')
        effective_user = user_id if user_id else DMS_USER

        prop_names = string_array_type(
            ['%TARGET_LIBRARY', 'DOCNAME', 'TYPE_ID', 'APP_ID', 'AUTHOR_ID', 'TYPIST_ID', 'ABSTRACT', 'SECURITY'])
        prop_values = {'anyType': [
            xsd.AnyObject(string_type, 'RTA_MAIN'), xsd.AnyObject(string_type, folder_name),
            xsd.AnyObject(string_type, 'FOLDER'), xsd.AnyObject(string_type, 'FOLDER'),
            xsd.AnyObject(string_type, effective_user), xsd.AnyObject(string_type, effective_user),
            xsd.AnyObject(string_type, description), xsd.AnyObject(string_type, '1')
        ]}

        create_reply = svc_client.service.CreateObject(call={'dstIn': dst, 'objectType': 'DEF_PROF',
                                                             'properties': {'propertyCount': 8,
                                                                            'propertyNames': prop_names,
                                                                            'propertyValues': prop_values}})
        if create_reply.resultCode != 0: raise Exception(f"Create error: {getattr(create_reply, 'errorDoc', '')}")

        ret_names = create_reply.retProperties.propertyNames.string
        new_id = create_reply.retProperties.propertyValues.anyType[ret_names.index('%OBJECT_IDENTIFIER')]

        parent_ver = get_doc_version_info(dst, target_parent_id)
        ci_names = string_array_type(
            ['%TARGET_LIBRARY', 'PARENT', 'PARENT_VERSION', 'DOCNUMBER', '%FOLDERITEM_LIBRARY_NAME', 'DISPLAYNAME',
             'VERSION_TYPE'])
        ci_values = {'anyType': [
            xsd.AnyObject(string_type, 'RTA_MAIN'), xsd.AnyObject(string_type, str(target_parent_id)),
            xsd.AnyObject(string_type, str(parent_ver)), xsd.AnyObject(string_type, str(new_id)),
            xsd.AnyObject(string_type, 'RTA_MAIN'), xsd.AnyObject(string_type, folder_name),
            xsd.AnyObject(string_type, 'R')
        ]}

        link_reply = svc_client.service.CreateObject(call={'dstIn': dst, 'objectType': 'ContentItem',
                                                           'properties': {'propertyCount': 7, 'propertyNames': ci_names,
                                                                          'propertyValues': ci_values}})
        if link_reply.resultCode != 0: raise Exception(f"Link error: {getattr(link_reply, 'errorDoc', '')}")

        return new_id
    except Exception as e:
        logging.info(f"Create folder failed: {e}")
        return None

def parse_binary_result_buffer(buffer):
    """
    Parses the raw text buffer from DMS to extract folder/file items.
    Handles zlib compression.
    Identifies type based on APP_ID (e.g. ACROBAT, MSWORD) mapping and extensions.
    """
    items = []
    try:
        # --- Step 1: Handle Decompression ---
        try:
            if len(buffer) > 8 and buffer[8:10] == b'\x78\x9c':
                decompressed = zlib.decompress(buffer[8:])
                buffer = decompressed
        except Exception as e:
            pass

        # --- Step 2: Decode Text ---
        try:
            raw_text = buffer.decode('utf-16-le', errors='ignore')
        except:
            raw_text = buffer.decode('utf-8', errors='ignore')

        # Clean text but keep dots for extensions
        clean_text = re.sub(r'[^\w\s\-\.]', ' ', raw_text)
        tokens = clean_text.split()

        # Known Folder Applications
        FOLDER_APPS = {'FOLDER', 'DEF_PROF', 'SAVED_SEARCHES', 'CONTENTSCOLLECTION'}

        # Extensions Map
        EXT_MAP = {
            'pdf': 'pdf', 'doc': 'pdf', 'docx': 'pdf', 'txt': 'pdf', 'xls': 'pdf', 'xlsx': 'pdf', 'ppt': 'pdf',
            'pptx': 'pdf',
            'jpg': 'image', 'jpeg': 'image', 'png': 'image', 'gif': 'image', 'bmp': 'image', 'tif': 'image',
            'tiff': 'image', 'webp': 'image',
            'mp4': 'video', 'mov': 'video', 'avi': 'video', 'wmv': 'video', 'mkv': 'video', 'flv': 'video',
            'webm': 'video', '3gp': 'video'
        }

        i = 0
        while i < len(tokens):
            token = tokens[i]

            # Check if token is likely a Doc ID (numeric, 5+ digits)
            if token.isdigit() and len(token) >= 5:
                # Look ahead until the NEXT ID
                if i + 1 < len(tokens):
                    chunk_tokens = []
                    j = i + 1

                    # Consume until next ID
                    while j < len(tokens):
                        next_token = tokens[j]
                        if next_token.isdigit() and len(next_token) >= 5:
                            break
                        chunk_tokens.append(next_token)
                        j += 1

                    # Advance main loop
                    i = j - 1

                    if chunk_tokens:
                        item_type = 'folder'  # Default to folder if nothing matches
                        media_type = 'folder'
                        name_parts = []
                        is_folder = False

                        # --- 1. Strict Folder Check ---
                        # Check for 'F' at end
                        if chunk_tokens[-1] == 'F' or (len(chunk_tokens) > 1 and chunk_tokens[-2] == 'F'):
                            is_folder = True
                        else:
                            # Check for folder keywords in tokens
                            for t in chunk_tokens:
                                if t.upper() in FOLDER_APPS:
                                    is_folder = True
                                    break

                        # --- 2. File Type Detection ---
                        if not is_folder:
                            item_type = 'file'
                            media_type = 'resolve'  # Default to resolve if unknown

                            # Check for extension in name tokens
                            for t in chunk_tokens:
                                if '.' in t:
                                    ext = t.split('.')[-1].lower()
                                    if ext in EXT_MAP:
                                        media_type = EXT_MAP[ext]
                                        break

                        # --- CLEANUP NAME ---
                        name_parts = []
                        for t in chunk_tokens:
                            if t not in ['N', 'D', 'F'] and t.upper() not in FOLDER_APPS:
                                name_parts.append(t)

                        full_name = " ".join(name_parts).strip()

                        if len(full_name) > 0:
                            items.append({
                                'id': token,
                                'name': full_name,
                                'type': item_type,
                                'media_type': media_type,
                                'node_type': 'F' if is_folder else 'N',
                                'is_standard': False
                            })
            i += 1
    except Exception as e:
        logging.info(f"Error parsing binary buffer: {e}")
        pass
    return items

def get_recursive_doc_ids(dst, media_type_filter=None):
    """
    Recursively scans the root folder and returns a list of DOCNUMBERS (and names) that match the given media_type.
    """
    import db_connector

    logging.info(f"DEBUG: Starting recursive scan. Root: {SMART_EDMS_ROOT_ID}, Filter: {media_type_filter}")
    matching_docs = []

    folder_queue = [SMART_EDMS_ROOT_ID]
    processed_folders = set()
    MAX_FOLDERS_TO_SCAN = 100

    search_client = get_soap_client('BasicHttpBinding_IDMSvc')
    data_client = find_client_with_operation('GetDataW') or find_client_with_operation('GetData')
    method_name = 'GetDataW' if hasattr(data_client.service, 'GetDataW') else 'GetData'

    if not data_client:
        logging.info("DEBUG: Data client not found.")
        return []

    try:
        while folder_queue:
            if len(processed_folders) >= MAX_FOLDERS_TO_SCAN:
                logging.info(f"DEBUG: Recursion limit reached ({MAX_FOLDERS_TO_SCAN}).")
                break

            current_folder_id = folder_queue.pop(0)
            if current_folder_id in processed_folders:
                continue
            processed_folders.add(current_folder_id)

            logging.info(f"DEBUG: Scanning folder {current_folder_id}...")

            search_call = {
                'call': {
                    'dstIn': dst,
                    'objectType': 'ContentsCollection',
                    'signature': {
                        'libraries': {'string': ['RTA_MAIN']},
                        'criteria': {
                            'criteriaCount': 1,
                            'criteriaNames': {'string': ['%ITEM']},
                            'criteriaValues': {'string': [str(current_folder_id)]}
                        },
                        'retProperties': {
                            'string': ['FI.DOCNUMBER', '%DISPLAY_NAME', 'FI.NODE_TYPE', 'DOCNAME', 'APPLICATION',
                                       'APP_ID', 'DOSEXTENSION']},
                        'sortProps': {'propertyCount': 1, 'propertyNames': {'string': ['%DISPLAY_NAME']},
                                      'propertyFlags': {'int': [1]}},
                        'maxRows': 0
                    }
                }
            }

            search_reply = search_client.service.Search(**search_call)

            if not (search_reply and search_reply.resultCode == 0 and search_reply.resultSetID):
                code = getattr(search_reply, 'resultCode', 'N/A')
                logging.info(f"DEBUG: Search failed/empty for {current_folder_id}. Code: {code}")
                continue

            result_set_id = search_reply.resultSetID

            chunk_size = 500
            start_row = 0

            while True:
                get_data_call = {
                    'call': {'resultSetID': result_set_id, 'requestedRows': chunk_size, 'startingRow': start_row}}
                data_reply = getattr(data_client.service, method_name)(**get_data_call)

                items_batch = []
                has_data = False

                # 1. XML Logic (Primary)
                row_nodes = None
                if hasattr(data_reply, 'rowNode'):
                    row_nodes = data_reply.rowNode
                elif hasattr(data_reply, 'RowNode'):
                    row_nodes = data_reply.RowNode
                elif isinstance(data_reply, dict):
                    row_nodes = data_reply.get('rowNode') or data_reply.get('RowNode')

                if row_nodes:
                    has_data = True
                    logging.info(f"DEBUG: Found {len(row_nodes)} items via XML in {current_folder_id}")
                    for row in row_nodes:
                        try:
                            props = row.propValues.anyType
                            doc_id = props[0]
                            name = props[1] if len(props) > 1 else str(doc_id)
                            node_type = props[2] if len(props) > 2 else 'N'

                            is_folder = (node_type == 'F')
                            media_type = 'folder' if is_folder else 'resolve'

                            if not is_folder and len(props) > 6 and props[6]:
                                dos_ext = str(props[6]).lower().replace('.', '').strip()
                                if dos_ext in ['jpg', 'jpeg', 'png', 'gif', 'bmp']:
                                    media_type = 'image'
                                elif dos_ext in ['mp4', 'mov', 'avi', 'mkv']:
                                    media_type = 'video'
                                elif dos_ext in ['pdf', 'doc', 'docx']:
                                    media_type = 'pdf'

                            items_batch.append({
                                'id': str(doc_id),
                                'name': name,
                                'media_type': media_type,
                                'type': 'folder' if is_folder else 'file'
                            })
                        except Exception as e:
                            logging.info(f"DEBUG: XML parse error: {e}")

                            # 2. Binary Fallback
                if not items_batch and hasattr(data_reply, 'resultSetData') and data_reply.resultSetData:
                    container = data_reply.resultSetData
                    if hasattr(container, 'resultBuffer') and container.resultBuffer:
                        logging.info(f"DEBUG: Using binary buffer for {current_folder_id}")
                        parsed = parse_binary_result_buffer(container.resultBuffer)
                        if parsed:
                            has_data = True
                            items_batch.extend(parsed)

                if not has_data or not items_batch:
                    break

                ids_to_resolve = []

                for item in items_batch:
                    # Log every item
                    # logging.info(f"DEBUG: Item {item['id']} ({item['name']}) -> Type: {item.get('type')}, Media: {item.get('media_type')}")

                    if item.get('type') == 'folder' or item.get('media_type') == 'folder':
                        if item['id'] not in processed_folders and item['id'] not in folder_queue:
                            logging.info(f"DEBUG: Enqueueing subfolder: {item['id']}")
                            folder_queue.append(item['id'])
                        continue

                    m_type = item.get('media_type')
                    if m_type == 'resolve':
                        ids_to_resolve.append(item)
                    elif m_type == media_type_filter:
                        logging.info(f"DEBUG: Match found (Pre-resolved): {item['id']}")
                        matching_docs.append({'id': item['id'], 'name': item['name']})

                if ids_to_resolve:
                    try:
                        resolve_ids_only = [x['id'] for x in ids_to_resolve]
                        logging.info(f"DEBUG: Resolving {len(resolve_ids_only)} IDs via DB.")
                        resolved_map = db_connector.resolve_media_types_from_db(resolve_ids_only)
                        for resolve_item in ids_to_resolve:
                            doc_id = resolve_item['id']
                            r_type = resolved_map.get(doc_id, 'pdf')

                            if r_type == media_type_filter:
                                logging.info(f"DEBUG: Match found (DB-resolved): {doc_id}")
                                matching_docs.append({'id': doc_id, 'name': resolve_item['name']})
                    except Exception as e:
                        logging.info(f"DEBUG: DB resolve error: {e}")

                start_row += chunk_size
                if start_row > 2000: break

            try:
                search_client.service.ReleaseData(call={'resultSetID': result_set_id})
                search_client.service.ReleaseObject(call={'objectID': result_set_id})
            except:
                pass

    except Exception as e:
        logging.info(f"DEBUG: Recursive scan error: {e}")

    logging.info(f"DEBUG: Scan complete. Found {len(matching_docs)} items.")
    return matching_docs

def list_folder_contents(dst, parent_id=None, app_source=None, scope=None, media_type=None):
    """
    Lists contents of a folder.
    If scope='folders' and media_type is provided, it returns a flat list of all matching files recursively.
    """
    import db_connector

    items = []

    # --- Robust Media Type Detection ---
    if parent_id == 'images':
        media_type = 'image'
        scope = 'folders'
    elif parent_id == 'videos':
        media_type = 'video'
        scope = 'folders'
    elif parent_id == 'files':
        media_type = 'pdf'
        scope = 'folders'

    print(f"DEBUG: list_folder_contents called. Parent: {parent_id}, Scope: {scope}, Media: {media_type}")

    # --- Special Handling for Recursive Standard Folders ---
    if scope == 'folders' and media_type:
        # 1. Get all doc IDs recursively that match the media type
        doc_items = get_recursive_doc_ids(dst, media_type_filter=media_type)

        print(f"DEBUG: Recursive fetch returned {len(doc_items)} items.")

        # 2. If we have IDs, fetch their details to populate the folder view
        if doc_items:
            # Construct items directly from recursive result
            # We trust the recursion name/id
            for doc in doc_items:
                items.append({
                    'id': str(doc['id']),
                    'name': doc['name'],
                    'type': 'file',
                    'media_type': media_type,
                    'is_standard': False,
                    'thumbnail_url': f"cache/{doc['id']}.jpg"
                })

        return items

    # -------------------------------------------------------
    # Standard Folder Listing Logic

    target_id = parent_id
    is_root_view = False

    if not target_id or str(target_id).strip() == "" or str(target_id).lower() == "null":
        target_id = SMART_EDMS_ROOT_ID
        is_root_view = True

    # 1. Add Standard Folders (Root Only) - ONLY if we are at the root and NOT filtering by media type
    if is_root_view and not media_type:
        try:
            # Pass scope here so db_connector knows to use the DMS folder counting logic
            counts = db_connector.get_media_type_counts(app_source, scope=scope)

            def get_cnt(k):
                return counts.get(k, 0) if counts else 0

            items.append(
                {'id': 'images', 'name': 'Images', 'type': 'folder', 'is_standard': True, 'count': get_cnt('images')})
            items.append(
                {'id': 'videos', 'name': 'Videos', 'type': 'folder', 'is_standard': True, 'count': get_cnt('videos')})
            items.append(
                {'id': 'files', 'name': 'Files', 'type': 'folder', 'is_standard': True, 'count': get_cnt('files')})
        except Exception:
            pass

    # 2. Fetch DMS Contents (Normal Folder View - Immediate Children)
    # Only fetch immediate children if we are NOT doing a recursive media type fetch
    if not media_type:
        try:
            search_client = get_soap_client('BasicHttpBinding_IDMSvc')
            criteria_name = '%ITEM'
            criteria_value = str(target_id)

            # Added APP_ID and APPLICATION to retProperties
            search_call = {
                'call': {
                    'dstIn': dst,
                    'objectType': 'ContentsCollection',
                    'signature': {
                        'libraries': {'string': ['RTA_MAIN']},
                        'criteria': {
                            'criteriaCount': 1,
                            'criteriaNames': {'string': [criteria_name]},
                            'criteriaValues': {'string': [criteria_value]}
                        },
                        'retProperties': {
                            'string': ['FI.DOCNUMBER', '%DISPLAY_NAME', 'FI.NODE_TYPE', 'DOCNAME', 'APPLICATION',
                                       'APP_ID', 'DOSEXTENSION']},  # Added DOSEXTENSION explicitly
                        'sortProps': {'propertyCount': 1, 'propertyNames': {'string': ['%DISPLAY_NAME']},
                                      'propertyFlags': {'int': [1]}},
                        'maxRows': 0
                    }
                }
            }

            search_reply = search_client.service.Search(**search_call)

            if not (search_reply and search_reply.resultCode == 0 and search_reply.resultSetID):
                return items

            result_set_id = search_reply.resultSetID
            data_client = find_client_with_operation('GetDataW') or find_client_with_operation('GetData')
            method_name = 'GetDataW' if hasattr(data_client.service, 'GetDataW') else 'GetData'

            if not data_client:
                print("FATAL: Neither GetDataW nor GetData found.")
                return items

            get_data_call = {'call': {'resultSetID': result_set_id, 'requestedRows': 500, 'startingRow': 0}}
            data_reply = getattr(data_client.service, method_name)(**get_data_call)

            # --- PARSING LOGIC ---

            # Check for binary buffer first
            if hasattr(data_reply, 'resultSetData') and data_reply.resultSetData:
                container = data_reply.resultSetData
                if hasattr(container, 'resultBuffer') and container.resultBuffer:
                    #  logging.info("Detected binary resultBuffer. Attempting custom parse.")
                    parsed_items = parse_binary_result_buffer(container.resultBuffer)
                    if parsed_items:
                        items.extend(parsed_items)
                        try:
                            search_client.service.ReleaseData(call={'resultSetID': result_set_id})
                            search_client.service.ReleaseObject(call={'objectID': result_set_id})
                        except:
                            pass
                        # Don't return yet, we need to resolve media types

            # Fallback to XML parsing
            # (This part is rarely hit if resultBuffer is present, but kept for safety)
            if not items:
                row_nodes = None
                if hasattr(data_reply, 'rowNode'):
                    row_nodes = data_reply.rowNode
                elif hasattr(data_reply, 'RowNode'):
                    row_nodes = data_reply.RowNode
                elif isinstance(data_reply, dict):
                    row_nodes = data_reply.get('rowNode') or data_reply.get('RowNode')

                if row_nodes:
                    for row in row_nodes:
                        try:
                            props = row.propValues.anyType
                            doc_id = props[0]
                            name_str = str(props[1] or props[3] or "Untitled")
                            node_type = props[2]

                            # Simplified fallback logic for XML path
                            is_folder = (node_type == 'F')
                            media_type_item = 'folder' if is_folder else 'resolve'  # Use resolve logic here too

                            if name_str.endswith(' D') or name_str.endswith(' N') or name_str.endswith(' F'):
                                name_str = name_str[:-2]

                            items.append({
                                'id': str(doc_id), 'name': name_str, 'type': 'folder' if is_folder else 'file',
                                'media_type': media_type_item,
                                'node_type': str(node_type), 'is_standard': False
                            })
                        except Exception:
                            pass

            try:
                search_client.service.ReleaseData(call={'resultSetID': result_set_id})
                search_client.service.ReleaseObject(call={'objectID': result_set_id})
            except Exception:
                pass

        except Exception as e:
            print(f"Error listing folder contents: {e}")
            return items

    # --- RESOLVE UNKNOWN TYPES VIA DATABASE ---
    ids_to_resolve = [item['id'] for item in items if item.get('media_type') == 'resolve']

    if ids_to_resolve:
        try:
            resolved_types = db_connector.resolve_media_types_from_db(ids_to_resolve)
            for item in items:
                if item.get('media_type') == 'resolve':
                    item['media_type'] = resolved_types.get(item['id'], 'pdf')
        except Exception as e:
            print(f"Error resolving types from DB: {e}")
            for item in items:
                if item.get('media_type') == 'resolve':
                    item['media_type'] = 'pdf'

    return items

def delete_document(dst, doc_number, force=False):
    """
    Deletes a document/folder.
    """
    try:
        svc_client = get_soap_client('BasicHttpBinding_IDMSvc')
        del_client = find_client_with_operation('DeleteObject')
        if not del_client: return False, "DeleteObject not found"

        string_type = del_client.get_type('{http://www.w3.org/2001/XMLSchema}string')
        string_array_type = del_client.get_type(
            '{http://schemas.microsoft.com/2003/10/Serialization/Arrays}ArrayOfstring')
        int_type = del_client.get_type('{http://www.w3.org/2001/XMLSchema}int')

        if force:
            links_to_remove = []
            coll_names = string_array_type(['%TARGET_LIBRARY', 'DOCNUMBER', '%CONTENTS_DIRECTIVE'])
            coll_values = {'anyType': [
                xsd.AnyObject(string_type, 'RTA_MAIN'),
                xsd.AnyObject(string_type, str(doc_number)),
                xsd.AnyObject(string_type, '%CONTENTS_WHERE_USED')
            ]}

            coll_call = {'call': {'dstIn': dst, 'objectType': 'ContentsCollection',
                                  'properties': {'propertyCount': 3, 'propertyNames': coll_names,
                                                 'propertyValues': coll_values}}}

            try:
                coll_reply = svc_client.service.CreateObject(**coll_call)
                col_id = None
                if coll_reply.resultCode == 0 and coll_reply.retProperties:
                    col_id = coll_reply.retProperties.propertyValues.anyType[0]

                if col_id:
                    enum_client = find_client_with_operation('NewEnum') or svc_client
                    enum_reply = enum_client.service.NewEnum(call={'dstIn': dst, 'collectionID': col_id})

                    if enum_reply.resultCode == 0 and enum_reply.enumID:
                        next_reply = enum_client.service.NextData(
                            call={'dstIn': dst, 'enumID': enum_reply.enumID, 'elementCount': 100})

                        if next_reply.resultCode in [0, 1] and next_reply.genericItemsData:
                            g_data = next_reply.genericItemsData
                            prop_names = g_data.propertyNames.string
                            rows = g_data.propertyRows.ArrayOfanyType

                            idx_sys = prop_names.index('SYSTEM_ID') if 'SYSTEM_ID' in prop_names else -1
                            idx_par = prop_names.index('PARENT') if 'PARENT' in prop_names else -1
                            idx_pver = prop_names.index('PARENT_VERSION') if 'PARENT_VERSION' in prop_names else -1

                            if idx_sys != -1 and rows:
                                for row in rows:
                                    try:
                                        link_data = {
                                            'SYSTEM_ID': row.anyType[idx_sys],
                                            'PARENT': row.anyType[idx_par] if idx_par != -1 else None,
                                            'PARENT_VERSION': row.anyType[idx_pver] if idx_pver != -1 else None
                                        }
                                        links_to_remove.append(link_data)
                                    except:
                                        pass

                        try:
                            enum_client.service.ReleaseObject(call={'objectID': enum_reply.enumID})
                        except:
                            pass
                    try:
                        svc_client.service.ReleaseObject(call={'objectID': col_id})
                    except:
                        pass
            except Exception as e:
                logging.info(f"WhereUsed failed: {e}")  # Changed to print

            if links_to_remove:
                for link in links_to_remove:
                    prop_n_list = ['%TARGET_LIBRARY', 'SYSTEM_ID']
                    prop_v_list = [xsd.AnyObject(string_type, 'RTA_MAIN'),
                                   xsd.AnyObject(int_type, int(link['SYSTEM_ID']))]

                    if link['PARENT']:
                        prop_n_list.append('PARENT')
                        prop_v_list.append(xsd.AnyObject(string_type, str(link['PARENT'])))
                    if link['PARENT_VERSION']:
                        prop_n_list.append('PARENT_VERSION')
                        prop_v_list.append(xsd.AnyObject(string_type, str(link['PARENT_VERSION'])))

                    del_link_call = {
                        'call': {
                            'dstIn': dst,
                            'objectType': 'ContentItem',
                            'properties': {
                                'propertyCount': len(prop_n_list),
                                'propertyNames': string_array_type(prop_n_list),
                                'propertyValues': {'anyType': prop_v_list}
                            }
                        }
                    }
                    try:
                        res = del_client.service.DeleteObject(**del_link_call)
                        if res.resultCode != 0:
                            logging.info(
                                f"Failed unlink {link['SYSTEM_ID']}: {getattr(res, 'errorDoc', '')}")  # Changed to print
                    except Exception as e:
                        logging.info(f"Exception unlink {link['SYSTEM_ID']}: {e}")  # Changed to print

        del_props = {'propertyCount': 2, 'propertyNames': string_array_type(['%TARGET_LIBRARY', '%OBJECT_IDENTIFIER']),
                     'propertyValues': {
                         'anyType': [xsd.AnyObject(string_type, 'RTA_MAIN'), xsd.AnyObject(int_type, int(doc_number))]}}

        try:
            resp = del_client.service.DeleteObject(
                call={'dstIn': dst, 'objectType': 'DEF_PROF', 'properties': del_props})
            if resp.resultCode == 0:
                return True, "Success"

            err_doc = getattr(resp, 'errorDoc', '')
            logging.info(f"Delete failed: {err_doc}")  # Changed to print
            return False, str(err_doc)

        except Fault as f:
            err_msg = f.message or str(f)
            return False, err_msg

    except Exception as e:
        logging.info(f"Delete exception: {e}")  # Changed to print
        return False, str(e)

def get_image_by_docnumber(dst, doc_number):
    """Retrieves a single document's image bytes."""
    svc_client, obj_client, content_id, stream_id = None, None, None, None
    try:
        svc_client = get_soap_client('BasicHttpBinding_IDMSvc')
        obj_client = get_soap_client('BasicHttpBinding_IDMObj')

        get_doc_call = {
            'call': {
                'dstIn': dst,
                'criteria': {
                    'criteriaCount': 3,
                    'criteriaNames': {'string': ['%TARGET_LIBRARY', '%DOCUMENT_NUMBER', '%VERSION_ID']},
                    'criteriaValues': {'string': ['RTA_MAIN', str(doc_number), '%VERSION_TO_INDEX']}
                }
            }
        }
        doc_reply = svc_client.service.GetDocSvr3(**get_doc_call)

        if not (doc_reply and doc_reply.resultCode == 0 and doc_reply.getDocID):
            return None, None
        content_id = doc_reply.getDocID

        stream_reply = obj_client.service.GetReadStream(call={'dstIn': dst, 'contentID': content_id})
        if not (stream_reply and stream_reply.resultCode == 0 and stream_reply.streamID):
            raise Exception("Failed to get read stream.")
        stream_id = stream_reply.streamID

        doc_buffer = bytearray()
        while True:
            read_reply = obj_client.service.ReadStream(call={'streamID': stream_id, 'requestedBytes': 65536})
            if not read_reply or read_reply.resultCode != 0: break
            chunk_data = read_reply.streamData.streamBuffer if read_reply.streamData else None
            if not chunk_data: break
            doc_buffer.extend(chunk_data)

        filename = f"{doc_number}.jpg"
        if doc_reply.docProperties and doc_reply.docProperties.propertyValues:
            try:
                prop_names = doc_reply.docProperties.propertyNames.string
                if '%VERSION_FILE_NAME' in prop_names:
                    index = prop_names.index('%VERSION_FILE_NAME')
                    version_file_name = doc_reply.docProperties.propertyValues.anyType[index]
                    _, extension = os.path.splitext(str(version_file_name))
                    if extension:
                        filename = f"{doc_number}{extension}"
            except Exception:
                pass

        return doc_buffer, filename

    except Fault as e:
        logging.info(f"DMS server fault during retrieval for doc: {doc_number}. Fault: {e}")  # Changed to print
        return None, None
    finally:
        if obj_client:
            if stream_id:
                try:
                    obj_client.service.ReleaseObject(call={'objectID': stream_id})
                except Exception:
                    pass
            if content_id:
                try:
                    obj_client.service.ReleaseObject(call={'objectID': content_id})
                except Exception:
                    pass

def get_dms_stream_details(dst, doc_number):
    """Opens a stream to a DMS document."""
    try:
        svc_client = get_soap_client('BasicHttpBinding_IDMSvc')
        obj_client = get_soap_client('BasicHttpBinding_IDMObj')

        get_doc_call = {'call': {'dstIn': dst,
                                 'criteria': {'criteriaCount': 2,
                                              'criteriaNames': {'string': ['%TARGET_LIBRARY', '%DOCUMENT_NUMBER']},
                                              'criteriaValues': {'string': ['RTA_MAIN', str(doc_number)]}}}}
        doc_reply = svc_client.service.GetDocSvr3(**get_doc_call)
        if not (doc_reply and doc_reply.resultCode == 0 and doc_reply.getDocID):
            return None

        content_id = doc_reply.getDocID
        stream_reply = obj_client.service.GetReadStream(call={'dstIn': dst, 'contentID': content_id})
        if not (stream_reply and stream_reply.resultCode == 0 and stream_reply.streamID):
            obj_client.service.ReleaseObject(call={'objectID': content_id})
            return None

        return {
            "obj_client": obj_client,
            "stream_id": stream_reply.streamID,
            "content_id": content_id
        }
    except Exception as e:
        return None

def dms_user_login(username, password):
    """Logs into the DMS SOAP service with user-provided credentials."""
    try:
        if not WSDL_URL: raise ValueError("WSDL_URL is not set.")
        client = get_soap_client()

        # If default client doesn't have LoginSvr5, search for it
        if not hasattr(client.service, 'LoginSvr5'):
            client = find_client_with_operation('LoginSvr5')
            if not client:
                logging.info("LoginSvr5 not found during user login.")  # Changed to print
                return None

        login_info_type = client.get_type(
            '{http://schemas.datacontract.org/2004/07/OpenText.DMSvr.Serializable}DMSvrLoginInfo')
        login_info_instance = login_info_type(network=0, loginContext='RTA_MAIN', username=username,
                                              password=password)
        array_type = client.get_type(
            '{http://schemas.datacontract.org/2004/07/OpenText.DMSvr.Serializable}ArrayOfDMSvrLoginInfo')
        login_info_array_instance = array_type(DMSvrLoginInfo=[login_info_instance])
        call_data = {'call': {'loginInfo': login_info_array_instance, 'authen': 1, 'dstIn': ''}}
        response = client.service.LoginSvr5(**call_data)
        if response and response.resultCode == 0 and response.DSTOut:
            return response.DSTOut
        return None
    except Exception:
        return None

def upload_document_to_dms(dst, file_stream, metadata):
    """
    Uploads a document to the DMS.
    """
    import db_connector

    svc_client, obj_client = None, None
    created_doc_number, version_id, put_doc_id, stream_id = None, None, None, None
    event_id = metadata.get('event_id')

    try:
        svc_client = get_soap_client('BasicHttpBinding_IDMSvc')
        obj_client = get_soap_client('BasicHttpBinding_IDMObj')

        string_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}string')
        int_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}int')

        logging.info("Step 2: CreateObject - Creating document profile.")  # Changed to print
        string_array_type = svc_client.get_type(
            '{http://schemas.microsoft.com/2003/10/Serialization/Arrays}ArrayOfstring')

        property_names_list = [
            '%TARGET_LIBRARY', '%RECENTLY_USED_LOCATION', 'DOCNAME', 'TYPE_ID', 'AUTHOR_ID',
            'ABSTRACT', 'APP_ID', 'TYPIST_ID', 'SECURITY'
        ]

        property_values_list = [
            xsd.AnyObject(string_type, 'RTA_MAIN'),
            xsd.AnyObject(string_type, 'DOCSOPEN!L\\RTA_MAIN'),
            xsd.AnyObject(string_type, metadata['docname']),
            xsd.AnyObject(string_type, 'DEFAULT'),
            xsd.AnyObject(string_type, DMS_USER),
            xsd.AnyObject(string_type, metadata['abstract']),
            xsd.AnyObject(string_type, metadata['app_id']),
            xsd.AnyObject(string_type, DMS_USER),
            xsd.AnyObject(string_type, '1')
        ]

        doc_date = metadata.get('doc_date')
        if doc_date:
            property_names_list.append('RTADOCDATE')
            property_values_list.append(xsd.AnyObject(string_type, doc_date.strftime('%m/%d/%y')))

        property_names = string_array_type(property_names_list)

        create_object_call = {
            'call': {
                'dstIn': dst,
                'objectType': 'DEF_PROF',
                'properties': {
                    'propertyCount': len(property_names.string),
                    'propertyNames': property_names,
                    'propertyValues': {
                        'anyType': property_values_list
                    }
                }
            }
        }
        create_reply = svc_client.service.CreateObject(**create_object_call)

        if not (create_reply and create_reply.resultCode == 0 and create_reply.retProperties):
            raise Exception("CreateObject failed")

        ret_prop_names = create_reply.retProperties.propertyNames.string
        ret_prop_values = create_reply.retProperties.propertyValues.anyType
        created_doc_number = ret_prop_values[ret_prop_names.index('%OBJECT_IDENTIFIER')]
        version_id = ret_prop_values[ret_prop_names.index('%VERSION_ID')]

        logging.info(
            f"CreateObject successful. New docnumber: {created_doc_number}, VersionID: {version_id}")  # Changed to print

        put_doc_call = {
            'call': {
                'dstIn': dst,
                'libraryName': 'RTA_MAIN',
                'documentNumber': created_doc_number,
                'versionID': version_id
            }
        }
        put_doc_reply = svc_client.service.PutDoc(**put_doc_call)
        if not (put_doc_reply and put_doc_reply.resultCode == 0 and put_doc_reply.putDocID):
            raise Exception(f"PutDoc failed. Result code: {getattr(put_doc_reply, 'resultCode', 'N/A')}")
        put_doc_id = put_doc_reply.putDocID

        get_stream_call = {'call': {'dstIn': dst, 'contentID': put_doc_id}}
        get_stream_reply = obj_client.service.GetWriteStream(**get_stream_call)
        if not (get_stream_reply and get_stream_reply.resultCode == 0 and get_stream_reply.streamID):
            raise Exception(f"GetWriteStream failed. Result code: {getattr(get_stream_reply, 'resultCode', 'N/A')}")
        stream_id = get_stream_reply.streamID

        chunk_size = 48 * 1024
        while True:
            try:
                chunk = file_stream.read(chunk_size)
            except Exception:
                raise Exception(f"Failed to read file stream")

            if not chunk:
                break

            stream_data_type = obj_client.get_type(
                '{http://schemas.datacontract.org/2004/07/OpenText.DMSvr.Serializable}StreamData')
            stream_data_instance = stream_data_type(bufferSize=len(chunk), streamBuffer=chunk)

            write_stream_call = {
                'call': {
                    'streamID': stream_id,
                    'streamData': stream_data_instance
                }
            }
            write_reply = obj_client.service.WriteStream(**write_stream_call)
            if write_reply.resultCode != 0:
                raise Exception(f"WriteStream chunk failed. Result code: {write_reply.resultCode}")

        commit_stream_call = {'call': {'streamID': stream_id, 'flags': 0}}
        commit_reply = obj_client.service.CommitStream(**commit_stream_call)
        if commit_reply.resultCode != 0:
            raise Exception(f"CommitStream failed. Result code: {commit_reply.resultCode}")

        form_prop_names = string_array_type(
            ['%OBJECT_TYPE_ID', '%OBJECT_IDENTIFIER', '%TARGET_LIBRARY', 'FORM'])
        form_prop_values_list = [
            xsd.AnyObject(string_type, 'def_prof'),
            xsd.AnyObject(int_type, created_doc_number),
            xsd.AnyObject(string_type, 'RTA_MAIN'),
            xsd.AnyObject(string_type, '2740')
        ]
        update_form_call = {
            'call': {
                'dstIn': dst,
                'objectType': 'Profile',
                'properties': {
                    'propertyCount': len(form_prop_names.string),
                    'propertyNames': form_prop_names,
                    'propertyValues': {
                        'anyType': form_prop_values_list
                    }
                }
            }
        }
        update_form_reply = svc_client.service.UpdateObject(**update_form_call)

        unlock_prop_names = string_array_type(
            ['%OBJECT_TYPE_ID', '%OBJECT_IDENTIFIER', '%TARGET_LIBRARY', '%STATUS'])

        unlock_prop_values_list = [
            xsd.AnyObject(string_type, 'def_prof'),
            xsd.AnyObject(int_type, created_doc_number),
            xsd.AnyObject(string_type, 'rta_main'),
            xsd.AnyObject(string_type, '%UNLOCK')
        ]

        update_object_call = {
            'call': {
                'dstIn': dst,
                'objectType': 'Profile',
                'properties': {
                    'propertyCount': len(unlock_prop_names.string),
                    'propertyNames': unlock_prop_names,
                    'propertyValues': {
                        'anyType': unlock_prop_values_list
                    }
                }
            }
        }
        update_reply = svc_client.service.UpdateObject(**update_object_call)

        if created_doc_number and event_id is not None:
            db_connector.link_document_to_event(created_doc_number, event_id)

        return created_doc_number

    except Exception as e:
        logging.info(f"DMS upload failed. Error: {e}")  # Changed to print
        return None
    finally:
        if obj_client:
            if stream_id:
                try:
                    obj_client.service.ReleaseObject(call={'objectID': stream_id})
                except Exception:
                    pass
            if put_doc_id:
                try:
                    obj_client.service.ReleaseObject(call={'objectID': put_doc_id})
                except Exception:
                    pass

def get_document_from_dms(dst, doc_number):
    """Retrieves a document's full content."""
    try:
        content_bytes, filename = get_image_by_docnumber(dst, doc_number)
        return content_bytes, filename
    except Exception as e:
        logging.info(f"Error retrieving doc {doc_number}: {e}")  # Changed to print
        return None, None

def get_root_folder_counts(dst):
    """
    Counts the media types (image, video, file) directly under the SMART_EDMS_ROOT_ID folder.
    This function is used by db_connector.py to calculate counts for the main folders page.
    """
    import db_connector

    # Reuse the recursive logic but don't filter by a specific type yet
    # We want counts for ALL types.

    counts = {'images': 0, 'videos': 0, 'files': 0}

    # We can't reuse get_recursive_doc_ids easily because it returns a list for ONE type.
    # So we'll implement a simplified traversal here that counts everything.

    folder_queue = [SMART_EDMS_ROOT_ID]
    processed_folders = set()
    MAX_FOLDERS_TO_SCAN = 100

    search_client = get_soap_client('BasicHttpBinding_IDMSvc')
    data_client = find_client_with_operation('GetDataW') or find_client_with_operation('GetData')
    method_name = 'GetDataW' if hasattr(data_client.service, 'GetDataW') else 'GetData'

    if not data_client:
        return counts

    try:
        while folder_queue:
            if len(processed_folders) >= MAX_FOLDERS_TO_SCAN:
                break

            current_folder_id = folder_queue.pop(0)
            if current_folder_id in processed_folders:
                continue
            processed_folders.add(current_folder_id)

            search_call = {
                'call': {
                    'dstIn': dst,
                    'objectType': 'ContentsCollection',
                    'signature': {
                        'libraries': {'string': ['RTA_MAIN']},
                        'criteria': {
                            'criteriaCount': 1,
                            'criteriaNames': {'string': ['%ITEM']},
                            'criteriaValues': {'string': [str(current_folder_id)]}
                        },
                        'retProperties': {
                            'string': ['FI.DOCNUMBER', '%DISPLAY_NAME', 'FI.NODE_TYPE', 'DOCNAME', 'APPLICATION',
                                       'APP_ID', 'DOSEXTENSION']},
                        'sortProps': {'propertyCount': 1, 'propertyNames': {'string': ['%DISPLAY_NAME']},
                                      'propertyFlags': {'int': [1]}},
                        'maxRows': 0
                    }
                }
            }

            search_reply = search_client.service.Search(**search_call)
            if not (search_reply and search_reply.resultCode == 0 and search_reply.resultSetID):
                continue

            result_set_id = search_reply.resultSetID
            chunk_size = 500
            start_row = 0

            while True:
                get_data_call = {
                    'call': {'resultSetID': result_set_id, 'requestedRows': chunk_size, 'startingRow': start_row}}
                data_reply = getattr(data_client.service, method_name)(**get_data_call)

                items_batch = []
                has_data = False

                # 1. XML
                row_nodes = None
                if hasattr(data_reply, 'rowNode'):
                    row_nodes = data_reply.rowNode
                elif hasattr(data_reply, 'RowNode'):
                    row_nodes = data_reply.RowNode
                elif isinstance(data_reply, dict):
                    row_nodes = data_reply.get('rowNode') or data_reply.get('RowNode')

                if row_nodes:
                    has_data = True
                    for row in row_nodes:
                        try:
                            props = row.propValues.anyType
                            doc_id = props[0]
                            node_type = props[2] if len(props) > 2 else 'N'

                            is_folder = (node_type == 'F')
                            media_type = 'folder' if is_folder else 'resolve'

                            # Simple ext check
                            if not is_folder and len(props) > 6 and props[6]:
                                dos_ext = str(props[6]).lower().replace('.', '').strip()
                                if dos_ext in ['jpg', 'jpeg', 'png', 'gif', 'bmp']:
                                    media_type = 'image'
                                elif dos_ext in ['mp4', 'mov', 'avi', 'mkv']:
                                    media_type = 'video'
                                elif dos_ext in ['pdf', 'doc', 'docx']:
                                    media_type = 'pdf'

                            items_batch.append({'id': str(doc_id), 'media_type': media_type,
                                                'type': 'folder' if is_folder else 'file'})
                        except Exception:
                            pass

                # 2. Binary
                if not items_batch and hasattr(data_reply, 'resultSetData') and data_reply.resultSetData:
                    container = data_reply.resultSetData
                    if hasattr(container, 'resultBuffer') and container.resultBuffer:
                        parsed = parse_binary_result_buffer(container.resultBuffer)
                        if parsed:
                            has_data = True
                            items_batch.extend(parsed)

                if not has_data or not items_batch:
                    break

                ids_to_resolve = []
                for item in items_batch:
                    if item.get('type') == 'folder' or item.get('media_type') == 'folder':
                        if item['id'] not in processed_folders and item['id'] not in folder_queue:
                            folder_queue.append(item['id'])
                        continue

                    m_type = item.get('media_type')
                    if m_type == 'image':
                        counts['images'] += 1
                    elif m_type == 'video':
                        counts['videos'] += 1
                    elif m_type == 'pdf':
                        counts['files'] += 1
                    elif m_type == 'resolve':
                        ids_to_resolve.append(item['id'])

                if ids_to_resolve:
                    try:
                        resolved_map = db_connector.resolve_media_types_from_db(ids_to_resolve)
                        for doc_id in ids_to_resolve:
                            r_type = resolved_map.get(doc_id, 'pdf')
                            if r_type == 'image':
                                counts['images'] += 1
                            elif r_type == 'video':
                                counts['videos'] += 1
                            else:
                                counts['files'] += 1
                    except Exception:
                        counts['files'] += len(ids_to_resolve)  # Default fallback

                start_row += chunk_size
                if start_row > 2000: break

            try:
                search_client.service.ReleaseData(call={'resultSetID': result_set_id})
                search_client.service.ReleaseObject(call={'objectID': result_set_id})
            except:
                pass

    except Exception as e:
        print(f"Error getting root folder counts: {e}")

    return counts