import logging
from .base import get_soap_client, find_client_with_operation
from .config import SMART_EDMS_ROOT_ID, DMS_USER
from .utils import parse_binary_result_buffer
from .documents import get_doc_version_info
import os
from zeep import Client, Settings, xsd

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
        return None

async def get_recursive_doc_ids(dst, media_type_filter=None, search_term=None, start_node=None, username=None, user_groups=None, permission_checker=None):
    import db_connector
    from .users import get_object_trustees, get_groups_for_user
    
    root_node = start_node if start_node else SMART_EDMS_ROOT_ID
    matching_docs = []
    folder_queue = [root_node]
    processed_folders = set()
    MAX_FOLDERS_TO_SCAN = 100

    # Get user's groups for permission checking if not already provided
    if username and user_groups is None:
        try:
            groups_data = get_groups_for_user(dst, username)
            user_groups = [g.get('group_id', '').upper() for g in groups_data if g.get('group_id')]
        except Exception as e:
            logging.warning(f"Could not fetch groups for user {username}: {e}")
            user_groups = []
    
    def folder_has_permission(folder_id):
        """Check if user has permission to see this folder based on trustees."""
        # Use external permission checker if provided
        if permission_checker:
            return permission_checker(folder_id)
        
        if not username:
            return True  # No user filtering if username not provided
        
        try:
            trustees = get_object_trustees(dst, folder_id)
            if not trustees:
                return True  # No trustees means folder is accessible to all
            
            # Check if user is directly in trustees list or via group
            for trustee in trustees:
                trustee_name = trustee.get('username', '').upper()
                if trustee_name == username.upper():
                    return True
                if user_groups and trustee_name in user_groups:
                    return True
            
            return False
        except Exception as e:
            logging.warning(f"Could not check permissions for folder {folder_id}: {e}")
            return True  # On error, allow access

    search_client = get_soap_client('BasicHttpBinding_IDMSvc')
    data_client = find_client_with_operation('GetDataW') or find_client_with_operation('GetData')
    method_name = 'GetDataW' if hasattr(data_client.service, 'GetDataW') else 'GetData'

    if not data_client: return []

    try:
        while folder_queue:
            if len(processed_folders) >= MAX_FOLDERS_TO_SCAN: break
            current_folder_id = folder_queue.pop(0)
            if current_folder_id in processed_folders: continue
            processed_folders.add(current_folder_id)

            search_call = {
                'call': {'dstIn': dst, 'objectType': 'ContentsCollection',
                         'signature': {'libraries': {'string': ['RTA_MAIN']},
                                       'criteria': {'criteriaCount': 1, 'criteriaNames': {'string': ['%ITEM']},
                                                    'criteriaValues': {'string': [str(current_folder_id)]}},
                                       'retProperties': {
                                           'string': ['FI.DOCNUMBER', '%DISPLAY_NAME', 'FI.NODE_TYPE', 'DOCNAME',
                                                      'APPLICATION', 'APP_ID', 'DOSEXTENSION']},
                                       'sortProps': {'propertyCount': 1,
                                                     'propertyNames': {'string': ['%DISPLAY_NAME']},
                                                     'propertyFlags': {'int': [1]}},
                                       'maxRows': 0}}
            }
            search_reply = search_client.service.Search(**search_call)
            if not (search_reply and search_reply.resultCode == 0 and search_reply.resultSetID): continue

            result_set_id = search_reply.resultSetID
            chunk_size = 500
            start_row = 0

            while True:
                get_data_call = {
                    'call': {'resultSetID': result_set_id, 'requestedRows': chunk_size, 'startingRow': start_row}}
                data_reply = getattr(data_client.service, method_name)(**get_data_call)
                items_batch = []
                has_data = False

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
                            name = props[1] if len(props) > 1 else str(doc_id)
                            node_type = props[2] if len(props) > 2 else 'N'
                            is_folder = (node_type == 'F')
                            media_type = 'folder' if is_folder else 'resolve'
                            if not is_folder and len(props) > 6 and props[6]:
                                dos_ext = str(props[6]).lower().replace('.', '').strip()
                                if dos_ext in ['jpg', 'jpeg', 'png', 'gif', 'bmp', 'tif', 'tiff', 'webp', 'heic']:
                                    media_type = 'image'
                                elif dos_ext in ['mp4', 'mov', 'avi', 'mkv', 'wmv', 'flv', 'webm', '3gp']:
                                    media_type = 'video'
                                elif dos_ext in ['pdf', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx', 'txt']:
                                    media_type = 'pdf'
                            items_batch.append({'id': str(doc_id), 'name': name, 'media_type': media_type,
                                                'type': 'folder' if is_folder else 'file'})
                        except Exception:
                            pass

                if not items_batch and hasattr(data_reply, 'resultSetData') and data_reply.resultSetData:
                    container = data_reply.resultSetData
                    if hasattr(container, 'resultBuffer') and container.resultBuffer:
                        parsed = parse_binary_result_buffer(container.resultBuffer)
                        if parsed:
                            has_data = True
                            items_batch.extend(parsed)

                if not has_data or not items_batch: break

                ids_to_resolve = []
                for item in items_batch:
                    if item.get('type') == 'folder' or item.get('media_type') == 'folder':
                        # Check permission before adding folder to queue
                        if not folder_has_permission(item['id']):
                            continue  # Skip folders the user doesn't have permission to see
                        
                        if item['id'] not in processed_folders and item['id'] not in folder_queue:
                            folder_queue.append(item['id'])
                        if search_term and search_term.lower() in item['name'].lower():
                            item['thumbnail_url'] = f"cache/{item['id']}.jpg"
                            matching_docs.append(item)
                        continue

                    if item.get('media_type') == 'resolve':
                        ids_to_resolve.append(item)
                        continue

                    is_match = True
                    if search_term and search_term.lower() not in item['name'].lower():
                        is_match = False

                    if is_match and media_type_filter:
                        item_media_type = item.get('media_type')
                        if media_type_filter == 'files':
                            if item_media_type in ['image', 'video']:
                                is_match = False
                        else:
                            if item_media_type != media_type_filter:
                                is_match = False

                    if is_match:
                        if 'thumbnail_url' not in item:
                            item['thumbnail_url'] = f"cache/{item['id']}.jpg"
                        matching_docs.append(item)

                if ids_to_resolve:
                    try:
                        resolve_ids_only = [x['id'] for x in ids_to_resolve]
                        resolved_map = await db_connector.resolve_media_types_from_db(resolve_ids_only)
                        for resolve_item in ids_to_resolve:
                            doc_id = resolve_item['id']
                            r_type = resolved_map.get(doc_id, 'pdf')
                            resolve_item['media_type'] = r_type

                            media_match = True
                            if media_type_filter:
                                if media_type_filter == 'files':
                                    if r_type in ['image', 'video']:
                                        media_match = False
                                else:
                                    if r_type != media_type_filter:
                                        media_match = False

                            search_match = True
                            if search_term and search_term.lower() not in resolve_item['name'].lower():
                                search_match = False

                            if media_match and search_match:
                                if 'thumbnail_url' not in resolve_item:
                                    resolve_item['thumbnail_url'] = f"cache/{doc_id}.jpg"
                                matching_docs.append(resolve_item)
                    except Exception:
                        pass

                start_row += chunk_size
                if start_row > 2000: break

            try:
                search_client.service.ReleaseData(call={'resultSetID': result_set_id})
                search_client.service.ReleaseObject(call={'objectID': result_set_id})
            except:
                pass
    except Exception:
        pass
    return matching_docs

async def list_folder_contents(dst, parent_id=None, app_source=None, scope=None, media_type=None, search_term=None, username=None):
    import db_connector
    from .users import get_object_trustees, get_groups_for_user
    
    items = []
    if parent_id == 'images':
        media_type = 'image'
        scope = 'folders'
    elif parent_id == 'videos':
        media_type = 'video'
        scope = 'folders'
    elif parent_id == 'files':
        media_type = 'files'
        scope = 'folders'

    target_id = parent_id
    if not target_id or str(target_id).strip() == "" or str(target_id).lower() == "null" or target_id in ['images',
                                                                                                          'videos',
                                                                                                          'files']:
        target_id = SMART_EDMS_ROOT_ID

    # Get user's groups for permission checking
    user_groups = []
    if username:
        try:
            groups_data = get_groups_for_user(dst, username)
            user_groups = [g.get('group_id', '').upper() for g in groups_data if g.get('group_id')]
        except Exception as e:
            logging.warning(f"Could not fetch groups for user {username}: {e}")
    
    def user_has_permission(folder_id):
        """Check if user has permission to see this folder based on trustees."""
        if not username:
            return True  # No user filtering if username not provided
        
        try:
            trustees = get_object_trustees(dst, folder_id)
            if not trustees:
                return True  # No trustees means folder is accessible to all
            
            # Check if user is directly in trustees list
            for trustee in trustees:
                trustee_name = trustee.get('username', '').upper()
                if trustee_name == username.upper():
                    return True
                # Check if any of user's groups is in trustees list
                if trustee_name in user_groups:
                    return True
            
            return False
        except Exception as e:
            logging.warning(f"Could not check permissions for folder {folder_id}: {e}")
            return True  # On error, allow access

    if (scope == 'folders' and media_type) or search_term:
        start_node = target_id
        return await get_recursive_doc_ids(dst, media_type_filter=media_type, search_term=search_term,
                                           start_node=start_node, username=username, user_groups=user_groups)

    is_root_view = (target_id == SMART_EDMS_ROOT_ID)
    if is_root_view and not media_type and not search_term:
        try:
            # AWAIT HERE - pass username for permission-filtered counts
            counts = await db_connector.get_media_type_counts(app_source, scope=scope, username=username)

            def get_cnt(k):
                return counts.get(k, 0) if counts else 0

            items.append({'id': 'images', 'name': 'Images', 'type': 'folder', 'is_standard': True,
                          'count': get_cnt('images')})
            items.append({'id': 'videos', 'name': 'Videos', 'type': 'folder', 'is_standard': True,
                          'count': get_cnt('videos')})
            items.append(
                {'id': 'files', 'name': 'Files', 'type': 'folder', 'is_standard': True, 'count': get_cnt('files')})
        except Exception:
            pass

    try:
        search_client = get_soap_client('BasicHttpBinding_IDMSvc')
        search_call = {'call': {'dstIn': dst, 'objectType': 'ContentsCollection',
                                'signature': {'libraries': {'string': ['RTA_MAIN']},
                                              'criteria': {'criteriaCount': 1, 'criteriaNames': {'string': ['%ITEM']},
                                                           'criteriaValues': {'string': [str(target_id)]}},
                                              'retProperties': {
                                                  'string': ['FI.DOCNUMBER', '%DISPLAY_NAME', 'FI.NODE_TYPE', 'DOCNAME',
                                                             'APPLICATION', 'APP_ID', 'DOSEXTENSION']},
                                              'sortProps': {'propertyCount': 1,
                                                            'propertyNames': {'string': ['%DISPLAY_NAME']},
                                                            'propertyFlags': {'int': [1]}}, 'maxRows': 0}}}
        search_reply = search_client.service.Search(**search_call)
        if not (search_reply and search_reply.resultCode == 0 and search_reply.resultSetID): return items

        result_set_id = search_reply.resultSetID
        data_client = find_client_with_operation('GetDataW') or find_client_with_operation('GetData')
        method_name = 'GetDataW' if hasattr(data_client.service, 'GetDataW') else 'GetData'
        if not data_client: return items

        get_data_call = {'call': {'resultSetID': result_set_id, 'requestedRows': 500, 'startingRow': 0}}
        data_reply = getattr(data_client.service, method_name)(**get_data_call)

        if hasattr(data_reply, 'resultSetData') and data_reply.resultSetData:
            container = data_reply.resultSetData
            if hasattr(container, 'resultBuffer') and container.resultBuffer:
                parsed_items = parse_binary_result_buffer(container.resultBuffer)
                if parsed_items:
                    items.extend(parsed_items)
                    try:
                        search_client.service.ReleaseData(call={'resultSetID': result_set_id})
                        search_client.service.ReleaseObject(call={'objectID': result_set_id})
                    except:
                        pass

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
                        is_folder = (node_type == 'F')
                        media_type_item = 'folder' if is_folder else 'resolve'
                        if name_str.endswith(' D') or name_str.endswith(' N') or name_str.endswith(
                                ' F'): name_str = name_str[:-2]
                        items.append({'id': str(doc_id), 'name': name_str, 'type': 'folder' if is_folder else 'file',
                                      'media_type': media_type_item, 'node_type': str(node_type), 'is_standard': False})
                    except Exception:
                        pass

        try:
            search_client.service.ReleaseData(call={'resultSetID': result_set_id})
            search_client.service.ReleaseObject(call={'objectID': result_set_id})
        except Exception:
            pass
    except Exception as e:
        logging.error(f"Error in SOAP call: {e}")
        return items

    folder_docnumbers = []
    for item in items:
        if item.get('type') == 'folder' and item.get('id') and item['id'].isdigit():
            folder_docnumbers.append(item['id'])

    if folder_docnumbers:
        try:
            system_id_map = await db_connector.get_folder_system_ids(folder_docnumbers)
            # logging.info(f"Found {len(system_id_map)} SYSTEM_IDs for {len(folder_docnumbers)} folders")

            # Add system_id to folder items
            for item in items:
                if item.get('type') == 'folder' and item.get('id'):
                    system_id = system_id_map.get(item['id'])
                    if system_id:
                        item['system_id'] = system_id
                    # else:
                        # Log warning if system_id not found
                        # logging.warning(f"SYSTEM_ID not found in database for folder docnumber: {item['id']}")
        except Exception as e:
            logging.error(f"Error getting SYSTEM_IDs from database: {e}")

    ids_to_resolve = [item['id'] for item in items if item.get('media_type') == 'resolve']
    if ids_to_resolve:
        try:
            resolved_types = await db_connector.resolve_media_types_from_db(ids_to_resolve)
            for item in items:
                if item.get('media_type') == 'resolve':
                    item['media_type'] = resolved_types.get(item['id'], 'pdf')
        except Exception:
            for item in items:
                if item.get('media_type') == 'resolve': item['media_type'] = 'pdf'
    
    # Filter folders based on user permissions
    if username:
        filtered_items = []
        for item in items:
            # Keep standard folders (images, videos, files) - they're virtual
            if item.get('is_standard'):
                filtered_items.append(item)
                continue
            
            # For real folders, check if user has permission
            if item.get('type') == 'folder':
                if user_has_permission(item['id']):
                    filtered_items.append(item)
            else:
                # Keep non-folder items (files)
                filtered_items.append(item)
        
        items = filtered_items
    
    return items

async def get_root_folder_counts(dst, username=None):
    """
    Counts documents by media type (images, videos, files) recursively from the root.
    Only counts items in folders the user has permission to see.
    """
    try:
        # Pass media_type_filter=None to get all docs, with username for permission filtering
        items = await get_recursive_doc_ids(dst, media_type_filter=None, username=username)

        counts = {'images': 0, 'videos': 0, 'files': 0}

        image_exts = {'jpg', 'jpeg', 'png', 'gif', 'bmp', 'tif', 'tiff', 'webp'}
        video_exts = {'mp4', 'mov', 'avi', 'wmv', 'mkv', 'flv', 'webm', '3gp'}
        pdf_exts = {'pdf', 'doc', 'docx', 'txt', 'xls', 'xlsx', 'ppt', 'pptx'}

        for item in items:
            m_type = item.get('media_type', 'resolve')

            if m_type == 'resolve':
                item_name = item.get('name', '')
                if '.' in item_name:
                    ext = item_name.split('.')[-1].lower()
                    if ext in image_exts:
                        m_type = 'image'
                    elif ext in video_exts:
                        m_type = 'video'
                    elif ext in pdf_exts:
                        m_type = 'pdf'
                    else:
                        m_type = 'file'

            # Count based on resolved type
            if m_type == 'image':
                counts['images'] += 1
            elif m_type == 'video':
                counts['videos'] += 1
            else:
                # Group pdf, file, text, excel, powerpoint, etc into 'files'
                counts['files'] += 1

        return counts
    except Exception as e:
        logging.error(f"Error in get_root_folder_counts: {e}")
        return {'images': 0, 'videos': 0, 'files': 0}

async def delete_folder_contents(dst, folder_id, delete_root=True):
    """
    Recursively empties a folder.
    """
    wsdl_url = os.getenv("WSDL_URL")
    settings = Settings(strict=False, xml_huge_tree=True)

    # --- Client Setup ---
    base_client = Client(wsdl_url, settings=settings)

    def find_client(op_name):
        try:
            for service in base_client.wsdl.services.values():
                for port in service.ports.values():
                    if op_name in port.binding.port_type.operations:
                        return Client(wsdl_url, port_name=port.name, settings=settings)
        except Exception:
            pass
        return base_client

    svc_client = base_client if hasattr(base_client.service, 'Search') else find_client('Search')
    del_client = base_client if hasattr(base_client.service, 'DeleteObject') else find_client('DeleteObject')

    if not svc_client or not del_client:
        return False

    # --- Type Definitions ---
    string_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}string')
    int_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}int')
    string_array_type = svc_client.get_type('{http://schemas.microsoft.com/2003/10/Serialization/Arrays}ArrayOfstring')

    def get_current_version_id(doc_number):
        if not doc_number or str(doc_number) == '0': return None

        # Strategy 1: VersionsSearch (Matches Trace)
        try:
            search_call = {
                'dstIn': dst,
                'objectType': 'VersionsSearch',
                'signature': {
                    'libraries': {'string': ['RTA_MAIN']},
                    'criteria': {
                        'criteriaCount': 1,
                        'criteriaNames': {'string': ['%OBJECT_IDENTIFIER']},
                        'criteriaValues': {'string': [str(doc_number)]}
                    },
                    'retProperties': {'string': [
                        'VERSION_ID', 'VERSION_LABEL', 'AUTHOR', 'TYPIST', 'COMMENTS',
                        'LASTEDITDATE', 'LASTEDITTIME', 'VERSION', 'SUBVERSION',
                        'STATUS', '%WHERE_READONLY', 'FILE_EXTENSION'
                    ]},
                    'sortProps': {
                        'propertyCount': 2,
                        'propertyNames': {'string': ['VERSION', 'SUBVERSION']},
                        'propertyFlags': {'int': [2, 2]}
                    },
                    'maxRows': 0
                }
            }
            s_reply = svc_client.service.Search(call=search_call)

            if s_reply and s_reply.resultCode == 0 and s_reply.resultSetID:
                d_client = find_client('GetDataW')
                d_meth = 'GetDataW' if hasattr(d_client.service, 'GetDataW') else 'GetData'

                d_reply = getattr(d_client.service, d_meth)(
                    call={'resultSetID': s_reply.resultSetID, 'requestedRows': 500, 'startingRow': 0})

                try:
                    svc_client.service.ReleaseObject(call={'objectID': s_reply.resultSetID})
                except:
                    pass

                # 1. Try Binary Buffer (Priority from Trace)
                if hasattr(d_reply, 'resultSetData') and d_reply.resultSetData:
                    container = d_reply.resultSetData
                    if hasattr(container, 'resultBuffer') and container.resultBuffer:
                        parsed = parse_binary_result_buffer(container.resultBuffer)
                        if parsed and isinstance(parsed, list) and len(parsed) > 0:
                            # Assuming first item, and looking for VERSION_ID or first value
                            first_item = parsed[0]
                            if isinstance(first_item, dict):
                                if 'VERSION_ID' in first_item:
                                    return first_item['VERSION_ID']
                                # If dict but no VERSION_ID key, likely positional, take first value
                                return list(first_item.values())[0]

                # 2. Try XML RowNode
                rows = getattr(d_reply, 'rowNode', []) or getattr(d_reply, 'RowNode', []) or []
                if rows:
                    for row in rows:
                        vals = row.propValues.anyType
                        if vals and vals[0]:
                            return vals[0]
        except Exception:
            pass

        # Strategy 2: GetDocSvr3 (Fallback)
        try:
            call_data = {
                'dstIn': dst,
                'criteria': {
                    'criteriaCount': 2,
                    'criteriaNames': {'string': ['%TARGET_LIBRARY', '%DOCUMENT_NUMBER']},
                    'criteriaValues': {'string': ['RTA_MAIN', str(doc_number)]}
                }
            }
            reply = svc_client.service.GetDocSvr3(call=call_data)
            if reply and reply.resultCode == 0 and reply.docProperties:
                p_names = reply.docProperties.propertyNames.string
                p_vals = reply.docProperties.propertyValues.anyType
                for key in ['%VERSION_ID', 'VERSION_ID']:
                    if key in p_names:
                        val = p_vals[p_names.index(key)]
                        if val and str(val) != '0': return val
        except Exception:
            pass

        return None

    current_folder_version = get_current_version_id(folder_id)

    def force_delete_item(item_id, direct_link_id=None, known_parent_id=None, known_parent_ver=None):
        unlinked_current = False

        # 1. UNLOCK
        try:
            u_p = ['%TARGET_LIBRARY', '%OBJECT_IDENTIFIER', '%STATUS']
            u_v = [xsd.AnyObject(string_type, 'RTA_MAIN'), xsd.AnyObject(int_type, int(item_id)),
                   xsd.AnyObject(string_type, '%UNLOCK')]
            svc_client.service.UpdateObject(call={
                'dstIn': dst, 'objectType': 'DEF_PROF',
                'properties': {'propertyCount': 3, 'propertyNames': string_array_type(u_p),
                               'propertyValues': {'anyType': u_v}}
            })
        except Exception:
            pass

        links_to_remove = []

        # 2. IMMEDIATE UNLINK
        if direct_link_id and known_parent_id and known_parent_ver:
            links_to_remove.append({
                'SYSTEM_ID': direct_link_id,
                'PARENT': known_parent_id,
                'PARENT_VERSION': known_parent_ver,
                'IS_DIRECT': True
            })
        elif direct_link_id:
            links_to_remove.append({'SYSTEM_ID': direct_link_id, 'IS_DIRECT': True})

        # 3. GLOBAL DISCOVERY (Where Used)
        try:
            coll_names = string_array_type(['%TARGET_LIBRARY', 'DOCNUMBER', '%CONTENTS_DIRECTIVE'])
            coll_values = {'anyType': [xsd.AnyObject(string_type, 'RTA_MAIN'),
                                       xsd.AnyObject(string_type, str(item_id)),
                                       xsd.AnyObject(string_type, '%CONTENTS_WHERE_USED')]}

            coll_reply = svc_client.service.CreateObject(call={
                'dstIn': dst, 'objectType': 'ContentsCollection',
                'properties': {'propertyCount': 3, 'propertyNames': coll_names, 'propertyValues': coll_values}})

            if coll_reply and coll_reply.resultCode == 0 and coll_reply.retProperties:
                col_id = coll_reply.retProperties.propertyValues.anyType[0]
                enum_client = find_client('NewEnum')
                enum_reply = enum_client.service.NewEnum(call={'dstIn': dst, 'collectionID': col_id})

                if enum_reply and enum_reply.resultCode == 0 and enum_reply.enumID:
                    next_reply = enum_client.service.NextData(
                        call={'dstIn': dst, 'enumID': enum_reply.enumID, 'elementCount': 100})

                    if next_reply and next_reply.genericItemsData:
                        g_data = next_reply.genericItemsData
                        p_names = g_data.propertyNames.string
                        rows = g_data.propertyRows.ArrayOfanyType

                        idx_sys = p_names.index('SYSTEM_ID') if 'SYSTEM_ID' in p_names else -1
                        idx_par = p_names.index('PARENT') if 'PARENT' in p_names else -1
                        idx_ver = p_names.index('PARENT_VERSION') if 'PARENT_VERSION' in p_names else -1

                        if idx_sys != -1 and rows:
                            for row in rows:
                                try:
                                    s_id = row.anyType[idx_sys]
                                    if not direct_link_id or str(s_id) != str(direct_link_id):
                                        link = {'SYSTEM_ID': s_id}
                                        parent_id = row.anyType[idx_par] if idx_par != -1 else None
                                        p_ver = row.anyType[idx_ver] if idx_ver != -1 else None

                                        if parent_id:
                                            link['PARENT'] = parent_id
                                            if not p_ver or str(p_ver) == '0':
                                                p_ver = get_current_version_id(parent_id)
                                            link['PARENT_VERSION'] = p_ver
                                        links_to_remove.append(link)
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
        except Exception:
            pass

        # 4. EXECUTE LINK DELETION
        for link in links_to_remove:
            try:
                # Order matters: %TARGET_LIBRARY, PARENT, PARENT_VERSION, SYSTEM_ID
                p_n = ['%TARGET_LIBRARY']
                p_v = [xsd.AnyObject(string_type, 'RTA_MAIN')]

                if link.get('PARENT') and link.get('PARENT_VERSION'):
                    p_n.append('PARENT')
                    p_v.append(xsd.AnyObject(string_type, str(link['PARENT'])))
                    p_n.append('PARENT_VERSION')
                    p_v.append(xsd.AnyObject(string_type, str(link['PARENT_VERSION'])))

                p_n.append('SYSTEM_ID')
                p_v.append(xsd.AnyObject(int_type, int(link['SYSTEM_ID'])))

                resp = del_client.service.DeleteObject(call={
                    'dstIn': dst, 'objectType': 'ContentItem',
                    'properties': {'propertyCount': len(p_n), 'propertyNames': string_array_type(p_n),
                                   'propertyValues': {'anyType': p_v}}
                })

                if resp.resultCode == 0:
                    if link.get('IS_DIRECT'):
                        unlinked_current = True
            except Exception:
                pass

        # 5. DELETE PROFILE (Global Destroy)
        try:
            del_props = {'propertyCount': 3,
                         'propertyNames': string_array_type(
                             ['%TARGET_LIBRARY', '%OBJECT_IDENTIFIER', '%DELETE_OPTION']),
                         'propertyValues': {'anyType': [
                             xsd.AnyObject(string_type, 'RTA_MAIN'),
                             xsd.AnyObject(int_type, int(item_id)),
                             xsd.AnyObject(string_type, '')
                         ]}}
            resp = del_client.service.DeleteObject(
                call={'dstIn': dst, 'objectType': 'v_defprof', 'properties': del_props})
            if resp.resultCode == 0:
                return True
        except Exception:
            pass

        return unlinked_current

    # --- Iteration Loop ---
    while True:
        try:
            search_call = {
                'dstIn': dst, 'objectType': 'ContentsCollection',
                'signature': {
                    'libraries': {'string': ['RTA_MAIN']},
                    'criteria': {'criteriaCount': 1, 'criteriaNames': {'string': ['%ITEM']},
                                 'criteriaValues': {'string': [str(folder_id)]}},
                    'retProperties': {'string': ['FI.DOCNUMBER', 'FI.NODE_TYPE', '%DISPLAY_NAME', 'SYSTEM_ID']},
                    'sortProps': {'propertyCount': 1, 'propertyNames': {'string': ['%DISPLAY_NAME']},
                                  'propertyFlags': {'int': [1]}},
                    'maxRows': 0
                }
            }

            search_reply = svc_client.service.Search(call=search_call)

            if not (search_reply and search_reply.resultCode == 0 and search_reply.resultSetID):
                break

            rs_id = search_reply.resultSetID
            data_client = find_client('GetDataW')
            d_method = 'GetDataW' if hasattr(data_client.service, 'GetDataW') else 'GetData'

            d_reply = getattr(data_client.service, d_method)(
                call={'resultSetID': rs_id, 'requestedRows': 50, 'startingRow': 0})

            items = []
            rows = getattr(d_reply, 'rowNode', []) or getattr(d_reply, 'RowNode', []) or []
            if rows:
                for row in rows:
                    if row.propValues.anyType:
                        vals = row.propValues.anyType
                        if len(vals) >= 1:
                            d_id = vals[0]
                            n_type = vals[1] if len(vals) > 1 else 'N'
                            s_id = vals[3] if len(vals) > 3 else None
                            items.append((d_id, n_type, s_id))

            try:
                svc_client.service.ReleaseObject(call={'objectID': rs_id})
            except:
                pass

            if not items:
                break

            for d_id, n_type, s_id in items:
                if n_type == 'F':
                    await delete_folder_contents(dst, d_id, delete_root=True)
                else:
                    success = force_delete_item(d_id, direct_link_id=s_id, known_parent_id=folder_id,
                                                known_parent_ver=current_folder_version)
                    if not success:
                        return False

        except Exception as e:
            logging.error(f"Iteration error: {e}")
            return False

    if delete_root:
        return force_delete_item(folder_id)

    return True