import logging
from zeep import xsd
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

async def get_recursive_doc_ids(dst, media_type_filter=None, search_term=None, start_node=None):
    import db_connector
    root_node = start_node if start_node else SMART_EDMS_ROOT_ID
    matching_docs = []
    folder_queue = [root_node]
    processed_folders = set()
    MAX_FOLDERS_TO_SCAN = 100

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

async def list_folder_contents(dst, parent_id=None, app_source=None, scope=None, media_type=None, search_term=None):
    import db_connector
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

    if (scope == 'folders' and media_type) or search_term:
        start_node = target_id
        return await get_recursive_doc_ids(dst, media_type_filter=media_type, search_term=search_term,
                                           start_node=start_node)

    is_root_view = (target_id == SMART_EDMS_ROOT_ID)
    if is_root_view and not media_type and not search_term:
        try:
            # AWAIT HERE
            counts = await db_connector.get_media_type_counts(app_source, scope=scope)

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
    except Exception:
        return items

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
    return items

async def get_root_folder_counts(dst):
    """
    Counts documents by media type (images, videos, files) recursively from the root.
    """
    try:
        # Pass media_type_filter=None to get all docs
        items = await get_recursive_doc_ids(dst, media_type_filter=None)

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

        # Strategy A: GetDocSvr3
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

        # Strategy B: VersionsSearch (Matches Trace)
        try:
            search_call = {
                'dstIn': dst, 'objectType': 'VersionsSearch',
                'signature': {
                    'libraries': {'string': ['RTA_MAIN']},
                    'criteria': {'criteriaCount': 1, 'criteriaNames': {'string': ['%OBJECT_IDENTIFIER']},
                                 'criteriaValues': {'string': [str(doc_number)]}},
                    'retProperties': {'string': ['VERSION_ID', 'VERSION', 'SUBVERSION']},
                    'maxRows': 0  # Updated to 0 to match trace
                }
            }
            s_reply = svc_client.service.Search(call=search_call)
            if s_reply and s_reply.resultCode == 0 and s_reply.resultSetID:
                d_client = find_client('GetDataW')
                d_meth = 'GetDataW' if hasattr(d_client.service, 'GetDataW') else 'GetData'
                d_reply = getattr(d_client.service, d_meth)(
                    call={'resultSetID': s_reply.resultSetID, 'requestedRows': 1, 'startingRow': 0})

                rows = getattr(d_reply, 'rowNode', []) or getattr(d_reply, 'RowNode', []) or []
                try:
                    svc_client.service.ReleaseObject(call={'objectID': s_reply.resultSetID})
                except:
                    pass

                if rows and rows[0].propValues.anyType:
                    return rows[0].propValues.anyType[0]
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

        # 3. GLOBAL DISCOVERY
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
                p_n = ['%TARGET_LIBRARY', 'SYSTEM_ID']
                p_v = [xsd.AnyObject(string_type, 'RTA_MAIN'), xsd.AnyObject(int_type, int(link['SYSTEM_ID']))]

                if link.get('PARENT') and link.get('PARENT_VERSION'):
                    p_n.append('PARENT')
                    p_v.append(xsd.AnyObject(string_type, str(link['PARENT'])))
                    p_n.append('PARENT_VERSION')
                    p_v.append(xsd.AnyObject(string_type, str(link['PARENT_VERSION'])))

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
            # Using v_defprof and adding %DELETE_OPTION to match Fiddler trace
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

        # Return success if we at least cleared it from the current folder
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
                        print(f"Failed to clear item {d_id} (Link: {s_id}) from folder {folder_id}.")
                        return False

        except Exception as e:
            print(f"Iteration error: {e}")
            return False

    if delete_root:
        return force_delete_item(folder_id)

    return True