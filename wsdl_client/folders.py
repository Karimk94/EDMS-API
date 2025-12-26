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
    Recursively empties a folder following the OpenText deletion pattern.
    Based on successful Fiddler trace analysis.
    """
    import os
    from zeep import Client, Settings, xsd

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
        logging.error("Failed to initialize SOAP clients")
        return False

    # --- Type Definitions ---
    string_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}string')
    int_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}int')
    string_array_type = svc_client.get_type('{http://schemas.microsoft.com/2003/10/Serialization/Arrays}ArrayOfstring')

    def get_where_used_links(doc_id):
        """
        Step 1: Get all ContentItem links using %CONTENTS_WHERE_USED
        This matches the Fiddler trace CreateObject call
        """
        try:
            coll_names = string_array_type(['%TARGET_LIBRARY', 'DOCNUMBER', '%CONTENTS_DIRECTIVE'])
            coll_values = {'anyType': [
                xsd.AnyObject(string_type, 'RTA_MAIN'),
                xsd.AnyObject(string_type, str(doc_id)),
                xsd.AnyObject(string_type, '%CONTENTS_WHERE_USED')
            ]}

            coll_reply = svc_client.service.CreateObject(call={
                'dstIn': dst,
                'objectType': 'ContentsCollection',
                'properties': {
                    'propertyCount': 3,
                    'propertyNames': coll_names,
                    'propertyValues': coll_values
                }
            })

            if not (coll_reply and coll_reply.resultCode == 0 and coll_reply.retProperties):
                return []

            col_id = coll_reply.retProperties.propertyValues.anyType[0]

            # Step 2: Create enumerator (NewEnum)
            enum_client = find_client('NewEnum')
            enum_reply = enum_client.service.NewEnum(call={
                'dstIn': dst,
                'collectionID': col_id
            })

            if not (enum_reply and enum_reply.resultCode == 0 and enum_reply.enumID):
                try:
                    svc_client.service.ReleaseObject(call={'objectID': col_id})
                except:
                    pass
                return []

            enum_id = enum_reply.enumID

            # Step 3: Get data from enumerator (NextData)
            next_reply = enum_client.service.NextData(call={
                'dstIn': dst,
                'enumID': enum_id,
                'elementCount': 500  # Match trace
            })

            links = []
            if next_reply and next_reply.genericItemsData:
                g_data = next_reply.genericItemsData
                p_names = g_data.propertyNames.string
                rows = g_data.propertyRows.ArrayOfanyType

                # Log what properties we actually got
                logging.info(f"WHERE_USED properties for {doc_id}: {p_names}")

                # Get indices for the properties we need
                idx_sys = p_names.index('SYSTEM_ID') if 'SYSTEM_ID' in p_names else -1
                idx_par = p_names.index('PARENT') if 'PARENT' in p_names else -1
                idx_pver = p_names.index('PARENT_VERSION') if 'PARENT_VERSION' in p_names else -1
                idx_doc = p_names.index('DOCNUMBER') if 'DOCNUMBER' in p_names else -1

                # Also check for alternative column names
                if idx_par == -1:
                    idx_par = p_names.index('FOLDERDOCNO_RO') if 'FOLDERDOCNO_RO' in p_names else -1

                if idx_sys != -1 and rows:
                    for row in rows:
                        try:
                            vals = row.anyType
                            parent_val = vals[idx_par] if idx_par != -1 and idx_par < len(vals) else None
                            parent_ver_val = vals[idx_pver] if idx_pver != -1 and idx_pver < len(vals) else None

                            link_data = {
                                'SYSTEM_ID': vals[idx_sys] if idx_sys < len(vals) else None,
                                'PARENT': parent_val,
                                'PARENT_VERSION': parent_ver_val,
                                'DOCNUMBER': vals[idx_doc] if idx_doc != -1 and idx_doc < len(vals) else None
                            }

                            logging.info(
                                f"Found link: sys_id={link_data['SYSTEM_ID']}, parent={link_data['PARENT']}, parent_ver={link_data['PARENT_VERSION']}")
                            links.append(link_data)
                        except Exception as e:
                            logging.error(f"Error parsing WHERE_USED row: {e}")

            # Cleanup
            try:
                enum_client.service.ReleaseObject(call={'objectID': enum_id})
            except:
                pass
            try:
                svc_client.service.ReleaseObject(call={'objectID': col_id})
            except:
                pass

            return links

        except Exception as e:
            logging.error(f"Error getting WHERE_USED links: {e}")
            return []

    def get_parent_version(parent_id):
        """
        Get the current version of a parent folder
        Tries multiple strategies to find the VERSION_ID
        """
        try:
            # Strategy 1: Use GetDocSvr3 (most reliable)
            try:
                get_doc_call = {
                    'call': {
                        'dstIn': dst,
                        'criteria': {
                            'criteriaCount': 2,
                            'criteriaNames': {'string': ['%TARGET_LIBRARY', '%DOCUMENT_NUMBER']},
                            'criteriaValues': {'string': ['RTA_MAIN', str(parent_id)]}
                        }
                    }
                }
                doc_reply = svc_client.service.GetDocSvr3(**get_doc_call)

                if doc_reply and doc_reply.resultCode == 0 and doc_reply.docProperties:
                    p_names = doc_reply.docProperties.propertyNames.string
                    p_vals = doc_reply.docProperties.propertyValues.anyType

                    logging.info(f"GetDocSvr3 properties for {parent_id}: {p_names}")

                    # Try both VERSION_ID variants
                    for key in ['%VERSION_ID', 'VERSION_ID']:
                        if key in p_names:
                            val = p_vals[p_names.index(key)]
                            if val and str(val) != '0':
                                logging.info(f"GetDocSvr3 found version {val} for parent {parent_id}")
                                return val

                    # If VERSION_ID is 0 or missing, try to find any version-related field
                    for i, name in enumerate(p_names):
                        if 'VERSION' in name.upper():
                            logging.info(f"  {name} = {p_vals[i]}")

            except Exception as e:
                logging.debug(f"GetDocSvr3 failed for {parent_id}: {e}")

            # Strategy 2: Search using DEF_PROF (matches Fiddler trace)
            try:
                search_call = {
                    'dstIn': dst,
                    'objectType': '%RTF:RTA_MAIN V_DEFPROF DOCUMENT_LIST %HITLIST',
                    'signature': {
                        'libraries': {'string': ['RTA_MAIN']},
                        'criteria': {
                            'criteriaCount': 1,
                            'criteriaNames': {'string': ['DOCNUM']},
                            'criteriaValues': {'string': [str(parent_id)]}
                        },
                        'retProperties': {
                            'string': ['VERSION_ID', 'DOCNUM', 'DOCNAME', 'STATUS', 'SYSTEM_ID']
                        },
                        'maxRows': 0
                    }
                }

                search_reply = svc_client.service.Search(call=search_call)
                logging.info(
                    f"DEF_PROF search for {parent_id}: resultCode={search_reply.resultCode if search_reply else 'None'}")

                if search_reply and search_reply.resultCode == 0 and search_reply.resultSetID:
                    result_set_id = search_reply.resultSetID
                    data_client = find_client('GetDataW')
                    d_method = 'GetDataW' if hasattr(data_client.service, 'GetDataW') else 'GetData'

                    d_reply = getattr(data_client.service, d_method)(call={
                        'resultSetID': result_set_id,
                        'requestedRows': 2147483647,
                        'startingRow': 0
                    })

                    try:
                        svc_client.service.ReleaseObject(call={'objectID': result_set_id})
                    except:
                        pass

                    rows = getattr(d_reply, 'rowNode', []) or getattr(d_reply, 'RowNode', []) or []
                    logging.info(f"DEF_PROF returned {len(rows)} rows")

                    if rows and rows[0].propValues.anyType:
                        vals = rows[0].propValues.anyType
                        logging.info(f"DEF_PROF values: {vals}")
                        # VERSION_ID is typically the first column
                        version_id = vals[0]
                        if version_id and str(version_id) != '0':
                            logging.info(f"DEF_PROF search found version {version_id} for parent {parent_id}")
                            return version_id
            except Exception as e:
                logging.error(f"DEF_PROF search failed for {parent_id}: {e}", exc_info=True)

            # Strategy 3: VersionsSearch (matches Fiddler exactly)
            try:
                search_call = {
                    'dstIn': dst,
                    'objectType': 'VersionsSearch',
                    'signature': {
                        'libraries': {'string': ['RTA_MAIN']},
                        'criteria': {
                            'criteriaCount': 1,
                            'criteriaNames': {'string': ['%OBJECT_IDENTIFIER']},
                            'criteriaValues': {'string': [str(parent_id)]}
                        },
                        'retProperties': {
                            'string': ['VERSION_ID', 'VERSION_LABEL', 'AUTHOR', 'TYPIST', 'COMMENTS',
                                       'LASTEDITDATE', 'LASTEDITTIME', 'VERSION', 'SUBVERSION',
                                       'STATUS', '%WHERE_READONLY', 'FILE_EXTENSION']
                        },
                        'sortProps': {
                            'propertyCount': 2,
                            'propertyNames': {'string': ['VERSION', 'SUBVERSION']},
                            'propertyFlags': {'int': [2, 2]}  # Descending
                        },
                        'maxRows': 0  # Match Fiddler trace exactly
                    }
                }

                search_reply = svc_client.service.Search(call=search_call)
                logging.info(
                    f"VersionsSearch for {parent_id}: resultCode={search_reply.resultCode if search_reply else 'None'}")

                if search_reply and search_reply.resultCode == 0 and search_reply.resultSetID:
                    result_set_id = search_reply.resultSetID
                    data_client = find_client('GetDataW')
                    d_method = 'GetDataW' if hasattr(data_client.service, 'GetDataW') else 'GetData'

                    d_reply = getattr(data_client.service, d_method)(call={
                        'resultSetID': result_set_id,
                        'requestedRows': 500,
                        'startingRow': 0
                    })

                    try:
                        svc_client.service.ReleaseData(call={'resultSetID': result_set_id})
                        svc_client.service.ReleaseObject(call={'objectID': result_set_id})
                    except:
                        pass

                    rows = getattr(d_reply, 'rowNode', []) or getattr(d_reply, 'RowNode', []) or []
                    logging.info(f"VersionsSearch returned {len(rows)} rows")

                    if rows and rows[0].propValues.anyType:
                        vals = rows[0].propValues.anyType
                        logging.info(f"VersionsSearch values: {vals}")
                        version_id = vals[0]
                        if version_id and str(version_id) != '0':
                            logging.info(f"VersionsSearch found version {version_id} for parent {parent_id}")
                            return version_id
            except Exception as e:
                logging.error(f"VersionsSearch failed for {parent_id}: {e}", exc_info=True)

            logging.error(f"All strategies failed to get version for parent {parent_id}")

        except Exception as e:
            logging.error(f"Error getting parent version for {parent_id}: {e}", exc_info=True)

        return None

    def get_folder_contents(parent_id):
        """
        Get all items inside a folder using ContentsCollection search
        This matches the Fiddler trace Search call for ContentsCollection
        """
        try:
            search_call = {
                'dstIn': dst,
                'objectType': 'ContentsCollection',
                'signature': {
                    'libraries': {'string': ['RTA_MAIN']},
                    'criteria': {
                        'criteriaCount': 1,
                        'criteriaNames': {'string': ['%ITEM']},
                        'criteriaValues': {'string': [str(parent_id)]}
                    },
                    'retProperties': {
                        'string': [
                            'FI.SYSTEM_ID', 'FI.PARENT_LIBRARY', 'FI.LIBRARY',
                            'FI.DOCNUMBER', 'FI.VERSION', 'FI.NEXT', 'FI.STATUS',
                            'FI.ISFIRST', 'FI.PARENT_VERSION', 'FI.VERSION_TYPE',
                            'FI.NODE_TYPE', 'VS.VERSION_LABEL', '%DISPLAY_NAME',
                            '%HAS_SUBFOLDERS'
                        ]
                    },
                    'sortProps': {
                        'propertyCount': 0,
                        'propertyNames': {'string': []},
                        'propertyFlags': {'int': []}
                    },
                    'maxRows': 0
                }
            }

            search_reply = svc_client.service.Search(call=search_call)

            if not (search_reply and search_reply.resultCode == 0 and search_reply.resultSetID):
                return []

            result_set_id = search_reply.resultSetID
            data_client = find_client('GetDataW')
            d_method = 'GetDataW' if hasattr(data_client.service, 'GetDataW') else 'GetData'

            d_reply = getattr(data_client.service, d_method)(call={
                'resultSetID': result_set_id,
                'requestedRows': 2147483647,  # Match trace - get all rows
                'startingRow': 0
            })

            items = []
            rows = getattr(d_reply, 'rowNode', []) or getattr(d_reply, 'RowNode', []) or []
            if rows:
                for row in rows:
                    if row.propValues.anyType and len(row.propValues.anyType) >= 11:
                        vals = row.propValues.anyType
                        items.append({
                            'SYSTEM_ID': vals[0],  # FI.SYSTEM_ID
                            'DOCNUMBER': vals[3],  # FI.DOCNUMBER
                            'NODE_TYPE': vals[10],  # FI.NODE_TYPE
                            'PARENT_VERSION': vals[8]  # FI.PARENT_VERSION
                        })

            # Cleanup
            try:
                svc_client.service.ReleaseData(call={'resultSetID': result_set_id})
                svc_client.service.ReleaseObject(call={'objectID': result_set_id})
            except:
                pass

            return items

        except Exception as e:
            logging.error(f"Error getting folder contents for {parent_id}: {e}")
            return []

    def delete_content_item(system_id, parent_id, parent_version):
        """
        Delete a ContentItem link
        This matches the Fiddler trace DeleteObject call for ContentItem
        """
        try:
            # If parent_version is 0 or None, try to get it first
            effective_parent_version = parent_version
            if not effective_parent_version or str(effective_parent_version) == '0':
                logging.info(f"Parent version is 0, checking if parent {parent_id} is a system folder...")
                # For system folders or folders without versions, we might need different approach
                effective_parent_version = parent_version  # Keep as 0

            # Attempt 1: Standard deletion with all parameters
            try:
                prop_names_list = ['%TARGET_LIBRARY', 'PARENT', 'PARENT_VERSION', 'SYSTEM_ID']
                prop_values_list = [
                    xsd.AnyObject(string_type, 'RTA_MAIN'),
                    xsd.AnyObject(string_type, str(parent_id)),
                    xsd.AnyObject(string_type, str(effective_parent_version)),
                    xsd.AnyObject(int_type, int(system_id))
                ]

                del_call = {
                    'call': {
                        'dstIn': dst,
                        'objectType': 'ContentItem',
                        'properties': {
                            'propertyCount': 4,
                            'propertyNames': string_array_type(prop_names_list),
                            'propertyValues': {'anyType': prop_values_list}
                        }
                    }
                }

                resp = del_client.service.DeleteObject(**del_call)
                if resp.resultCode == 0:
                    logging.info(f"Successfully deleted ContentItem {system_id}")
                    return True
                else:
                    logging.warning(f"DeleteObject with PARENT_VERSION failed: {resp.resultCode}")
            except Exception as e:
                logging.warning(f"Standard ContentItem delete failed: {e}")

            # Attempt 2: Try with just SYSTEM_ID and PARENT (no version)
            try:
                logging.info(f"Trying ContentItem delete without PARENT_VERSION...")
                prop_names_list = ['%TARGET_LIBRARY', 'PARENT', 'SYSTEM_ID']
                prop_values_list = [
                    xsd.AnyObject(string_type, 'RTA_MAIN'),
                    xsd.AnyObject(string_type, str(parent_id)),
                    xsd.AnyObject(int_type, int(system_id))
                ]

                del_call = {
                    'call': {
                        'dstIn': dst,
                        'objectType': 'ContentItem',
                        'properties': {
                            'propertyCount': 3,
                            'propertyNames': string_array_type(prop_names_list),
                            'propertyValues': {'anyType': prop_values_list}
                        }
                    }
                }

                resp = del_client.service.DeleteObject(**del_call)
                if resp.resultCode == 0:
                    logging.info(f"Successfully deleted ContentItem {system_id} without PARENT_VERSION")
                    return True
                else:
                    logging.warning(f"DeleteObject without PARENT_VERSION also failed: {resp.resultCode}")
            except Exception as e:
                logging.warning(f"ContentItem delete without version failed: {e}")

            # Attempt 3: Try with just SYSTEM_ID (last resort)
            try:
                logging.info(f"Trying ContentItem delete with only SYSTEM_ID...")
                prop_names_list = ['%TARGET_LIBRARY', 'SYSTEM_ID']
                prop_values_list = [
                    xsd.AnyObject(string_type, 'RTA_MAIN'),
                    xsd.AnyObject(int_type, int(system_id))
                ]

                del_call = {
                    'call': {
                        'dstIn': dst,
                        'objectType': 'ContentItem',
                        'properties': {
                            'propertyCount': 2,
                            'propertyNames': string_array_type(prop_names_list),
                            'propertyValues': {'anyType': prop_values_list}
                        }
                    }
                }

                resp = del_client.service.DeleteObject(**del_call)
                if resp.resultCode == 0:
                    logging.info(f"Successfully deleted ContentItem {system_id} with only SYSTEM_ID")
                    return True
                else:
                    logging.error(f"All delete attempts failed for ContentItem {system_id}")
            except Exception as e:
                logging.error(f"Final ContentItem delete attempt failed: {e}")

            return False

        except Exception as e:
            logging.error(f"Error deleting ContentItem {system_id}: {e}")
            return False

    def verify_no_links_remain(doc_id):
        """
        Verify no links remain using SQL query
        This matches the Fiddler trace SQLService call
        """
        try:
            sql_client = find_client('SQLService')
            if not sql_client:
                return True  # Can't verify, proceed anyway

            sql_query = f"(SELECT ENTITYID FROM DOCSADM.REGISTRY WHERE ENTITYID IN (SELECT ENTITYID FROM DOCSADM.REGISTRY WHERE NAME = 'LINKID' AND DATA LIKE '{doc_id}') AND NAME = 'LIBRARY' AND DATA LIKE 'RTA_MAIN')"

            sql_call = {
                'call': {
                    'dstIn': dst,
                    'libraryName': 'RTA_MAIN',
                    'sql': sql_query
                }
            }

            resp = sql_client.service.SQLService(**sql_call)

            # param1 = 0 means no links found (safe to delete)
            if resp.resultCode == 0 and resp.param1 == 0:
                return True

            return False

        except Exception as e:
            logging.warning(f"Error verifying links for {doc_id}: {e}")
            return True  # If verification fails, try deletion anyway

    def delete_profile(doc_id):
        """
        Delete the document profile using v_defprof
        This matches the Fiddler trace final DeleteObject call
        """
        try:
            prop_names_list = ['%TARGET_LIBRARY', '%OBJECT_IDENTIFIER', '%DELETE_OPTION']
            prop_values_list = [
                xsd.AnyObject(string_type, 'RTA_MAIN'),
                xsd.AnyObject(string_type, str(doc_id)),
                xsd.AnyObject(string_type, '')  # Empty string as in trace
            ]

            del_call = {
                'call': {
                    'dstIn': dst,
                    'objectType': 'v_defprof',  # Use v_defprof as in trace
                    'properties': {
                        'propertyCount': 3,
                        'propertyNames': string_array_type(prop_names_list),
                        'propertyValues': {'anyType': prop_values_list}
                    }
                }
            }

            resp = del_client.service.DeleteObject(**del_call)
            return resp.resultCode == 0

        except Exception as e:
            logging.error(f"Error deleting profile {doc_id}: {e}")
            return False

    # =====================================================
    # MAIN DELETION LOGIC - Following Fiddler Trace Pattern
    # =====================================================

    try:
        # Step 1: Get all contents of the folder
        contents = get_folder_contents(folder_id)

        logging.info(f"Found {len(contents)} items in folder {folder_id}")

        # Step 2: Recursively delete child folders first, then files
        for item in contents:
            doc_number = item['DOCNUMBER']
            node_type = item['NODE_TYPE']
            system_id = item['SYSTEM_ID']
            parent_version = item['PARENT_VERSION']

            if node_type == 'F':
                # It's a folder - recurse
                logging.info(f"Recursively deleting child folder {doc_number}")
                success = await delete_folder_contents(dst, doc_number, delete_root=True)
                if not success:
                    logging.warning(f"Failed to delete child folder {doc_number}")
            else:
                # It's a document - first remove ALL its references
                logging.info(f"Processing document {doc_number}")
                doc_links = get_where_used_links(doc_number)

                for doc_link in doc_links:
                    link_parent = doc_link.get('PARENT')
                    link_parent_ver = doc_link.get('PARENT_VERSION')
                    link_sys_id = doc_link.get('SYSTEM_ID')

                    if link_parent and link_sys_id:
                        if not link_parent_ver or str(link_parent_ver) == '0':
                            link_parent_ver = get_parent_version(link_parent)
                            if not link_parent_ver:
                                logging.warning(
                                    f"Could not get version for parent {link_parent}, will try deletion anyway")

                        logging.info(
                            f"Attempting to delete doc link: sys_id={link_sys_id}, parent={link_parent}, ver={link_parent_ver}")
                        delete_content_item(link_sys_id, link_parent, link_parent_ver)

                # Verify and delete the document profile
                if verify_no_links_remain(doc_number):
                    delete_profile(doc_number)
                else:
                    logging.warning(f"Links still remain for document {doc_number} after cleanup attempt")

            # Also delete the ContentItem link from the current folder
            logging.info(
                f"Attempting to delete item link from current folder: sys_id={system_id}, parent={folder_id}, ver={parent_version}")
            delete_content_item(system_id, folder_id, parent_version)

        # Step 3: If we're supposed to delete the root folder itself
        if delete_root:
            logging.info(f"Deleting root folder {folder_id}")

            # Get WHERE_USED links for this folder (what's referencing it)
            links = get_where_used_links(folder_id)

            logging.info(f"Found {len(links)} references to folder {folder_id}")

            # Delete all ContentItem links pointing to this folder
            for link in links:
                parent_id = link.get('PARENT')
                parent_version = link.get('PARENT_VERSION')
                system_id = link.get('SYSTEM_ID')

                if parent_id and system_id:
                    # If parent_version is missing or 0, try to get it
                    if not parent_version or str(parent_version) == '0':
                        logging.info(f"Getting version for parent {parent_id}")
                        parent_version = get_parent_version(parent_id)
                        if not parent_version:
                            logging.warning(f"Could not get version for parent {parent_id}, will try deletion anyway")

                    # ALWAYS attempt deletion, even if parent_version is 0/None
                    # The delete_content_item function now has fallback strategies
                    logging.info(
                        f"Attempting to delete folder link: sys_id={system_id}, parent={parent_id}, ver={parent_version}")
                    success = delete_content_item(system_id, parent_id, parent_version)
                    if not success:
                        logging.error(f"Failed to delete link sys_id={system_id}")
                else:
                    logging.warning(f"Incomplete link data: parent_id={parent_id}, system_id={system_id}")

            # Verify no links remain before final deletion
            if verify_no_links_remain(folder_id):
                logging.info(f"No links remain, deleting profile for {folder_id}")
                return delete_profile(folder_id)
            else:
                logging.error(f"Links still remain for folder {folder_id} after all cleanup attempts")
                # Try one more aggressive approach - search for ALL possible links
                logging.info("Attempting aggressive cleanup...")

                # Search for any ContentItems that reference this folder
                try:
                    search_call = {
                        'dstIn': dst,
                        'objectType': 'ContentsCollection',
                        'signature': {
                            'libraries': {'string': ['RTA_MAIN']},
                            'criteria': {
                                'criteriaCount': 2,
                                'criteriaNames': {'string': ['%OBJECT_TYPE_ID', '%ITEM']},
                                'criteriaValues': {
                                    'string': ['%RTF:RTA_MAIN V_DEFPROF DOCUMENT_LIST %HITLIST', str(folder_id)]}
                            },
                            'retProperties': {
                                'string': ['FI.SYSTEM_ID', 'FI.PARENT', 'FI.PARENT_VERSION', 'FI.DOCNUMBER']
                            },
                            'maxRows': 0
                        }
                    }

                    search_reply = svc_client.service.Search(call=search_call)

                    if search_reply and search_reply.resultCode == 0 and search_reply.resultSetID:
                        result_set_id = search_reply.resultSetID
                        data_client = find_client('GetDataW')
                        d_method = 'GetDataW' if hasattr(data_client.service, 'GetDataW') else 'GetData'

                        d_reply = getattr(data_client.service, d_method)(call={
                            'resultSetID': result_set_id,
                            'requestedRows': 2147483647,
                            'startingRow': 0
                        })

                        rows = getattr(d_reply, 'rowNode', []) or getattr(d_reply, 'RowNode', []) or []
                        for row in rows:
                            if row.propValues.anyType:
                                vals = row.propValues.anyType
                                sys_id = vals[0] if len(vals) > 0 else None
                                par_id = vals[1] if len(vals) > 1 else None
                                par_ver = vals[2] if len(vals) > 2 else None

                                if sys_id and par_id:
                                    if not par_ver or str(par_ver) == '0':
                                        par_ver = get_parent_version(par_id)
                                    if par_ver:
                                        logging.info(f"Aggressive cleanup: deleting sys_id={sys_id}")
                                        delete_content_item(sys_id, par_id, par_ver)

                        try:
                            svc_client.service.ReleaseData(call={'resultSetID': result_set_id})
                            svc_client.service.ReleaseObject(call={'objectID': result_set_id})
                        except:
                            pass

                    # Try deletion again
                    if verify_no_links_remain(folder_id):
                        return delete_profile(folder_id)
                    else:
                        return False

                except Exception as e:
                    logging.error(f"Aggressive cleanup failed: {e}")
                    return False

        return True

    except Exception as e:
        logging.error(f"Error in delete_folder_contents: {e}", exc_info=True)
        return False