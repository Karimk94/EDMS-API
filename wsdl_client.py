import os
import logging
import re
import zlib
from zeep import Client, Settings, xsd
from zeep.exceptions import Fault
from dotenv import load_dotenv

load_dotenv()

WSDL_URL = os.getenv("WSDL_URL")
DMS_USER = os.getenv("DMS_USER")
DMS_PASSWORD = os.getenv("DMS_PASSWORD")

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
    except Exception:
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
        return None

def parse_binary_result_buffer(buffer):
    items = []
    try:
        try:
            if len(buffer) > 8 and buffer[8:10] == b'\x78\x9c':
                decompressed = zlib.decompress(buffer[8:])
                buffer = decompressed
        except Exception:
            pass

        try:
            raw_text = buffer.decode('utf-16-le', errors='ignore')
        except:
            raw_text = buffer.decode('utf-8', errors='ignore')

        clean_text = re.sub(r'[^\w\s\-\.]', ' ', raw_text)
        tokens = clean_text.split()
        FOLDER_APPS = {'FOLDER', 'DEF_PROF', 'SAVED_SEARCHES', 'CONTENTSCOLLECTION'}
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
            if token.isdigit() and len(token) >= 5:
                if i + 1 < len(tokens):
                    chunk_tokens = []
                    j = i + 1
                    while j < len(tokens):
                        next_token = tokens[j]
                        if next_token.isdigit() and len(next_token) >= 5: break
                        chunk_tokens.append(next_token)
                        j += 1
                    i = j - 1

                    if chunk_tokens:
                        item_type = 'folder'
                        media_type = 'folder'
                        is_folder = False
                        if chunk_tokens[-1] == 'F' or (len(chunk_tokens) > 1 and chunk_tokens[-2] == 'F'):
                            is_folder = True
                        else:
                            for t in chunk_tokens:
                                if t.upper() in FOLDER_APPS:
                                    is_folder = True
                                    break
                        if not is_folder:
                            item_type = 'file'
                            media_type = 'resolve'
                            for t in chunk_tokens:
                                if '.' in t:
                                    ext = t.split('.')[-1].lower()
                                    if ext in EXT_MAP:
                                        media_type = EXT_MAP[ext]
                                        break
                        name_parts = []
                        for t in chunk_tokens:
                            if t not in ['N', 'D', 'F'] and t.upper() not in FOLDER_APPS:
                                name_parts.append(t)
                        full_name = " ".join(name_parts).strip()
                        if len(full_name) > 0:
                            items.append({
                                'id': token, 'name': full_name, 'type': item_type,
                                'media_type': media_type, 'node_type': 'F' if is_folder else 'N', 'is_standard': False
                            })
            i += 1
    except Exception:
        pass
    return items

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
                                if dos_ext in ['jpg', 'jpeg', 'png', 'gif', 'bmp']:
                                    media_type = 'image'
                                elif dos_ext in ['mp4', 'mov', 'avi', 'mkv']:
                                    media_type = 'video'
                                elif dos_ext in ['pdf', 'doc', 'docx']:
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

                    is_match = True
                    if search_term and search_term.lower() not in item['name'].lower(): is_match = False
                    if is_match and media_type_filter:
                        m_type = item.get('media_type')
                        if m_type == 'resolve':
                            ids_to_resolve.append(item)
                            is_match = False
                        elif m_type != media_type_filter:
                            is_match = False

                    if is_match:
                        if 'thumbnail_url' not in item: item['thumbnail_url'] = f"cache/{item['id']}.jpg"
                        matching_docs.append(item)

                if ids_to_resolve:
                    try:
                        resolve_ids_only = [x['id'] for x in ids_to_resolve]
                        resolved_map = await db_connector.resolve_media_types_from_db(resolve_ids_only)
                        for resolve_item in ids_to_resolve:
                            doc_id = resolve_item['id']
                            r_type = resolved_map.get(doc_id, 'pdf')
                            media_match = True
                            if media_type_filter and r_type != media_type_filter: media_match = False
                            search_match = True
                            if search_term and search_term.lower() not in resolve_item[
                                'name'].lower(): search_match = False
                            if media_match and search_match:
                                resolve_item['media_type'] = r_type
                                if 'thumbnail_url' not in resolve_item: resolve_item[
                                    'thumbnail_url'] = f"cache/{doc_id}.jpg"
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
        media_type = 'image';
        scope = 'folders'
    elif parent_id == 'videos':
        media_type = 'video';
        scope = 'folders'
    elif parent_id == 'files':
        media_type = 'pdf';
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

def delete_document(dst, doc_number, force=False):
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
            except Exception:
                pass

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
                        del_client.service.DeleteObject(**del_link_call)
                    except Exception:
                        pass

        del_props = {'propertyCount': 2, 'propertyNames': string_array_type(['%TARGET_LIBRARY', '%OBJECT_IDENTIFIER']),
                     'propertyValues': {
                         'anyType': [xsd.AnyObject(string_type, 'RTA_MAIN'), xsd.AnyObject(int_type, int(doc_number))]}}

        try:
            resp = del_client.service.DeleteObject(
                call={'dstIn': dst, 'objectType': 'DEF_PROF', 'properties': del_props})
            if resp.resultCode == 0: return True, "Success"
            return False, str(getattr(resp, 'errorDoc', ''))
        except Fault as f:
            return False, f.message or str(f)
    except Exception as e:
        return False, str(e)

def get_image_by_docnumber(dst, doc_number):
    svc_client, obj_client, content_id, stream_id = None, None, None, None
    try:
        svc_client = get_soap_client('BasicHttpBinding_IDMSvc')
        obj_client = get_soap_client('BasicHttpBinding_IDMObj')
        get_doc_call = {'call': {'dstIn': dst, 'criteria': {'criteriaCount': 3, 'criteriaNames': {
            'string': ['%TARGET_LIBRARY', '%DOCUMENT_NUMBER', '%VERSION_ID']}, 'criteriaValues': {
            'string': ['RTA_MAIN', str(doc_number), '%VERSION_TO_INDEX']}}}}
        doc_reply = svc_client.service.GetDocSvr3(**get_doc_call)
        if not (doc_reply and doc_reply.resultCode == 0 and doc_reply.getDocID): return None, None
        content_id = doc_reply.getDocID
        stream_reply = obj_client.service.GetReadStream(call={'dstIn': dst, 'contentID': content_id})
        if not (stream_reply and stream_reply.resultCode == 0 and stream_reply.streamID): raise Exception(
            "Failed to get read stream.")
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
                    if extension: filename = f"{doc_number}{extension}"
            except Exception:
                pass
        return doc_buffer, filename
    except Fault:
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
    try:
        svc_client = get_soap_client('BasicHttpBinding_IDMSvc')
        obj_client = get_soap_client('BasicHttpBinding_IDMObj')
        get_doc_call = {'call': {'dstIn': dst, 'criteria': {'criteriaCount': 2, 'criteriaNames': {
            'string': ['%TARGET_LIBRARY', '%DOCUMENT_NUMBER']}, 'criteriaValues': {
            'string': ['RTA_MAIN', str(doc_number)]}}}}
        doc_reply = svc_client.service.GetDocSvr3(**get_doc_call)
        if not (doc_reply and doc_reply.resultCode == 0 and doc_reply.getDocID): return None
        content_id = doc_reply.getDocID
        stream_reply = obj_client.service.GetReadStream(call={'dstIn': dst, 'contentID': content_id})
        if not (stream_reply and stream_reply.resultCode == 0 and stream_reply.streamID):
            obj_client.service.ReleaseObject(call={'objectID': content_id})
            return None
        return {"obj_client": obj_client, "stream_id": stream_reply.streamID, "content_id": content_id}
    except Exception:
        return None

def dms_user_login(username, password):
    try:
        if not WSDL_URL: raise ValueError("WSDL_URL is not set.")
        client = get_soap_client()
        if not hasattr(client.service, 'LoginSvr5'):
            client = find_client_with_operation('LoginSvr5')
            if not client: return None

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

async def upload_document_to_dms(dst, file_stream, metadata, parent_id=None):
    import db_connector

    svc_client, obj_client = None, None
    created_doc_number, version_id, put_doc_id, stream_id = None, None, None, None
    event_id = metadata.get('event_id')

    try:
        svc_client = get_soap_client('BasicHttpBinding_IDMSvc')
        obj_client = get_soap_client('BasicHttpBinding_IDMObj')

        string_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}string')
        int_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}int')
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

        if parent_id and str(parent_id).strip():
            try:
                target_parent_id = str(parent_id)
                parent_ver = get_doc_version_info(dst, target_parent_id)

                ci_names = string_array_type(
                    ['%TARGET_LIBRARY', 'PARENT', 'PARENT_VERSION', 'DOCNUMBER', '%FOLDERITEM_LIBRARY_NAME',
                     'DISPLAYNAME', 'VERSION_TYPE'])

                ci_values = {'anyType': [
                    xsd.AnyObject(string_type, 'RTA_MAIN'),
                    xsd.AnyObject(string_type, target_parent_id),
                    xsd.AnyObject(string_type, str(parent_ver)),
                    xsd.AnyObject(string_type, str(created_doc_number)),
                    xsd.AnyObject(string_type, 'RTA_MAIN'),
                    xsd.AnyObject(string_type, metadata['docname']),
                    xsd.AnyObject(string_type, 'R')
                ]}

                svc_client.service.CreateObject(call={
                    'dstIn': dst,
                    'objectType': 'ContentItem',
                    'properties': {
                        'propertyCount': 7,
                        'propertyNames': ci_names,
                        'propertyValues': ci_values
                    }
                })
            except Exception as e:
                logging.error(f"Failed to link document {created_doc_number} to parent folder {parent_id}: {e}")

        if created_doc_number and event_id is not None:
            await db_connector.link_document_to_event(created_doc_number, event_id)

        return created_doc_number

    except Exception as e:
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
    try:
        content_bytes, filename = get_image_by_docnumber(dst, doc_number)
        return content_bytes, filename
    except Exception:
        return None, None

def rename_document(dst, doc_id, new_name):
    try:
        svc_client = get_soap_client('BasicHttpBinding_IDMSvc')
        string_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}string')
        int_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}int')
        string_array_type = svc_client.get_type(
            '{http://schemas.microsoft.com/2003/10/Serialization/Arrays}ArrayOfstring')

        prop_names = string_array_type(['%TARGET_LIBRARY', '%OBJECT_IDENTIFIER', 'DOCNAME'])
        prop_values = {'anyType': [
            xsd.AnyObject(string_type, 'RTA_MAIN'),
            xsd.AnyObject(int_type, int(doc_id)),
            xsd.AnyObject(string_type, new_name)
        ]}

        update_call = {
            'call': {
                'dstIn': dst,
                'objectType': 'DEF_PROF',
                'properties': {
                    'propertyCount': 3,
                    'propertyNames': prop_names,
                    'propertyValues': prop_values
                }
            }
        }

        response = svc_client.service.UpdateObject(**update_call)

        if response.resultCode == 0:
            return True
        else:
            return False

    except Exception:
        return False

def set_trustees(dst, doc_id, library, trustees, security_enabled="1"):
    try:
        svc_client = get_soap_client('BasicHttpBinding_IDMSvc')
        string_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}string')
        string_array_type = svc_client.get_type(
            '{http://schemas.microsoft.com/2003/10/Serialization/Arrays}ArrayOfstring')
        int_array_type = svc_client.get_type('{http://schemas.microsoft.com/2003/10/Serialization/Arrays}ArrayOfint')

        names = [t.username for t in trustees]
        flags = [t.flag if t.flag is not None else 2 for t in trustees]
        rights = [t.rights for t in trustees]

        trustee_names_obj = string_array_type(names)
        trustee_flags_obj = int_array_type(flags)
        trustee_rights_obj = int_array_type(rights)

        prop_names = string_array_type(['%TARGET_LIBRARY', '%OBJECT_IDENTIFIER', '%RECENTLY_USED_LOCATION', 'SECURITY'])

        val_list = [
            xsd.AnyObject(string_type, library),
            xsd.AnyObject(string_type, str(doc_id)),
            xsd.AnyObject(string_type, f"DOCSOPEN!L\\{library}"),
            xsd.AnyObject(string_type, str(security_enabled))
        ]
        prop_values = {'anyType': val_list}

        call_data = {
            'dstIn': dst,
            'objectType': 'DEF_PROF',
            'properties': {
                'propertyCount': 4,
                'propertyNames': prop_names,
                'propertyValues': prop_values
            },
            'trustees': {
                'trusteeCount': len(trustees),
                'trusteeNames': trustee_names_obj,
                'trusteeFlags': trustee_flags_obj,
                'trusteeRights': trustee_rights_obj
            }
        }

        response = svc_client.service.SetTrustees(call=call_data)
        if response.resultCode == 0:
            return True, "Success"
        return False, f"Error: {getattr(response, 'resultCode', 'Unknown')}"

    except Exception as e:
        return False, str(e)

def get_object_trustees(dst, doc_id, library='RTA_MAIN'):
    try:
        svc_client = get_soap_client('BasicHttpBinding_IDMSvc')
        string_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}string')
        string_array_type = svc_client.get_type(
            '{http://schemas.microsoft.com/2003/10/Serialization/Arrays}ArrayOfstring')

        prop_names = string_array_type(['%TARGET_LIBRARY', '%OBJECT_IDENTIFIER'])
        prop_values = {'anyType': [
            xsd.AnyObject(string_type, library),
            xsd.AnyObject(string_type, str(doc_id))
        ]}

        call_data = {
            'dstIn': dst,
            'objectType': 'DEF_PROF',
            'properties': {
                'propertyCount': 2,
                'propertyNames': prop_names,
                'propertyValues': prop_values
            }
        }

        if hasattr(svc_client.service, 'GetTrustees'):
            response = svc_client.service.GetTrustees(call=call_data)

            if response.resultCode == 0 and response.trustees:
                trustees = []
                # Safely access properties with defaults
                t_names = response.trustees.trusteeNames.string if (
                            response.trustees.trusteeNames and hasattr(response.trustees.trusteeNames,
                                                                       'string')) else []
                t_flags = response.trustees.trusteeFlags.int if (
                            response.trustees.trusteeFlags and hasattr(response.trustees.trusteeFlags, 'int')) else []
                t_rights = response.trustees.trusteeRights.int if (
                            response.trustees.trusteeRights and hasattr(response.trustees.trusteeRights, 'int')) else []

                # Ensure count is an int
                count = response.trustees.trusteeCount if response.trustees.trusteeCount is not None else 0

                # Ensure we don't go out of bounds if arrays are shorter than count for some reason
                limit = min(count, len(t_names), len(t_flags), len(t_rights))

                for i in range(limit):
                    trustees.append({
                        'username': t_names[i],
                        'flag': t_flags[i],
                        'rights': t_rights[i]
                    })
                return trustees
        return []
    except Exception as e:
        logging.error(f"Error getting trustees: {e}")
        return []

def get_group_members(dst, group_id, library='RTA_MAIN'):
    try:
        svc_client = get_soap_client('BasicHttpBinding_IDMSvc')

        # Based on Search XML trace for v_peoplegroups
        search_call = {
            'call': {
                'dstIn': dst,
                'objectType': 'v_peoplegroups',
                'signature': {
                    'libraries': {'string': [library]},
                    'criteria': {
                        'criteriaCount': 1,
                        'criteriaNames': {'string': ['GROUP_ID']},
                        'criteriaValues': {'string': [group_id]}
                    },
                    'retProperties': {
                        'string': ['USER_ID', 'FULL_NAME', 'PEOPLE_SYSTEM_ID', 'Disabled', 'ALLOW_LOGIN']
                    },
                    'sortProps': {
                        'propertyCount': 1,
                        'propertyNames': {'string': ['FULL_NAME']},
                        'propertyFlags': {'int': [1]}
                    },
                    'maxRows': 0
                }
            }
        }

        search_reply = svc_client.service.Search(**search_call)

        if not (search_reply and search_reply.resultCode == 0 and search_reply.resultSetID):
            return []

        result_set_id = search_reply.resultSetID

        # Fetch Data
        data_client = find_client_with_operation('GetDataW') or svc_client
        method_name = 'GetDataW' if hasattr(data_client.service, 'GetDataW') else 'GetData'

        get_data_call = {
            'call': {
                'resultSetID': result_set_id,
                'requestedRows': 500,
                'startingRow': 0
            }
        }

        data_reply = getattr(data_client.service, method_name)(**get_data_call)

        members = []
        # Parse logic similar to list_folder_contents but specific columns
        # If binary buffer, use existing parser or if RowNode use that.
        # Trace shows binary buffer.

        if hasattr(data_reply, 'resultSetData') and data_reply.resultSetData:
            container = data_reply.resultSetData
            if hasattr(container, 'resultBuffer') and container.resultBuffer:
                # Reusing the parse_binary_result_buffer might be tricky if it expects specific folder columns.
                # However, for simple string lists it might work if the binary format is standard.
                # If not, we might need a specific parser or rely on RowNode if available.
                # Let's check if RowNode is an option in configured binding, otherwise we might need to parse the binary.
                # The binary parser in existing code is tailored for folder lists (icons etc).
                # A generic text parser for the buffer:
                try:
                    if len(container.resultBuffer) > 8 and container.resultBuffer[8:10] == b'\x78\x9c':
                        buff = zlib.decompress(container.resultBuffer[8:])
                    else:
                        buff = container.resultBuffer

                    try:
                        text = buff.decode('utf-16-le', errors='ignore')
                    except:
                        text = buff.decode('utf-8', errors='ignore')

                    # Very rough parsing of space-separated strings from buffer if that's the format
                    # Ideally we want RowNode structure.
                    # If GetData returns RowNode (often XML configuration dependent), we iterate it.
                    pass
                except:
                    pass

        # If data_reply has RowNode/rowNode (Text/XML mode)
        row_nodes = getattr(data_reply, 'rowNode', None) or getattr(data_reply, 'RowNode', None)

        if row_nodes:
            for row in row_nodes:
                vals = row.propValues.anyType
                if vals:
                    members.append({
                        'user_id': vals[0],
                        'full_name': vals[1],
                        'system_id': vals[2]
                    })

        # Cleanup
        try:
            svc_client.service.ReleaseData(call={'resultSetID': result_set_id})
            svc_client.service.ReleaseObject(call={'objectID': result_set_id})
        except:
            pass

        return members

    except Exception as e:
        logging.error(f"Error getting group members: {e}")
        return []

def get_all_groups(dst, library='RTA_MAIN'):
    try:
        svc_client = get_soap_client('BasicHttpBinding_IDMSvc')

        search_call = {
            'call': {
                'dstIn': dst,
                'objectType': 'v_groups',
                'signature': {
                    'libraries': {'string': [library]},
                    'criteria': {
                        'criteriaCount': 0,  # Search all
                        'criteriaNames': {'string': []},
                        'criteriaValues': {'string': []}
                    },
                    'retProperties': {
                        'string': ['GROUP_ID', 'FULL_NAME', 'DESCRIPTION']
                    },
                    'sortProps': {
                        'propertyCount': 1,
                        'propertyNames': {'string': ['GROUP_ID']},
                        'propertyFlags': {'int': [1]}
                    },
                    'maxRows': 0
                }
            }
        }

        search_reply = svc_client.service.Search(**search_call)

        # If v_groups fails, fallback might be needed, but let's stick to the pattern.
        if not (search_reply and search_reply.resultCode == 0 and search_reply.resultSetID):
            return []

        result_set_id = search_reply.resultSetID

        data_client = find_client_with_operation('GetDataW') or svc_client
        method_name = 'GetDataW' if hasattr(data_client.service, 'GetDataW') else 'GetData'

        get_data_call = {
            'call': {
                'resultSetID': result_set_id,
                'requestedRows': 500,
                'startingRow': 0
            }
        }

        data_reply = getattr(data_client.service, method_name)(**get_data_call)

        groups = []
        row_nodes = getattr(data_reply, 'rowNode', None) or getattr(data_reply, 'RowNode', None)

        if row_nodes:
            for row in row_nodes:
                vals = row.propValues.anyType
                if vals:
                    groups.append({
                        'group_id': vals[0],
                        'group_name': vals[1] if len(vals) > 1 else vals[0],
                        'description': vals[2] if len(vals) > 2 else ""
                    })

        # Cleanup
        try:
            svc_client.service.ReleaseData(call={'resultSetID': result_set_id})
            svc_client.service.ReleaseObject(call={'objectID': result_set_id})
        except:
            pass

        return groups

    except Exception as e:
        logging.error(f"Error getting groups: {e}")
        return []

def get_current_user_group_members(dst, username, library='RTA_MAIN'):
    return search_users_in_group(dst, "EDMS_TEST_GRP_2", "")

def search_users_in_group(dst, group_id, search_term, library='RTA_MAIN'):
    try:
        svc_client = get_soap_client('BasicHttpBinding_IDMSvc')

        # Ensure we have a valid group_id to search by, as implied by the working trace
        if not group_id:
            return []

        search_call = {
            'call': {
                'dstIn': dst,
                'objectType': 'v_peoplegroups',
                'signature': {
                    'libraries': {'string': [library]},
                    'criteria': {
                        'criteriaCount': 1,
                        'criteriaNames': {'string': ['GROUP_ID']},
                        'criteriaValues': {'string': [group_id]}
                    },
                    'retProperties': {
                        'string': ['USER_ID', 'FULL_NAME', 'PEOPLE_SYSTEM_ID']
                    },
                    'sortProps': {
                        'propertyCount': 1,
                        'propertyNames': {'string': ['FULL_NAME']},
                        'propertyFlags': {'int': [1]}
                    },
                    'maxRows': 0
                }
            }
        }

        search_reply = svc_client.service.Search(**search_call)
        if not (search_reply and search_reply.resultCode == 0 and search_reply.resultSetID):
            return []

        result_set_id = search_reply.resultSetID

        data_client = find_client_with_operation('GetDataW') or svc_client
        method_name = 'GetDataW' if hasattr(data_client.service, 'GetDataW') else 'GetData'

        get_data_call = {'call': {'resultSetID': result_set_id, 'requestedRows': 500, 'startingRow': 0}}
        data_reply = getattr(data_client.service, method_name)(**get_data_call)

        members = []
        row_nodes = getattr(data_reply, 'rowNode', None) or getattr(data_reply, 'RowNode', None)

        if row_nodes:
            for row in row_nodes:
                vals = row.propValues.anyType
                if vals:
                    # Depending on retProperties order
                    # USER_ID, FULL_NAME, SYSTEM_ID
                    u_id = vals[0]
                    f_name = vals[1]

                    # Filter locally since we fetched by Group
                    if not search_term or (
                            search_term.lower() in str(f_name).lower() or search_term.lower() in str(u_id).lower()):
                        members.append({'user_id': u_id, 'full_name': f_name})

        try:
            svc_client.service.ReleaseData(call={'resultSetID': result_set_id})
            svc_client.service.ReleaseObject(call={'objectID': result_set_id})
        except:
            pass

        return members

    except Exception as e:
        logging.error(f"Error searching group users: {e}")
        return []