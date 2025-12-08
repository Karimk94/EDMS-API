import zeep
import os
import logging
import struct
import re
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
        logging.error(f"Login failed: {e}")
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
        logging.error(f"Create folder failed: {e}")
        return None


def parse_binary_result_buffer(buffer):
    items = []
    try:
        try:
            raw_text = buffer.decode('utf-16-le', errors='ignore')
        except:
            raw_text = buffer.decode('utf-8', errors='ignore')
        clean_text = re.sub(r'[^\w\s\-\.]', ' ', raw_text)
        tokens = clean_text.split()

        i = 0
        while i < len(tokens):
            token = tokens[i]
            # MODIFIED: Aggressively accept ANY integer sequence as a potential ID
            if token.isdigit():
                if i + 1 < len(tokens):
                    potential_name = tokens[i + 1]
                    # Accept even if name is short or number, just grab it
                    items.append(
                        {'id': token, 'name': potential_name, 'type': 'folder', 'node_type': 'N', 'is_standard': False})
                    # Don't skip next token, it might be the start of another ID if name was empty/short
            i += 1
    except Exception:
        pass
    return items


def delete_document(dst, doc_number, force=False):
    """
    Deletes a document or folder.
    Phase 1: Unlink from all parents using 'Where Used' collection.
    Phase 2: Delete the Profile.
    """
    try:
        svc_client = get_soap_client('BasicHttpBinding_IDMSvc')

        # Check for DeleteObject client
        del_client = find_client_with_operation('DeleteObject')
        if not del_client: return False, "DeleteObject not found"

        string_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}string')
        string_array_type = svc_client.get_type(
            '{http://schemas.microsoft.com/2003/10/Serialization/Arrays}ArrayOfstring')
        int_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}int')

        # --- STEP 1: Unlink (Force) ---
        if force:
            logging.info(f"Force Delete: performing 'Where Used' check for {doc_number}...")
            link_ids = []

            # 1. Create a "Where Used" Collection to find parents
            coll_prop_names = string_array_type(['%TARGET_LIBRARY', 'DOCNUMBER', '%CONTENTS_DIRECTIVE'])
            coll_prop_values = {'anyType': [
                xsd.AnyObject(string_type, 'RTA_MAIN'),
                xsd.AnyObject(string_type, str(doc_number)),
                xsd.AnyObject(string_type, '%CONTENTS_WHERE_USED')  # Specific directive from trace
            ]}

            coll_call = {
                'call': {
                    'dstIn': dst,
                    'objectType': 'ContentsCollection',
                    'properties': {
                        'propertyCount': 3,
                        'propertyNames': coll_prop_names,
                        'propertyValues': coll_prop_values
                    }
                }
            }

            try:
                # Need client with CreateObject (IDMSvc)
                coll_reply = svc_client.service.CreateObject(**coll_call)

                # Check for IUNKNOWN_INTERFACE (Collection ID)
                collection_id = None
                if coll_reply.resultCode == 0 and coll_reply.retProperties:
                    props = coll_reply.retProperties
                    if props.propertyValues and props.propertyValues.anyType:
                        collection_id = props.propertyValues.anyType[0]  # Usually first prop is the ID

                if collection_id:
                    logging.info(f"Created WhereUsed Collection: {collection_id}")

                    # 2. Iterate using NewEnum / NextData
                    # Note: Need client with NewEnum (usually IDMSvc or IDMNav)
                    # Try finding one
                    enum_client = find_client_with_operation('NewEnum') or svc_client

                    enum_call = {'call': {'dstIn': dst, 'collectionID': collection_id}}
                    enum_reply = enum_client.service.NewEnum(**enum_call)

                    if enum_reply.resultCode == 0 and enum_reply.enumID:
                        enum_id = enum_reply.enumID

                        # Fetch Data
                        next_call = {'call': {'dstIn': dst, 'enumID': enum_id, 'elementCount': 100}}
                        data_reply = enum_client.service.NextData(**next_call)

                        if data_reply.resultCode in [0, 1] and data_reply.genericItemsData:
                            # Parse genericItemsData
                            g_data = data_reply.genericItemsData
                            prop_names = g_data.propertyNames.string
                            rows = g_data.propertyRows.ArrayOfanyType

                            sys_id_idx = -1
                            if 'SYSTEM_ID' in prop_names:
                                sys_id_idx = prop_names.index('SYSTEM_ID')

                            if sys_id_idx != -1 and rows:
                                for row in rows:
                                    try:
                                        sys_val = row.anyType[sys_id_idx]
                                        link_ids.append(sys_val)
                                    except:
                                        pass

                        # Cleanup Enum
                        try:
                            enum_client.service.ReleaseObject(call={'objectID': enum_id})
                        except:
                            pass

                    # Cleanup Collection
                    try:
                        svc_client.service.ReleaseObject(call={'objectID': collection_id})
                    except:
                        pass

            except Exception as e:
                logging.warning(f"Where Used check failed: {e}")

            if link_ids:
                logging.info(f"Unlinking {len(link_ids)} parents. Link IDs (SYSTEM_ID): {link_ids}")
                for link_id in link_ids:
                    del_link_call = {
                        'call': {
                            'dstIn': dst,
                            'objectType': 'ContentItem',
                            'properties': {
                                'propertyCount': 2,
                                'propertyNames': string_array_type(['%TARGET_LIBRARY', '%OBJECT_IDENTIFIER']),
                                'propertyValues': {'anyType': [
                                    xsd.AnyObject(string_type, 'RTA_MAIN'),
                                    xsd.AnyObject(string_type, str(link_id))
                                ]}
                            }
                        }
                    }
                    try:
                        del_client.service.DeleteObject(**del_link_call)
                        logging.info(f"Unlinked reference {link_id}")
                    except Exception as e:
                        logging.warning(f"Unlink fail {link_id}: {e}")
            else:
                logging.warning("Force delete: No links found via Where Used.")

        # --- STEP 2: Delete Profile ---
        logging.info(f"Deleting profile {doc_number}...")
        del_props = {'propertyCount': 2, 'propertyNames': string_array_type(['%TARGET_LIBRARY', '%OBJECT_IDENTIFIER']),
                     'propertyValues': {
                         'anyType': [xsd.AnyObject(string_type, 'RTA_MAIN'), xsd.AnyObject(int_type, int(doc_number))]}}

        resp = del_client.service.DeleteObject(call={'dstIn': dst, 'objectType': 'DEF_PROF', 'properties': del_props})

        if resp.resultCode == 0:
            logging.info(f"Deleted {doc_number}.")
            return True, "Success"
        else:
            err = getattr(resp, 'errorDoc', 'Unknown Error')
            logging.error(f"Delete failed: {err}")
            return False, str(err)

    except Exception as e:
        logging.error(f"Delete exception: {e}")
        return False, str(e)

def list_folder_contents(dst, parent_id=None, app_source=None):
    """
    Lists contents of a folder.
    """
    import db_connector

    items = []

    target_id = parent_id
    is_root_view = False

    if not target_id or str(target_id).strip() == "" or str(target_id) == "null":
        target_id = SMART_EDMS_ROOT_ID
        is_root_view = True

    if is_root_view:
        try:
            counts = db_connector.get_media_type_counts(app_source)

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

    try:
        search_client = get_soap_client('BasicHttpBinding_IDMSvc')
        criteria_name = '%ITEM'
        criteria_value = str(target_id)

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
                    'retProperties': {'string': ['FI.DOCNUMBER', '%DISPLAY_NAME', 'FI.NODE_TYPE', 'DOCNAME']},
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
            logging.error("FATAL: Neither GetDataW nor GetData found.")
            return items

        get_data_call = {'call': {'resultSetID': result_set_id, 'requestedRows': 500, 'startingRow': 0}}
        data_reply = getattr(data_client.service, method_name)(**get_data_call)

        row_nodes = None

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
                    return items

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

                    is_folder = (node_type == 'N') or (node_type == 'R' and not str(doc_id).isdigit())
                    if '.' in name_str and len(name_str.split('.')[-1]) < 5: is_folder = False

                    items.append({
                        'id': str(doc_id), 'name': name_str, 'type': 'folder' if is_folder else 'file',
                        'node_type': str(node_type), 'is_standard': False
                    })
                except Exception:
                    pass

        try:
            search_client.service.ReleaseData(call={'resultSetID': result_set_id})
            search_client.service.ReleaseObject(call={'objectID': result_set_id})
        except Exception:
            pass

        return items

    except Exception as e:
        logging.error(f"Error listing folder contents: {e}", exc_info=True)
        return items

def rename_document(dst, doc_number, new_name):
    try:
        svc_client = get_soap_client('BasicHttpBinding_IDMSvc')
        string_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}string')
        int_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}int')
        string_array_type = svc_client.get_type(
            '{http://schemas.microsoft.com/2003/10/Serialization/Arrays}ArrayOfstring')

        logging.info(f"Renaming document {doc_number} to '{new_name}'...")

        prop_names = string_array_type(['%OBJECT_TYPE_ID', '%OBJECT_IDENTIFIER', '%TARGET_LIBRARY', 'DOCNAME'])
        prop_values = [
            xsd.AnyObject(string_type, 'DEF_PROF'),
            xsd.AnyObject(int_type, int(doc_number)),
            xsd.AnyObject(string_type, 'RTA_MAIN'),
            xsd.AnyObject(string_type, new_name)
        ]

        update_call = {
            'call': {
                'dstIn': dst,
                'objectType': 'Profile',
                'properties': {
                    'propertyCount': len(prop_names.string),
                    'propertyNames': prop_names,
                    'propertyValues': {'anyType': prop_values}
                }
            }
        }

        response = svc_client.service.UpdateObject(**update_call)

        if response.resultCode == 0:
            logging.info(f"Successfully renamed document {doc_number}.")
            return True
        else:
            err = getattr(response, 'errorDoc', 'Unknown Error')
            logging.error(f"Rename failed. Code: {response.resultCode} - {err}")
            return False

    except Exception as e:
        logging.error(f"Error renaming document: {e}", exc_info=True)
        return False

def get_image_by_docnumber(dst, doc_number):
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
        logging.error(f"DMS server fault during retrieval for doc: {doc_number}. Fault: {e}", exc_info=True)
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
    try:
        if not WSDL_URL: raise ValueError("WSDL_URL is not set.")
        client = get_soap_client()

        if not hasattr(client.service, 'LoginSvr5'):
            client = find_client_with_operation('LoginSvr5')
            if not client:
                logging.error("LoginSvr5 not found during user login.")
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
    import db_connector

    svc_client, obj_client = None, None
    created_doc_number, version_id, put_doc_id, stream_id = None, None, None, None
    event_id = metadata.get('event_id')

    try:
        svc_client = get_soap_client('BasicHttpBinding_IDMSvc')
        obj_client = get_soap_client('BasicHttpBinding_IDMObj')

        string_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}string')
        int_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}int')

        logging.info("Step 2: CreateObject - Creating document profile.")
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

        logging.info(f"CreateObject successful. New docnumber: {created_doc_number}, VersionID: {version_id}")

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
        logging.error(f"DMS upload failed. Error: {e}", exc_info=True)
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