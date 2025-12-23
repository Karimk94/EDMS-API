import os
import logging
import zlib
from zeep import xsd
from zeep.exceptions import Fault
from .base import get_soap_client, find_client_with_operation
from .config import DMS_USER

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

        if metadata.get('app_source') == 'edms-media':
            property_names_list.append('RTA_TEXT1')
            property_values_list.append(xsd.AnyObject(string_type, 'edms-media'))

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