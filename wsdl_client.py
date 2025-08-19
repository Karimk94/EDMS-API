import zeep
import os
import logging
from zeep import Client, Settings
from zeep.exceptions import Fault
from dotenv import load_dotenv

load_dotenv()

WSDL_URL = os.getenv("WSDL_URL")
DMS_USER = os.getenv("DMS_USER")
DMS_PASSWORD = os.getenv("DMS_PASSWORD")

def dms_login():
    """Logs into the DMS SOAP service and returns a session token (DST)."""
    try:
        settings = Settings(strict=False, xml_huge_tree=True)
        client = Client(WSDL_URL, settings=settings)

        login_info_type = client.get_type('{http://schemas.datacontract.org/2004/07/OpenText.DMSvr.Serializable}DMSvrLoginInfo')
        login_info_instance = login_info_type(network=0, loginContext='RTA_MAIN', username=DMS_USER, password=DMS_PASSWORD)

        array_type = client.get_type('{http://schemas.datacontract.org/2004/07/OpenText.DMSvr.Serializable}ArrayOfDMSvrLoginInfo')
        login_info_array_instance = array_type(DMSvrLoginInfo=[login_info_instance])

        call_data = {'call': {'loginInfo': login_info_array_instance, 'authen': 1, 'dstIn': ''}}

        response = client.service.LoginSvr5(**call_data)

        if response and response.resultCode == 0 and response.DSTOut:
            return response.DSTOut
        else:
            result_code = getattr(response, 'resultCode', 'N/A')
            logging.error(f"DMS login failed. Result code: {result_code}")
            return None
    except Exception as e:
        logging.error(f"An unexpected error occurred during DMS login: {e}", exc_info=True)
        return None

def get_image_by_docnumber(dst, doc_number):
    """Retrieves a single document's image bytes from the DMS using the multi-step process."""
    svc_client, obj_client, content_id, stream_id = None, None, None, None
    try:
        settings = Settings(strict=False, xml_huge_tree=True)
        svc_client = Client(WSDL_URL, port_name='BasicHttpBinding_IDMSvc', settings=settings)
        obj_client = Client(WSDL_URL, port_name='BasicHttpBinding_IDMObj', settings=settings)

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
            logging.warning(f"Document not found in DMS for doc_number: {doc_number}. Result code: {getattr(doc_reply, 'resultCode', 'N/A')}")
            return None, None
        content_id = doc_reply.getDocID

        stream_reply = obj_client.service.GetReadStream(call={'dstIn': dst, 'contentID': content_id})
        if not (stream_reply and stream_reply.resultCode == 0 and stream_reply.streamID):
            raise Exception(f"Failed to get read stream. Result code: {getattr(stream_reply, 'resultCode', 'N/A')}")
        stream_id = stream_reply.streamID

        doc_buffer = bytearray()
        while True:
            read_reply = obj_client.service.ReadStream(call={'streamID': stream_id, 'requestedBytes': 65536})
            if not read_reply or read_reply.resultCode != 0:
                raise Exception(f"Stream read failed. Result code: {getattr(read_reply, 'resultCode', 'N/A')}")
            chunk_data = read_reply.streamData.streamBuffer if read_reply.streamData else None
            if not chunk_data: break
            doc_buffer.extend(chunk_data)

        filename = f"{doc_number}.jpg" # Default
        if doc_reply.docProperties and doc_reply.docProperties.propertyValues:
            try:
                prop_names = doc_reply.docProperties.propertyNames.string
                if '%VERSION_FILE_NAME' in prop_names:
                    index = prop_names.index('%VERSION_FILE_NAME')
                    version_file_name = doc_reply.docProperties.propertyValues.anyType[index]
                    _, extension = os.path.splitext(str(version_file_name))
                    if extension:
                        filename = f"{doc_number}{extension}"
            except Exception as e:
                logging.warning(f"Could not determine original filename, will use default. Error: {e}")
        
        return doc_buffer, filename

    except Fault as e:
        logging.error(f"DMS server fault during retrieval for doc: {doc_number}. Fault: {e}", exc_info=True)
        return None, None
    finally:
        if obj_client:
            if stream_id:
                try: obj_client.service.ReleaseObject(call={'objectID': stream_id})
                except Exception: pass
            if content_id:
                try: obj_client.service.ReleaseObject(call={'objectID': content_id})
                except Exception: pass
