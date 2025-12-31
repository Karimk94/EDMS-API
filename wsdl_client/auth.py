from .base import get_soap_client, find_client_with_operation
from .config import DMS_USER, DMS_PASSWORD, WSDL_URL
import logging

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
            dst = response.DSTOut
            logging.info(f"[DEBUG LOGIN] Login successful for {username}")

            # DEBUG: Check why user doesn't appear in group search
            try:
                svc_client = get_soap_client('BasicHttpBinding_IDMSvc')

                search_call = {
                    'call': {
                        'dstIn': dst,
                        'objectType': 'v_peoplegroups',
                        'signature': {
                            'libraries': {'string': ['RTA_MAIN']},
                            'criteria': {
                                'criteriaCount': 2,
                                'criteriaNames': {'string': ['GROUP_ID', 'USER_ID']},
                                'criteriaValues': {'string': ['DOCS_USERS', username]}
                            },
                            'retProperties': {
                                'string': ['USER_ID', 'FULL_NAME', 'GROUP_ID', 'DISABLED']
                            },
                            'maxRows': 10
                        }
                    }
                }

                search_reply = svc_client.service.Search(**search_call)
                logging.info(
                    f"[DEBUG USER] Search for {username} in DOCS_USERS - resultCode: {getattr(search_reply, 'resultCode', 'N/A')}")

                if search_reply and search_reply.resultCode == 0 and search_reply.resultSetID:
                    data_client = find_client_with_operation('GetDataW') or svc_client
                    method_name = 'GetDataW' if hasattr(data_client.service, 'GetDataW') else 'GetData'

                    get_data_call = {
                        'call': {'resultSetID': search_reply.resultSetID, 'requestedRows': 10, 'startingRow': 0}}
                    data_reply = getattr(data_client.service, method_name)(**get_data_call)

                    row_nodes = getattr(data_reply, 'rowNode', None) or getattr(data_reply, 'RowNode', None)

                    if row_nodes:
                        for idx, row in enumerate(row_nodes):
                            vals = row.propValues.anyType if hasattr(row, 'propValues') else []
                            logging.info(
                                f"[DEBUG USER] {username} record: USER_ID={vals[0] if len(vals) > 0 else ''}, FULL_NAME={vals[1] if len(vals) > 1 else ''}, GROUP_ID={vals[2] if len(vals) > 2 else ''}, DISABLED={vals[3] if len(vals) > 3 else ''}")
                    else:
                        logging.info(f"[DEBUG USER] {username} NOT FOUND in v_peoplegroups for DOCS_USERS")

                    try:
                        svc_client.service.ReleaseData(call={'resultSetID': search_reply.resultSetID})
                    except:
                        pass

            except Exception as e:
                logging.error(f"[DEBUG USER] Error: {e}")

            return dst

        return None
    except Exception:
        return None