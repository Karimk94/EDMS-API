from .base import get_soap_client, find_client_with_operation
from .config import DMS_USER, DMS_PASSWORD, WSDL_URL

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
            return response.DSTOut
        return None
    except Exception:
        return None