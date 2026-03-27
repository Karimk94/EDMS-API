from .base import get_soap_client, find_client_with_operation
from .config import DMS_USER, DMS_PASSWORD, WSDL_URL
import logging
import time

# --- DMS Token Cache ---
_cached_dst = None
_cached_dst_time = 0
_DST_TTL_SECONDS = 30 * 60  # 30 minutes

def dms_system_login(force_refresh=False):
    """Logs into DMS with cached token (30-min TTL). Set force_refresh=True to bypass cache."""
    global _cached_dst, _cached_dst_time

    if not force_refresh and _cached_dst and (time.time() - _cached_dst_time) < _DST_TTL_SECONDS:
        return _cached_dst

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
            _cached_dst = response.DSTOut
            _cached_dst_time = time.time()
            return _cached_dst
        return None
    except Exception:
        _cached_dst = None
        _cached_dst_time = 0
        return None

def dms_user_login(username, password):
    try:
        if not WSDL_URL: raise ValueError("WSDL_URL is not set.")
        client = get_soap_client()
        if not hasattr(client.service, 'LoginSvr5'):
            client = find_client_with_operation('LoginSvr5')
            if not client: return None, "Could not connect to EDMS service"

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
            # logging.info(f"[DEBUG LOGIN] Login successful for {username}")


            return dst, None
        else:
            # Try to get error message from response
            error_msg = getattr(response, 'errMsg', 'Unknown EDMS error')
            if not error_msg and hasattr(response, 'resultCode'):
                 error_msg = f"EDMS Error Code: {response.resultCode}"
            return None, error_msg

    except Exception as e:
        logging.error(f"[DEBUG USER] Error: {e}")
        return None, str(e)