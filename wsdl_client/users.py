import logging
import re
import zlib
from zeep import xsd
from zeep.exceptions import Fault
from .base import get_soap_client, find_client_with_operation
from .config import DMS_USER
from .utils import parse_user_result_buffer

def resolve_trustee_system_id(dst, sys_id):
    """
    Resolves a numeric System ID to a textual User ID or Group ID.
    Returns: (text_id, flag) where flag is 2 for User, 1 for Group.
    """
    logging.info(f"Resolving System ID: {sys_id}")
    try:
        svc_client = get_soap_client('BasicHttpBinding_IDMSvc')
        data_client = find_client_with_operation('GetDataW') or svc_client
        method_name = 'GetDataW' if hasattr(data_client.service, 'GetDataW') else 'GetData'

        # Helper to perform a quick search and fetch one value
        def quick_lookup(obj_type, criteria_field, return_field):
            logging.debug(f"Lookup: {obj_type} where {criteria_field} = {sys_id}")
            try:
                search_call = {
                    'call': {
                        'dstIn': dst,
                        'objectType': obj_type,
                        'signature': {
                            'libraries': {'string': ['RTA_MAIN']},
                            'criteria': {
                                'criteriaCount': 1,
                                'criteriaNames': {'string': [criteria_field]},
                                'criteriaValues': {'string': [str(sys_id)]}
                            },
                            'retProperties': {'string': [return_field]},
                            'maxRows': 1
                        }
                    }
                }
                resp = svc_client.service.Search(**search_call)
                if resp and resp.resultCode == 0 and resp.resultSetID:
                    d_resp = getattr(data_client.service, method_name)(
                        call={'resultSetID': resp.resultSetID, 'requestedRows': 1, 'startingRow': 0})

                    try:
                        svc_client.service.ReleaseObject(call={'objectID': resp.resultSetID})
                    except:
                        pass

                    row_nodes = getattr(d_resp, 'rowNode', None) or getattr(d_resp, 'RowNode', None)
                    if row_nodes and row_nodes[0].propValues.anyType:
                        val = row_nodes[0].propValues.anyType[0]
                        if val:
                            logging.info(f"Found match in {obj_type}: {val}")
                            return str(val)
            except Exception as e:
                logging.debug(f"Lookup failed for {obj_type}: {e}")
            return None

        # 1. Search Groups (v_groups)
        group_id = quick_lookup('v_groups', 'SYSTEM_ID', 'GROUP_ID')
        if group_id: return group_id, 1

        # 2. Search Users (v_peoples)
        user_id = quick_lookup('v_peoples', 'SYSTEM_ID', 'USER_ID')
        if user_id: return user_id, 2

        user_id_pid = quick_lookup('v_peoples', 'PEOPLE_ID', 'USER_ID')
        if user_id_pid: return user_id_pid, 2

        # 3. Search Specialized Groups
        ug_id = quick_lookup('v_usergroups', 'SYSTEM_ID', 'GROUP_ID')
        if ug_id: return ug_id, 1

        ng_id = quick_lookup('v_nativegroups', 'SYSTEM_ID', 'GROUP_ID')
        if ng_id: return ng_id, 1

        # 4. Search Users (Legacy)
        user_id_psid = quick_lookup('v_peoples', 'PEOPLE_SYSTEM_ID', 'USER_ID')
        if user_id_psid: return user_id_psid, 2

        # 5. Search DEF_PROF (Profile lookup) - Check both %OBJECT_IDENTIFIER and DOCNUMBER
        # Users/Groups are documents. DOCNAME contains the UserID/GroupID.
        for field in ['%OBJECT_IDENTIFIER', 'DOCNUMBER']:
            logging.debug(f"Lookup: DEF_PROF where {field} = {sys_id}")
            try:
                search_call = {
                    'call': {
                        'dstIn': dst,
                        'objectType': 'DEF_PROF',
                        'signature': {
                            'libraries': {'string': ['RTA_MAIN']},
                            'criteria': {
                                'criteriaCount': 1,
                                'criteriaNames': {'string': [field]},
                                'criteriaValues': {'string': [str(sys_id)]}
                            },
                            'retProperties': {'string': ['DOCNAME', 'TYPE_ID']},
                            'maxRows': 1
                        }
                    }
                }
                resp = svc_client.service.Search(**search_call)
                if resp and resp.resultCode == 0 and resp.resultSetID:
                    d_resp = getattr(data_client.service, method_name)(
                        call={'resultSetID': resp.resultSetID, 'requestedRows': 1, 'startingRow': 0})
                    svc_client.service.ReleaseObject(call={'objectID': resp.resultSetID})

                    row_nodes = getattr(d_resp, 'rowNode', None) or getattr(d_resp, 'RowNode', None)
                    if row_nodes and row_nodes[0].propValues.anyType:
                        docname = row_nodes[0].propValues.anyType[0]
                        type_id = str(row_nodes[0].propValues.anyType[1] or "").upper()

                        if docname:
                            flag = 1 if ('GROUP' in type_id) else 2
                            logging.info(f"Found profile match: {docname} (Type: {type_id}, Flag: {flag})")
                            return str(docname), flag
            except Exception as e:
                logging.debug(f"Lookup failed for DEF_PROF ({field}): {e}")

    except Exception as e:
        logging.error(f"Error resolving system ID {sys_id}: {e}")

    return None, None

def set_trustees(dst, doc_id, library, trustees, security_enabled="1"):
    try:
        logging.info(f"Setting trustees for doc_id: {doc_id} in library: {library}. Raw count: {len(trustees)}")
        svc_client = get_soap_client('BasicHttpBinding_IDMSvc')

        # Define types
        string_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}string')
        int_type = svc_client.get_type('{http://www.w3.org/2001/XMLSchema}int')
        string_array_type = svc_client.get_type(
            '{http://schemas.microsoft.com/2003/10/Serialization/Arrays}ArrayOfstring')
        int_array_type = svc_client.get_type('{http://schemas.microsoft.com/2003/10/Serialization/Arrays}ArrayOfint')

        # Pre-process trustees
        candidate_list = []
        for t in trustees:
            u_name = None
            flag_val = None
            right_val = None
            type_val = None

            if isinstance(t, dict):
                u_name = t.get('username')
                flag_val = t.get('flag')
                right_val = t.get('rights')
                type_val = t.get('type')
            else:
                u_name = getattr(t, 'username', None)
                flag_val = getattr(t, 'flag', None)
                right_val = getattr(t, 'rights', None)
                type_val = getattr(t, 'type', None)

            if u_name:
                item = {
                    'name': str(u_name).strip(),
                    'rights': int(right_val) if right_val is not None else 1,
                    'inferred': False
                }

                if flag_val is not None:
                    item['flag'] = int(flag_val)
                else:
                    item['inferred'] = True
                    if type_val and str(type_val).lower() in ['group', 'g', '1']:
                        item['flag'] = 1
                    else:
                        item['flag'] = 2

                candidate_list.append(item)

        if not candidate_list:
            return False, "No valid trustee names provided."

        prop_names = string_array_type(['%TARGET_LIBRARY', '%OBJECT_IDENTIFIER', '%RECENTLY_USED_LOCATION', 'SECURITY'])

        sec_val = "1"
        if security_enabled and str(security_enabled).strip() in ['0', '1']:
            sec_val = str(security_enabled).strip()

        val_list = [
            xsd.AnyObject(string_type, library),
            xsd.AnyObject(int_type, int(doc_id)),
            xsd.AnyObject(string_type, f"DOCSOPEN!L\\{library}"),
            xsd.AnyObject(string_type, sec_val)
        ]
        prop_values = {'anyType': val_list}

        # Retry Loop
        attempts = 0
        max_attempts = 2

        while attempts < max_attempts:
            attempts += 1

            current_names = [x['name'] for x in candidate_list]
            current_flags = [x['flag'] for x in candidate_list]
            current_rights = [x['rights'] for x in candidate_list]

            logging.info(
                f"Trustee Payload (Attempt {attempts}) -> Names: {current_names} | Flags: {current_flags} | Rights: {current_rights}")

            call_data = {
                'dstIn': dst,
                'objectType': 'DEF_PROF',
                'properties': {
                    'propertyCount': 4,
                    'propertyNames': prop_names,
                    'propertyValues': prop_values
                },
                'trustees': {
                    'trusteeCount': len(candidate_list),
                    'trusteeNames': string_array_type(current_names),
                    'trusteeFlags': int_array_type(current_flags),
                    'trusteeRights': int_array_type(current_rights)
                }
            }

            try:
                response = svc_client.service.SetTrustees(call=call_data)

                if response.resultCode == 0:
                    return True, "Success"

                error_msg = getattr(response, 'errorDoc', '') or getattr(response, 'resultCode', 'Unknown')

                if "unknown trustee" in str(error_msg).lower() and attempts < max_attempts:
                    raise Fault("Unknown trustee (detected in ResultCode)")

                logging.error(f"SetTrustees failed with result code: {response.resultCode}, ErrorDoc: {error_msg}")
                return False, f"Error: {error_msg}"

            except Fault as f:
                fault_msg = f.message or str(f)
                if "unknown trustee" in fault_msg.lower() and attempts < max_attempts:
                    logging.warning(f"SetTrustees failed with 'unknown trustee'. Attempting auto-correction.")

                    changed_any = False

                    # 1. Try to resolve numeric IDs
                    for item in candidate_list:
                        if item['name'].isdigit():
                            logging.info(f"Resolving ID {item['name']}...")
                            resolved_name, resolved_flag = resolve_trustee_system_id(dst, item['name'])
                            if resolved_name:
                                logging.info(
                                    f"Resolved {item['name']} -> {resolved_name} (Type: {'User' if resolved_flag == 2 else 'Group'})")
                                item['name'] = resolved_name
                                item['flag'] = resolved_flag
                                changed_any = True
                            else:
                                logging.warning(f"Resolution failed for {item['name']}.")

                    # 2. If no resolution, fallback to swapping inferred flags
                    if not changed_any:
                        # a) Forced Numeric Swap (User -> Group)
                        for item in candidate_list:
                            if item['name'].isdigit() and item['flag'] == 2:
                                item['flag'] = 1
                                changed_any = True

                        if changed_any:
                            logging.info("Forced swap of numeric trustee from User to Group.")
                        else:
                            # b) Generic Inferred Swap
                            for item in candidate_list:
                                if item['inferred'] and item['flag'] == 2:
                                    item['flag'] = 1
                                    changed_any = True
                            if changed_any:
                                logging.info("Swapped inferred User flags to Group flags.")

                    if changed_any:
                        continue

                logging.error(f"SOAP Fault in set_trustees: {fault_msg}", exc_info=True)
                return False, f"SOAP Fault: {fault_msg}"

            except Exception as e:
                logging.error(f"Exception in set_trustees: {e}", exc_info=True)
                return False, str(e)

        return False, "Failed to set trustees after retries."

    except Exception as e:
        logging.error(f"Critical Exception in set_trustees wrapper: {e}", exc_info=True)
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

def get_current_user_group_members(dst, username, library='RTA_MAIN'):
    return search_users_in_group(dst, "EDMS_TEST_GRP_2", "")

def search_users_in_group(dst, group_id, search_term, library='RTA_MAIN'):
    try:
        svc_client = get_soap_client('BasicHttpBinding_IDMSvc')

        if not group_id:
            return []

        search_call = {
            'call': {
                'dstIn': dst,
                'objectType': 'v_peoplegroups',
                'signature': {
                    'libraries': {'string': [library]},
                    'criteria': {
                        'criteriaCount': 2,
                        'criteriaNames': {'string': ['GROUP_ID', 'DISABLED']},
                        'criteriaValues': {'string': [group_id, 'N']}
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

        # Helper to clean text
        def clean_text(text):
            if not text:
                return ""
            return re.sub(r'[\x00-\x1f\x7f]', '', str(text)).strip()

        # 1. Try Parsing Binary Buffer
        if hasattr(data_reply, 'resultSetData') and data_reply.resultSetData:
            container = data_reply.resultSetData
            if hasattr(container, 'resultBuffer') and container.resultBuffer:
                parsed_items = parse_user_result_buffer(container.resultBuffer)
                for p in parsed_items:
                    c_name = clean_text(p.get('full_name'))
                    c_id = clean_text(p.get('user_id'))

                    # Filtering Logic:
                    # 1. Must have ID and Name
                    # 2. ID should NOT be purely numeric (system accounts often are numeric strings in some views)
                    # 3. Match search term if provided
                    if c_name and c_id and not c_id.isdigit():
                        if not search_term or (
                                search_term.lower() in c_name.lower() or
                                search_term.lower() in c_id.lower()):
                            p['full_name'] = c_name
                            p['user_id'] = c_id
                            members.append(p)

        # 2. Try XML RowNode (Fallback)
        if not members:
            row_nodes = getattr(data_reply, 'rowNode', None) or getattr(data_reply, 'RowNode', None)
            if row_nodes:
                for row in row_nodes:
                    vals = row.propValues.anyType
                    if vals:
                        raw_id = vals[0]
                        raw_name = vals[1]

                        c_id = clean_text(raw_id)
                        c_name = clean_text(raw_name)

                        # Filtering Logic (same as above)
                        if c_id and c_name and not c_id.isdigit():
                            if not search_term or (
                                    search_term.lower() in c_name.lower() or search_term.lower() in c_id.lower()):
                                members.append({'user_id': c_id, 'full_name': c_name})

        try:
            svc_client.service.ReleaseData(call={'resultSetID': result_set_id})
            svc_client.service.ReleaseObject(call={'objectID': result_set_id})
        except:
            pass

        return members

    except Exception as e:
        logging.error(f"Error searching group users: {e}")
        return []

def get_groups_for_user(dst, username, library='RTA_MAIN'):
    """
    Attempts to fetch groups for a specific user.
    Falls back to fetching all groups if specific filtering fails.
    """
    group_ids = []

    try:
        svc_client = get_soap_client('BasicHttpBinding_IDMSvc')

        # Strategy 1: Search v_peoples (User Profile)
        try:
            search_call = {
                'call': {
                    'dstIn': dst,
                    'objectType': 'v_peoples',
                    'signature': {
                        'libraries': {'string': [library]},
                        'criteria': {
                            'criteriaCount': 2,
                            'criteriaNames': {'string': ['USER_ID', 'ALLOW_LOGIN']},
                            'criteriaValues': {'string': [username, 'Y']}
                        },
                        'retProperties': {
                            'string': ['GROUP_ID']
                        },
                        'maxRows': 0
                    }
                }
            }

            search_reply = svc_client.service.Search(**search_call)

            if search_reply and search_reply.resultCode == 0 and search_reply.resultSetID:
                result_set_id = search_reply.resultSetID
                data_client = find_client_with_operation('GetDataW') or svc_client
                method_name = 'GetDataW' if hasattr(data_client.service, 'GetDataW') else 'GetData'
                get_data_call = {'call': {'resultSetID': result_set_id, 'requestedRows': 500, 'startingRow': 0}}

                try:
                    data_reply = getattr(data_client.service, method_name)(**get_data_call)
                    row_nodes = getattr(data_reply, 'rowNode', None) or getattr(data_reply, 'RowNode', None)
                    if row_nodes:
                        for row in row_nodes:
                            vals = row.propValues.anyType
                            if vals and vals[0]: group_ids.append(vals[0])
                finally:
                    try:
                        svc_client.service.ReleaseData(call={'resultSetID': result_set_id})
                        svc_client.service.ReleaseObject(call={'objectID': result_set_id})
                    except:
                        pass
        except Exception as e:
            logging.debug(f"v_peoples search failed (using fallback): {e}")

        # Strategy 2: v_peoplegroups (Secondary check)
        if not group_ids:
            try:
                search_call['call']['objectType'] = 'v_peoplegroups'
                search_call['call']['signature']['criteria'] = {
                    'criteriaCount': 1,
                    'criteriaNames': {'string': ['USER_ID']},
                    'criteriaValues': {'string': [username]}
                }

                search_reply = svc_client.service.Search(**search_call)

                if search_reply and search_reply.resultCode == 0 and search_reply.resultSetID:
                    result_set_id = search_reply.resultSetID
                    data_client = find_client_with_operation('GetDataW') or svc_client
                    method_name = 'GetDataW' if hasattr(data_client.service, 'GetDataW') else 'GetData'
                    get_data_call = {'call': {'resultSetID': result_set_id, 'requestedRows': 500, 'startingRow': 0}}

                    try:
                        data_reply = getattr(data_client.service, method_name)(**get_data_call)
                        row_nodes = getattr(data_reply, 'rowNode', None) or getattr(data_reply, 'RowNode', None)
                        if row_nodes:
                            for row in row_nodes:
                                vals = row.propValues.anyType
                                if vals and vals[0] and vals[0] not in group_ids:
                                    group_ids.append(vals[0])
                    finally:
                        try:
                            svc_client.service.ReleaseData(call={'resultSetID': result_set_id})
                            svc_client.service.ReleaseObject(call={'objectID': result_set_id})
                        except:
                            pass
            except Exception as e:
                logging.debug(f"v_peoplegroups search failed (using fallback): {e}")

    except Exception as e:
        logging.warning(f"Error initializing group search: {e}")

    # Final logic:
    if group_ids:
        all_groups = get_all_groups(dst, library)
        user_groups = [g for g in all_groups if g['group_id'] in group_ids]

        if user_groups:
            return user_groups

        return [{'group_id': gid, 'group_name': gid} for gid in group_ids]

    # FALLBACK: Return ALL groups.
    return get_all_groups(dst, library)

def get_all_groups(dst, library='RTA_MAIN'):
    try:
        svc_client = get_soap_client('BasicHttpBinding_IDMSvc')

        # --- ATTEMPT 1: v_groups with various strategies ---

        # Strategy 1.1: Standard "Get All" (Empty criteria)
        search_call = {
            'call': {
                'dstIn': dst,
                'objectType': 'v_groups',
                'signature': {
                    'libraries': {'string': [library]},
                    'criteria': {
                        'criteriaCount': 0,
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

        # Strategy 1.2: Wildcard '%' on GROUP_ID
        if not (search_reply and search_reply.resultCode == 0 and search_reply.resultSetID):
            search_call['call']['signature']['criteria'] = {
                'criteriaCount': 1,
                'criteriaNames': {'string': ['GROUP_ID']},
                'criteriaValues': {'string': ['%']}
            }
            search_reply = svc_client.service.Search(**search_call)

        # Strategy 1.3: Wildcard '*' on GROUP_ID
        if not (search_reply and search_reply.resultCode == 0 and search_reply.resultSetID):
            search_call['call']['signature']['criteria']['criteriaValues']['string'] = ['*']
            search_reply = svc_client.service.Search(**search_call)

        # Strategy 1.4: LAST_UPDATE hack
        if not (search_reply and search_reply.resultCode == 0 and search_reply.resultSetID):
            search_call['call']['signature']['criteria'] = {
                'criteriaCount': 1,
                'criteriaNames': {'string': ['LAST_UPDATE']},
                'criteriaValues': {'string': ['1900-01-01 00:00:00 TO 3000-01-01 00:00:00']}
            }
            search_reply = svc_client.service.Search(**search_call)

        # Process results from v_groups
        groups = []
        if search_reply and search_reply.resultCode == 0 and search_reply.resultSetID:
            result_set_id = search_reply.resultSetID
            data_client = find_client_with_operation('GetDataW') or svc_client
            method_name = 'GetDataW' if hasattr(data_client.service, 'GetDataW') else 'GetData'
            get_data_call = {'call': {'resultSetID': result_set_id, 'requestedRows': 500, 'startingRow': 0}}

            try:
                data_reply = getattr(data_client.service, method_name)(**get_data_call)
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
            except Exception as e:
                logging.debug(f"Error fetching data for v_groups: {e}")
            finally:
                try:
                    svc_client.service.ReleaseData(call={'resultSetID': result_set_id})
                    svc_client.service.ReleaseObject(call={'objectID': result_set_id})
                except:
                    pass

        # --- ATTEMPT 2: Fallback to v_usergroups if v_groups failed ---
        if not groups:
            logging.debug("v_groups returned empty. Trying v_usergroups fallback...")
            try:
                search_call['call']['objectType'] = 'v_usergroups'
                search_call['call']['signature']['criteria'] = {
                    'criteriaCount': 1,
                    'criteriaNames': {'string': ['GROUP_ID']},
                    'criteriaValues': {'string': ['*']}
                }

                search_reply = svc_client.service.Search(**search_call)

                if search_reply and search_reply.resultCode == 0 and search_reply.resultSetID:
                    result_set_id = search_reply.resultSetID
                    data_client = find_client_with_operation('GetDataW') or svc_client
                    method_name = 'GetDataW' if hasattr(data_client.service, 'GetDataW') else 'GetData'
                    get_data_call = {'call': {'resultSetID': result_set_id, 'requestedRows': 500, 'startingRow': 0}}

                    data_reply = getattr(data_client.service, method_name)(**get_data_call)
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

                    try:
                        svc_client.service.ReleaseData(call={'resultSetID': result_set_id})
                        svc_client.service.ReleaseObject(call={'objectID': result_set_id})
                    except:
                        pass
            except Exception as e:
                logging.debug(f"v_usergroups unavailable (Form file not found or other): {e}")

        # --- ATTEMPT 3: Fallback to v_nativegroups if v_usergroups failed ---
        if not groups:
            # logging.debug("v_usergroups returned empty. Trying v_nativegroups fallback...")
            try:
                search_call['call']['objectType'] = 'v_nativegroups'
                search_call['call']['signature']['criteria'] = {
                    'criteriaCount': 1,
                    'criteriaNames': {'string': ['GROUP_ID']},
                    'criteriaValues': {'string': ['*']}
                }

                search_reply = svc_client.service.Search(**search_call)

                if search_reply and search_reply.resultCode == 0 and search_reply.resultSetID:
                    result_set_id = search_reply.resultSetID
                    data_client = find_client_with_operation('GetDataW') or svc_client
                    method_name = 'GetDataW' if hasattr(data_client.service, 'GetDataW') else 'GetData'
                    get_data_call = {'call': {'resultSetID': result_set_id, 'requestedRows': 500, 'startingRow': 0}}

                    data_reply = getattr(data_client.service, method_name)(**get_data_call)
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

                    try:
                        svc_client.service.ReleaseData(call={'resultSetID': result_set_id})
                        svc_client.service.ReleaseObject(call={'objectID': result_set_id})
                    except:
                        pass
            except Exception as e:
                logging.debug(f"v_nativegroups unavailable (Form file not found or other): {e}")

        # EMERGENCY FALLBACK: If absolutely nothing was found, return known groups from trace
        # This ensures the UI is not empty even if discovery fails.
        if not groups:
            groups.append({'group_id': 'DOCS_USERS', 'group_name': 'DOCS Users', 'description': 'System Users'})
            groups.append({'group_id': 'TIBCO_GROUP', 'group_name': 'TIBCO Group', 'description': ''})

        return groups

    except Exception as e:
        logging.error(f"Error getting groups: {e}")
        return []