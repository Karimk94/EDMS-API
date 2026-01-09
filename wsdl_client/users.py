from zeep import xsd
from zeep.exceptions import Fault
from .base import get_soap_client, find_client_with_operation
from .utils import parse_user_result_buffer, parse_group_members_buffer, parse_groups_buffer, \
    clean_string, is_likely_user_id, looks_like_full_name
def is_valid_group_id(gid):
    """
    Validates that a group ID is a legitimate identifier.
    Filters out control characters, binary garbage, and other invalid entries.
    """
    if not gid:
        return False

    # Must be a string
    if not isinstance(gid, str):
        gid = str(gid)

    # Must have reasonable length (at least 2 chars, not too long)
    if len(gid) < 2 or len(gid) > 100:
        return False

    # Check for control characters (ASCII 0-31 and 127)
    for char in gid:
        code = ord(char)
        if code < 32 or code == 127:
            return False
        # Also filter out unusual Unicode ranges that are likely garbage
        # Allow basic Latin, Latin Extended, and common symbols
        if code > 0x024F and code < 0x0370:  # Skip some unusual ranges
            pass  # Allow Greek, Cyrillic, etc.
        if code > 0x07FF:  # Skip very high Unicode (likely binary garbage)
            return False

    # Must contain at least one alphanumeric character
    has_alnum = any(c.isalnum() for c in gid)
    if not has_alnum:
        return False

    # Whitespace-only is invalid
    if gid.strip() == '':
        return False

    return True

def get_all_groups(dst, library='RTA_MAIN'):
    """
    Fetches all available groups using multiple strategies.
    """
    groups = []
    seen_ids = set()

    def add_group(gid, gname=None, gdesc=""):
        if gid:
            gid = str(gid).strip()
        if not is_valid_group_id(gid):
            return
        if gid not in seen_ids:
            seen_ids.add(gid)
            if gname:
                gname = str(gname).strip()
                if not is_valid_group_id(gname):
                    gname = gid
            else:
                gname = gid
            groups.append({
                'group_id': gid,
                'group_name': gname,
                'description': gdesc if gdesc else ''
            })

    svc_client = get_soap_client('BasicHttpBinding_IDMSvc')
    data_client = find_client_with_operation('GetDataW') or svc_client
    method_name = 'GetDataW' if hasattr(data_client.service, 'GetDataW') else 'GetData'

    def fetch_and_release(result_set_id, extract_func):
        """Helper to fetch results and release resources"""
        results = []
        try:
            d_resp = getattr(data_client.service, method_name)(
                call={'resultSetID': result_set_id, 'requestedRows': 2000, 'startingRow': 0})

            # Try binary buffer first
            if hasattr(d_resp, 'resultSetData') and d_resp.resultSetData:
                container = d_resp.resultSetData
                actual_rows = getattr(container, 'actualRows', 0)
                columns = getattr(container, 'columns', 0)
                if hasattr(container, 'resultBuffer') and container.resultBuffer:
                    parsed = parse_user_result_buffer(container.resultBuffer, actual_rows, columns)
                    if parsed:
                        results.extend(extract_func(parsed, 'buffer'))

            # Also try RowNodes
            row_nodes = getattr(d_resp, 'rowNode', None) or getattr(d_resp, 'RowNode', None)
            if row_nodes:
                row_results = extract_func(row_nodes, 'rows')
                existing_ids = {r.get('group_id') or r.get('user_id') for r in results}
                for r in row_results:
                    rid = r.get('group_id') or r.get('user_id')
                    if rid and rid not in existing_ids:
                        results.append(r)
                        existing_ids.add(rid)

        except Exception:
            pass
        finally:
            try:
                svc_client.service.ReleaseData(call={'resultSetID': result_set_id})
            except:
                pass
            try:
                svc_client.service.ReleaseObject(call={'objectID': result_set_id})
            except:
                pass
        return results

    # STRATEGY 1: Query v_peoplegroups with wide date range
    try:
        search_call = {
            'call': {
                'dstIn': dst,
                'objectType': 'v_peoplegroups',
                'signature': {
                    'libraries': {'string': [library]},
                    'criteria': {
                        'criteriaCount': 1,
                        'criteriaNames': {'string': ['LAST_UPDATE']},
                        'criteriaValues': {'string': ['1900-01-01 00:00:00 TO 3000-01-01 00:00:00']}
                    },
                    'retProperties': {'string': ['GROUP_ID']},
                    'sortProps': {
                        'propertyCount': 1,
                        'propertyNames': {'string': ['GROUP_ID']},
                        'propertyFlags': {'int': [1]}
                    },
                    'maxRows': 0
                }
            }
        }

        resp = svc_client.service.Search(**search_call)
        if resp and resp.resultCode == 0 and resp.resultSetID:
            def extract_group_ids(data, data_type):
                extracted = []
                if data_type == 'buffer':
                    for p in data:
                        gid = p.get('col0') or p.get('user_id') or p.get('group_id')
                        if gid:
                            gid = clean_string(gid)
                            if gid:
                                extracted.append({'group_id': gid})
                else:
                    for row in data:
                        vals = row.propValues.anyType if hasattr(row, 'propValues') else []
                        if vals and vals[0]:
                            gid = clean_string(str(vals[0]))
                            if gid:
                                extracted.append({'group_id': gid})
                return extracted

            results = fetch_and_release(resp.resultSetID, extract_group_ids)
            for r in results:
                add_group(r['group_id'], r['group_id'])
    except Exception:
        pass

    # STRATEGY 2: Try v_groups with LAST_UPDATE range
    try:
        search_call = {
            'call': {
                'dstIn': dst,
                'objectType': 'v_groups',
                'signature': {
                    'libraries': {'string': [library]},
                    'criteria': {
                        'criteriaCount': 1,
                        'criteriaNames': {'string': ['LAST_UPDATE']},
                        'criteriaValues': {'string': ['1900-01-01 00:00:00 TO 3000-01-01 00:00:00']}
                    },
                    'retProperties': {'string': ['GROUP_ID', 'FULL_NAME', 'DESCRIPTION']},
                    'sortProps': {
                        'propertyCount': 1,
                        'propertyNames': {'string': ['GROUP_ID']},
                        'propertyFlags': {'int': [1]}
                    },
                    'maxRows': 0
                }
            }
        }

        resp = svc_client.service.Search(**search_call)
        if resp and resp.resultCode == 0 and resp.resultSetID:
            def extract_groups(data, data_type):
                extracted = []
                if data_type == 'buffer':
                    for p in data:
                        gid = p.get('col0') or p.get('group_id')
                        if gid:
                            extracted.append({
                                'group_id': clean_string(gid),
                                'group_name': clean_string(p.get('col1', '')),
                                'description': clean_string(p.get('col2', ''))
                            })
                else:
                    for row in data:
                        vals = row.propValues.anyType if hasattr(row, 'propValues') else []
                        if vals:
                            extracted.append({
                                'group_id': str(vals[0]).strip() if vals[0] else None,
                                'group_name': str(vals[1]).strip() if len(vals) > 1 and vals[1] else None,
                                'description': str(vals[2]).strip() if len(vals) > 2 and vals[2] else ''
                            })
                return extracted

            results = fetch_and_release(resp.resultSetID, extract_groups)
            for r in results:
                if r['group_id']:
                    add_group(r['group_id'], r.get('group_name'), r.get('description', ''))
    except Exception:
        pass

    # Sort groups alphabetically
    groups.sort(key=lambda x: x['group_id'].lower() if x['group_id'] else '')

    return groups

def search_groups(dst, name_pattern='', library='RTA_MAIN'):
    """
    Searches for groups matching a pattern.
    If no pattern or wildcard, returns all groups.
    """
    if not name_pattern or name_pattern in ['*', '%', '']:
        return get_all_groups(dst, library)

    # Get all groups and filter
    all_groups = get_all_groups(dst, library)

    pattern_lower = name_pattern.lower()
    filtered = [
        g for g in all_groups
        if pattern_lower in g['group_id'].lower() or
           pattern_lower in (g.get('group_name') or '').lower()
    ]

    return filtered

def resolve_trustee_system_id(dst, sys_id):
    """
    Resolves a numeric System ID to a textual User ID or Group ID.
    Returns: (text_id, flag) where flag is 2 for User, 1 for Group.
    """
    try:
        svc_client = get_soap_client('BasicHttpBinding_IDMSvc')
        data_client = find_client_with_operation('GetDataW') or svc_client
        method_name = 'GetDataW' if hasattr(data_client.service, 'GetDataW') else 'GetData'

        def quick_lookup(obj_type, criteria_field, return_field):
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
                            return str(val)
            except Exception:
                pass
            return None

        # 1. Search Groups (v_groups)
        group_id = quick_lookup('v_groups', 'SYSTEM_ID', 'GROUP_ID')
        if group_id:
            return group_id, 1

        # 2. Search Users (v_peoples)
        user_id = quick_lookup('v_peoples', 'SYSTEM_ID', 'USER_ID')
        if user_id:
            return user_id, 2

        user_id_pid = quick_lookup('v_peoples', 'PEOPLE_ID', 'USER_ID')
        if user_id_pid:
            return user_id_pid, 2

        # 3. Search Specialized Groups
        ug_id = quick_lookup('v_usergroups', 'SYSTEM_ID', 'GROUP_ID')
        if ug_id:
            return ug_id, 1

        ng_id = quick_lookup('v_nativegroups', 'SYSTEM_ID', 'GROUP_ID')
        if ng_id:
            return ng_id, 1

        # 4. Search Users (Legacy)
        user_id_psid = quick_lookup('v_peoples', 'PEOPLE_SYSTEM_ID', 'USER_ID')
        if user_id_psid:
            return user_id_psid, 2

    except Exception:
        pass

    return None, None

def set_trustees(dst, doc_id, library, trustees, security_enabled="1"):
    try:
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

                return False, f"Error: {error_msg}"

            except Fault as f:
                fault_msg = f.message or str(f)
                if "unknown trustee" in fault_msg.lower() and attempts < max_attempts:
                    changed_any = False

                    # 1. Try to resolve numeric IDs
                    for item in candidate_list:
                        if item['name'].isdigit():
                            resolved_name, resolved_flag = resolve_trustee_system_id(dst, item['name'])
                            if resolved_name:
                                item['name'] = resolved_name
                                item['flag'] = resolved_flag
                                changed_any = True

                    # 2. If no resolution, fallback to swapping inferred flags
                    if not changed_any:
                        for item in candidate_list:
                            if item['name'].isdigit() and item['flag'] == 2:
                                item['flag'] = 1
                                changed_any = True

                        if not changed_any:
                            for item in candidate_list:
                                if item['inferred'] and item['flag'] == 2:
                                    item['flag'] = 1
                                    changed_any = True

                    if changed_any:
                        continue

                return False, f"SOAP Fault: {fault_msg}"

            except Exception as e:
                return False, str(e)

        return False, "Failed to set trustees after retries."

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
                t_names = response.trustees.trusteeNames.string if (
                        response.trustees.trusteeNames and hasattr(response.trustees.trusteeNames,
                                                                   'string')) else []
                t_flags = response.trustees.trusteeFlags.int if (
                        response.trustees.trusteeFlags and hasattr(response.trustees.trusteeFlags, 'int')) else []
                t_rights = response.trustees.trusteeRights.int if (
                        response.trustees.trusteeRights and hasattr(response.trustees.trusteeRights, 'int')) else []

                count = response.trustees.trusteeCount if response.trustees.trusteeCount is not None else 0
                limit = min(count, len(t_names), len(t_flags), len(t_rights))

                for i in range(limit):
                    trustees.append({
                        'username': t_names[i],
                        'flag': t_flags[i],
                        'rights': t_rights[i]
                    })
                return trustees
        return []
    except Exception:
        return []

def get_group_members(dst, group_id, library='RTA_MAIN'):
    """
    Retrieves the members of a group by searching v_peoplegroups.
    Uses the pattern from Fiddler trace: GROUP_ID + LAST_UPDATE with date range.
    """
    try:
        svc_client = get_soap_client('BasicHttpBinding_IDMSvc')

        # Pattern from Fiddler: Use GROUP_ID and LAST_UPDATE with wide date range
        criteria_names = ['GROUP_ID', 'LAST_UPDATE']
        criteria_values = [str(group_id), '1900-01-01 00:00:00 TO 3000-01-01 00:00:00']

        ret_props = ['USER_ID', 'FULL_NAME', 'PEOPLE_SYSTEM_ID', 'Disabled', 'ALLOW_LOGIN']

        search_call = {
            'call': {
                'dstIn': dst,
                'objectType': 'v_peoplegroups',
                'signature': {
                    'libraries': {'string': [library]},
                    'criteria': {
                        'criteriaCount': len(criteria_names),
                        'criteriaNames': {'string': criteria_names},
                        'criteriaValues': {'string': criteria_values}
                    },
                    'retProperties': {'string': ret_props},
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

        get_data_call = {
            'call': {
                'resultSetID': result_set_id,
                'requestedRows': 500,
                'startingRow': 0
            }
        }

        data_reply = getattr(data_client.service, method_name)(**get_data_call)

        members = []
        seen_ids = set()

        # Process XML RowNode first (preferred)
        row_nodes = getattr(data_reply, 'rowNode', None) or getattr(data_reply, 'RowNode', None)
        if row_nodes:
            for row in row_nodes:
                vals = row.propValues.anyType if hasattr(row, 'propValues') else []
                if vals and len(vals) >= 2:
                    user_id = clean_string(str(vals[0])) if vals[0] else ''
                    full_name = clean_string(str(vals[1])) if len(vals) > 1 and vals[1] else ''

                    # Validate USER_ID vs FULL_NAME
                    if not is_likely_user_id(user_id) and is_likely_user_id(full_name):
                        user_id, full_name = full_name, user_id

                    disabled = str(vals[3]).upper() if len(vals) > 3 and vals[3] else 'N'

                    if user_id and user_id not in seen_ids and disabled != 'Y':
                        seen_ids.add(user_id)
                        members.append({
                            'user_id': user_id,
                            'full_name': full_name if full_name else user_id,
                            'system_id': str(vals[2]).strip() if len(vals) > 2 and vals[2] else None,
                            'disabled': disabled,
                            'allow_login': str(vals[4]) if len(vals) > 4 and vals[4] else None
                        })

        # Fallback: Use utility parser for binary result buffer
        if not members and hasattr(data_reply, 'resultSetData') and data_reply.resultSetData:
            container = data_reply.resultSetData
            actual_rows = getattr(container, 'actualRows', None)
            columns = getattr(container, 'columns', None)
            if hasattr(container, 'resultBuffer') and container.resultBuffer:
                column_names = ['USER_ID', 'FULL_NAME', 'PEOPLE_SYSTEM_ID', 'Disabled', 'ALLOW_LOGIN']
                parsed = parse_group_members_buffer(container.resultBuffer, column_names, actual_rows, columns)
                for p in parsed:
                    user_id = p.get('user_id', '')
                    if user_id and user_id not in seen_ids:
                        seen_ids.add(user_id)
                        members.append({
                            'user_id': user_id,
                            'full_name': p.get('full_name', user_id)
                        })

        try:
            svc_client.service.ReleaseData(call={'resultSetID': result_set_id})
            svc_client.service.ReleaseObject(call={'objectID': result_set_id})
        except:
            pass

        return members

    except Exception:
        return []

def search_users_in_group(dst, group_id, search_term, library='RTA_MAIN'):
    """
    Search for users within a specific group.
    Matches the exact Fiddler trace pattern for v_peoplegroups queries.

    Column order from retProperties:
    0 = USER_ID
    1 = FULL_NAME
    2 = PEOPLE_SYSTEM_ID
    3 = Disabled
    4 = ALLOW_LOGIN
    """
    try:
        svc_client = get_soap_client('BasicHttpBinding_IDMSvc')

        if not group_id:
            return []

        # Exact pattern from Fiddler trace: GROUP_ID + LAST_UPDATE with date range
        search_call = {
            'call': {
                'dstIn': dst,
                'objectType': 'v_peoplegroups',
                'signature': {
                    'libraries': {'string': [library]},
                    'criteria': {
                        'criteriaCount': 2,
                        'criteriaNames': {'string': ['GROUP_ID', 'LAST_UPDATE']},
                        'criteriaValues': {'string': [group_id, '1900-01-01 00:00:00 TO 3000-01-01 00:00:00']}
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

        data_client = find_client_with_operation('GetDataW') or svc_client
        method_name = 'GetDataW' if hasattr(data_client.service, 'GetDataW') else 'GetData'

        get_data_call = {'call': {'resultSetID': result_set_id, 'requestedRows': 500, 'startingRow': 0}}
        data_reply = getattr(data_client.service, method_name)(**get_data_call)

        members = []
        seen_ids = set()

        def add_member(user_id, full_name, disabled=None):
            if not user_id or user_id in seen_ids:
                return

            # Clean and validate
            user_id = clean_string(user_id)
            full_name = clean_string(full_name) if full_name else user_id

            # CRITICAL: Determine which value is actually the USER_ID
            # USER_IDs don't have spaces; FULL_NAMEs typically do
            swap_needed = False

            # Check if user_id looks like a full name (has spaces)
            if looks_like_full_name(user_id):
                # user_id has spaces - it's probably actually the full_name
                if is_likely_user_id(full_name) and not looks_like_full_name(full_name):
                    swap_needed = True
                elif ' ' not in full_name and ' ' in user_id:
                    # full_name has no spaces, user_id has spaces - swap
                    swap_needed = True

            # Also check if full_name looks more like a user_id than user_id does
            if not swap_needed and not is_likely_user_id(user_id) and is_likely_user_id(full_name):
                swap_needed = True

            if swap_needed:
                user_id, full_name = full_name, user_id

            if not user_id:
                return

            # Filter by search term if provided
            if search_term:
                search_lower = search_term.lower()
                if search_lower not in user_id.lower() and search_lower not in (full_name or '').lower():
                    return

            # Skip disabled users
            if disabled and str(disabled).upper() == 'Y':
                return

            seen_ids.add(user_id)
            members.append({'user_id': user_id, 'full_name': full_name or user_id})

        # Try RowNodes FIRST (more reliable)
        row_nodes = getattr(data_reply, 'rowNode', None) or getattr(data_reply, 'RowNode', None)

        if row_nodes:
            for row in row_nodes:
                vals = row.propValues.anyType if hasattr(row, 'propValues') else []
                if vals and len(vals) >= 2:
                    # Column order: USER_ID, FULL_NAME, PEOPLE_SYSTEM_ID, Disabled, ALLOW_LOGIN
                    user_id = str(vals[0]).strip() if vals[0] else ''
                    full_name = str(vals[1]).strip() if len(vals) > 1 and vals[1] else ''
                    disabled = str(vals[3]).upper() if len(vals) > 3 and vals[3] else 'N'

                    if user_id:
                        add_member(user_id, full_name, disabled)

        # Fallback: Try Binary Buffer
        if not members and hasattr(data_reply, 'resultSetData') and data_reply.resultSetData:
            container = data_reply.resultSetData
            actual_rows = getattr(container, 'actualRows', None)
            columns = getattr(container, 'columns', None)
            if hasattr(container, 'resultBuffer') and container.resultBuffer:
                # Use the specialized group members parser
                column_names = ['USER_ID', 'FULL_NAME', 'PEOPLE_SYSTEM_ID', 'Disabled', 'ALLOW_LOGIN']
                parsed_members = parse_group_members_buffer(container.resultBuffer, column_names, actual_rows, columns)

                for p in parsed_members:
                    user_id = p.get('user_id', '')
                    full_name = p.get('full_name', user_id)

                    # Filter by search term
                    if search_term:
                        search_lower = search_term.lower()
                        if search_lower not in user_id.lower() and search_lower not in full_name.lower():
                            continue

                    if user_id and user_id not in seen_ids:
                        seen_ids.add(user_id)
                        members.append({'user_id': user_id, 'full_name': full_name})

        try:
            svc_client.service.ReleaseData(call={'resultSetID': result_set_id})
            svc_client.service.ReleaseObject(call={'objectID': result_set_id})
        except:
            pass

        return members

    except Exception:
        return []

def get_all_users(dst, search_term='', library='RTA_MAIN'):
    """
    Get all users (v_peoples) with optional search filter.
    Uses the pattern from Fiddler trace.
    """
    try:
        svc_client = get_soap_client('BasicHttpBinding_IDMSvc')

        # Build criteria
        if search_term:
            criteria_names = ['USER_ID', 'LAST_UPDATE']
            criteria_values = [f'*{search_term}*', '1900-01-01 00:00:00 TO 3000-01-01 00:00:00']
        else:
            criteria_names = ['LAST_UPDATE']
            criteria_values = ['1900-01-01 00:00:00 TO 3000-01-01 00:00:00']

        search_call = {
            'call': {
                'dstIn': dst,
                'objectType': 'v_peoples',
                'signature': {
                    'libraries': {'string': [library]},
                    'criteria': {
                        'criteriaCount': len(criteria_names),
                        'criteriaNames': {'string': criteria_names},
                        'criteriaValues': {'string': criteria_values}
                    },
                    'retProperties': {'string': ['USER_ID', 'FULL_NAME', 'SYSTEM_ID']},
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

        users = []
        seen_ids = set()

        # Try RowNodes FIRST (more reliable)
        row_nodes = getattr(data_reply, 'rowNode', None) or getattr(data_reply, 'RowNode', None)
        if row_nodes:
            for row in row_nodes:
                vals = row.propValues.anyType if hasattr(row, 'propValues') else []
                if vals and vals[0]:
                    user_id = str(vals[0]).strip()
                    if user_id not in seen_ids:
                        seen_ids.add(user_id)
                        users.append({
                            'user_id': user_id,
                            'full_name': str(vals[1]).strip() if len(vals) > 1 and vals[1] else user_id,
                            'system_id': str(vals[2]).strip() if len(vals) > 2 and vals[2] else None
                        })

        # Fallback: Binary buffer
        if not users and hasattr(data_reply, 'resultSetData') and data_reply.resultSetData:
            container = data_reply.resultSetData
            actual_rows = getattr(container, 'actualRows', None)
            columns = getattr(container, 'columns', None)
            if hasattr(container, 'resultBuffer') and container.resultBuffer:
                parsed = parse_user_result_buffer(container.resultBuffer, actual_rows, columns)
                for p in parsed:
                    user_id = p.get('user_id')
                    if user_id and user_id not in seen_ids:
                        seen_ids.add(user_id)
                        users.append({
                            'user_id': user_id,
                            'full_name': p.get('full_name', user_id)
                        })

        try:
            svc_client.service.ReleaseData(call={'resultSetID': result_set_id})
            svc_client.service.ReleaseObject(call={'objectID': result_set_id})
        except:
            pass

        return users

    except Exception:
        return []

def get_groups_for_user(dst, username, library='RTA_MAIN'):
    """
    Get all groups that a specific user belongs to.

    Matches Fiddler trace pattern:
    - objectType: v_peoplegroups
    - criteria: USER_ID = {username}
    - retProperties: GROUP_ID, DISABLED

    If user belongs to DOCS_SUPERVISORS or ADMINS group, returns ALL groups.
    Otherwise returns only the groups the user belongs to.
    """
    ADMIN_GROUPS = {'DOCS_SUPERVISORS', 'ADMINS'}

    group_ids = []
    seen = set()
    is_admin = False

    try:
        svc_client = get_soap_client('BasicHttpBinding_IDMSvc')

        # Exact pattern from Fiddler trace
        search_call = {
            'call': {
                'dstIn': dst,
                'objectType': 'v_peoplegroups',
                'signature': {
                    'libraries': {'string': [library]},
                    'criteria': {
                        'criteriaCount': 1,
                        'criteriaNames': {'string': ['USER_ID']},
                        'criteriaValues': {'string': [username]}
                    },
                    'retProperties': {'string': ['GROUP_ID', 'DISABLED']},
                    'sortProps': {
                        'propertyCount': 0,
                        'propertyNames': {'string': []},
                        'propertyFlags': {'int': []}
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
            data_reply = getattr(data_client.service, method_name)(**get_data_call)

            # Try row nodes first
            row_nodes = getattr(data_reply, 'rowNode', None) or getattr(data_reply, 'RowNode', None)
            if row_nodes:
                for row in row_nodes:
                    vals = row.propValues.anyType if hasattr(row, 'propValues') else []
                    if vals and vals[0]:
                        gid = clean_string(str(vals[0]))
                        disabled = str(vals[1]).upper() if len(vals) > 1 and vals[1] else 'N'
                        if gid and gid not in seen and disabled != 'Y' and is_valid_group_id(gid):
                            seen.add(gid)
                            group_ids.append(gid)
                            # Check if user is admin
                            if gid.upper() in ADMIN_GROUPS:
                                is_admin = True

            # Fallback: Binary buffer parsing
            if not group_ids and hasattr(data_reply, 'resultSetData') and data_reply.resultSetData:
                container = data_reply.resultSetData
                actual_rows = getattr(container, 'actualRows', None)
                columns = getattr(container, 'columns', None)
                if hasattr(container, 'resultBuffer') and container.resultBuffer:
                    parsed = parse_groups_buffer(container.resultBuffer, actual_rows, columns)
                    for p in parsed:
                        gid = p.get('group_id', '')
                        disabled = p.get('disabled', 'N').upper()
                        if gid and gid not in seen and disabled != 'Y' and is_valid_group_id(gid):
                            seen.add(gid)
                            group_ids.append(gid)
                            # Check if user is admin
                            if gid.upper() in ADMIN_GROUPS:
                                is_admin = True

            try:
                svc_client.service.ReleaseData(call={'resultSetID': result_set_id})
                svc_client.service.ReleaseObject(call={'objectID': result_set_id})
            except:
                pass

    except Exception:
        pass

    # If user is admin, return ALL groups
    if is_admin:
        return get_all_groups(dst, library)

    # Return user's groups
    if group_ids:
        return [{'group_id': gid, 'group_name': gid} for gid in group_ids]

    # Fallback: return all groups (if couldn't determine user's groups)
    return get_all_groups(dst, library)