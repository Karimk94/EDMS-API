import logging
import re
from zeep import xsd
from zeep.exceptions import Fault
from .base import get_soap_client, find_client_with_operation
from .utils import parse_user_result_buffer

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

    Based on Fiddler trace analysis:
    - v_peoplegroups is successfully queried with GROUP_ID + LAST_UPDATE criteria
    - Groups like RTA_ADS_CORRESP exist but may not show in simple queries

    Strategy: Query v_peoplegroups with wide date range and extract distinct GROUP_IDs

    Note: The DMS server filters results based on the authenticated user's session (DST).
    Users with admin/supervisor access will see all groups, while regular users may only
    see groups they have permission to view.
    """
    groups = []
    seen_ids = set()

    def add_group(gid, gname=None, gdesc=""):
        # Clean the group ID
        if gid:
            gid = str(gid).strip()

        # Validate the group ID
        if not is_valid_group_id(gid):
            return

        if gid not in seen_ids:
            seen_ids.add(gid)
            # Also validate/clean the group name
            if gname:
                gname = str(gname).strip()
                if not is_valid_group_id(gname):
                    gname = gid  # Fall back to group_id if name is invalid
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
                if hasattr(container, 'resultBuffer') and container.resultBuffer:
                    parsed = parse_user_result_buffer(container.resultBuffer)
                    if parsed:
                        results.extend(extract_func(parsed, 'buffer'))

            # Try row nodes
            row_nodes = getattr(d_resp, 'rowNode', None) or getattr(d_resp, 'RowNode', None)
            if row_nodes:
                results.extend(extract_func(row_nodes, 'rows'))

        except Exception as e:
            logging.debug(f"Error fetching results: {e}")
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

    # ==========================================================================
    # STRATEGY 1: Query v_peoplegroups with wide date range (PROVEN WORKING)
    # Extract distinct GROUP_ID values - this matches the Fiddler trace pattern
    # ==========================================================================
    try:
        # logging.info("Strategy 1: Extracting distinct groups from v_peoplegroups...")

        # Use very wide date range to get ALL records
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
                    'maxRows': 0  # No limit
                }
            }
        }

        resp = svc_client.service.Search(**search_call)
        if resp and resp.resultCode == 0 and resp.resultSetID:
            def extract_group_ids(data, data_type):
                extracted = []
                if data_type == 'buffer':
                    for p in data:
                        gid = p.get('user_id') or p.get('full_name')  # GROUP_ID might be in first field
                        if gid:
                            extracted.append({'group_id': gid})
                else:
                    for row in data:
                        vals = row.propValues.anyType if hasattr(row, 'propValues') else []
                        if vals and vals[0]:
                            extracted.append({'group_id': str(vals[0]).strip()})
                return extracted

            results = fetch_and_release(resp.resultSetID, extract_group_ids)
            for r in results:
                add_group(r['group_id'], r['group_id'])

            # logging.info(f"Strategy 1 (v_peoplegroups) found {len(results)} records, {len(seen_ids)} distinct groups")
    except Exception as e:
        logging.debug(f"Strategy 1 failed: {e}")

    # ==========================================================================
    # STRATEGY 2: Try v_groups with LAST_UPDATE range
    # ==========================================================================
    try:
        # logging.info("Strategy 2: Querying v_groups...")
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
                if data_type == 'rows':
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

            # logging.info(f"Strategy 2 (v_groups) found {len(results)} groups")
    except Exception as e:
        logging.debug(f"Strategy 2 failed: {e}")

    # ==========================================================================
    # STRATEGY 3: Try v_usergroups
    # ==========================================================================
    try:
        # logging.info("Strategy 3: Querying v_usergroups...")
        search_call = {
            'call': {
                'dstIn': dst,
                'objectType': 'v_usergroups',
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
                if data_type == 'rows':
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

            # logging.info(f"Strategy 3 (v_usergroups) found {len(results)} groups")
    except Exception as e:
        logging.debug(f"Strategy 3 failed: {e}")

    # ==========================================================================
    # STRATEGY 4: Try v_nativegroups
    # ==========================================================================
    try:
        # logging.info("Strategy 4: Querying v_nativegroups...")
        search_call = {
            'call': {
                'dstIn': dst,
                'objectType': 'v_nativegroups',
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
                if data_type == 'rows':
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

            # logging.info(f"Strategy 4 (v_nativegroups) found {len(results)} groups")
    except Exception as e:
        logging.debug(f"Strategy 4 failed: {e}")

    # ==========================================================================
    # STRATEGY 5: Try wildcard search on v_groups with GROUP_ID
    # ==========================================================================
    try:
        # logging.info("Strategy 5: Wildcard search on v_groups...")
        for wildcard in ['*', '%', '?*']:
            search_call = {
                'call': {
                    'dstIn': dst,
                    'objectType': 'v_groups',
                    'signature': {
                        'libraries': {'string': [library]},
                        'criteria': {
                            'criteriaCount': 1,
                            'criteriaNames': {'string': ['GROUP_ID']},
                            'criteriaValues': {'string': [wildcard]}
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
                    if data_type == 'rows':
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
                new_found = 0
                for r in results:
                    if r['group_id'] and r['group_id'] not in seen_ids:
                        add_group(r['group_id'], r.get('group_name'), r.get('description', ''))
                        new_found += 1

                # logging.info(f"Strategy 5 (wildcard '{wildcard}') found {new_found} new groups")
                if new_found > 0:
                    break
    except Exception as e:
        logging.debug(f"Strategy 5 failed: {e}")

    # ==========================================================================
    # STRATEGY 6: Search v_peoplegroups with wildcard on GROUP_ID (different approach)
    # ==========================================================================
    try:
        # logging.info("Strategy 6: v_peoplegroups with GROUP_ID wildcard...")
        search_call = {
            'call': {
                'dstIn': dst,
                'objectType': 'v_peoplegroups',
                'signature': {
                    'libraries': {'string': [library]},
                    'criteria': {
                        'criteriaCount': 1,
                        'criteriaNames': {'string': ['GROUP_ID']},
                        'criteriaValues': {'string': ['*']}
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
                        gid = p.get('user_id') or p.get('full_name')
                        if gid:
                            extracted.append({'group_id': gid})
                else:
                    for row in data:
                        vals = row.propValues.anyType if hasattr(row, 'propValues') else []
                        if vals and vals[0]:
                            extracted.append({'group_id': str(vals[0]).strip()})
                return extracted

            results = fetch_and_release(resp.resultSetID, extract_group_ids)
            new_found = 0
            for r in results:
                if r['group_id'] and r['group_id'] not in seen_ids:
                    add_group(r['group_id'], r['group_id'])
                    new_found += 1

            # logging.info(f"Strategy 6 found {new_found} new groups")
    except Exception as e:
        logging.debug(f"Strategy 6 failed: {e}")

    # logging.info(f"Total groups found across all strategies: {len(groups)}")

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
    # logging.info(f"Resolving System ID: {sys_id}")
    try:
        svc_client = get_soap_client('BasicHttpBinding_IDMSvc')
        data_client = find_client_with_operation('GetDataW') or svc_client
        method_name = 'GetDataW' if hasattr(data_client.service, 'GetDataW') else 'GetData'

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
                            # logging.info(f"Found match in {obj_type}: {val}")
                            return str(val)
            except Exception as e:
                logging.debug(f"Lookup failed for {obj_type}: {e}")
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

    except Exception as e:
        logging.error(f"Error resolving system ID {sys_id}: {e}")

    return None, None

def set_trustees(dst, doc_id, library, trustees, security_enabled="1"):
    try:
        # logging.info(f"Setting trustees for doc_id: {doc_id} in library: {library}. Raw count: {len(trustees)}")
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

            # logging.info(f"Trustee Payload (Attempt {attempts}) -> Names: {current_names} | Flags: {current_flags} | Rights: {current_rights}")

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
                            # logging.info(f"Resolving ID {item['name']}...")
                            resolved_name, resolved_flag = resolve_trustee_system_id(dst, item['name'])
                            if resolved_name:
                                # logging.info(f"Resolved {item['name']} -> {resolved_name} (Type: {'User' if resolved_flag == 2 else 'Group'})")
                                item['name'] = resolved_name
                                item['flag'] = resolved_flag
                                changed_any = True
                            else:
                                logging.warning(f"Resolution failed for {item['name']}.")

                    # 2. If no resolution, fallback to swapping inferred flags
                    if not changed_any:
                        for item in candidate_list:
                            if item['name'].isdigit() and item['flag'] == 2:
                                item['flag'] = 1
                                changed_any = True

                        if changed_any:
                            logging.info("Forced swap of numeric trustee from User to Group.")
                        else:
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
    except Exception as e:
        logging.error(f"Error getting trustees: {e}")
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

        # Use utility parser for binary result buffer
        if hasattr(data_reply, 'resultSetData') and data_reply.resultSetData:
            container = data_reply.resultSetData
            if hasattr(container, 'resultBuffer') and container.resultBuffer:
                parsed = parse_user_result_buffer(container.resultBuffer)
                if parsed:
                    members.extend(parsed)

        # Fallback: Check for RowNodes
        row_nodes = getattr(data_reply, 'rowNode', None) or getattr(data_reply, 'RowNode', None)
        if row_nodes:
            for row in row_nodes:
                vals = row.propValues.anyType
                if vals:
                    members.append({
                        'user_id': vals[0],
                        'full_name': vals[1],
                        'system_id': vals[2] if len(vals) > 2 else None,
                        'disabled': vals[3] if len(vals) > 3 else None,
                        'allow_login': vals[4] if len(vals) > 4 else None
                    })

        try:
            svc_client.service.ReleaseData(call={'resultSetID': result_set_id})
            svc_client.service.ReleaseObject(call={'objectID': result_set_id})
        except:
            pass

        return members

    except Exception as e:
        logging.error(f"Error getting group members: {e}")
        return []

def search_users_in_group(dst, group_id, search_term, library='RTA_MAIN'):
    """
    Search for users within a specific group.
    Matches the Fiddler trace pattern for v_peoplegroups queries.
    """
    try:
        svc_client = get_soap_client('BasicHttpBinding_IDMSvc')

        if not group_id:
            return []

        # Use wide date range like Fiddler trace
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
            logging.warning(f"Search failed for group {group_id}")
            return []

        result_set_id = search_reply.resultSetID

        data_client = find_client_with_operation('GetDataW') or svc_client
        method_name = 'GetDataW' if hasattr(data_client.service, 'GetDataW') else 'GetData'

        get_data_call = {'call': {'resultSetID': result_set_id, 'requestedRows': 500, 'startingRow': 0}}
        data_reply = getattr(data_client.service, method_name)(**get_data_call)

        members = []
        seen_ids = set()

        def clean_text(text):
            if not text:
                return ""
            return re.sub(r'[\x00-\x1f\x7f]', '', str(text)).strip()

        def add_member(user_id, full_name, disabled=None, allow_login=None):
            if not user_id or user_id in seen_ids:
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

        # Process XML RowNode
        row_nodes = getattr(data_reply, 'rowNode', None) or getattr(data_reply, 'RowNode', None)

        if row_nodes:
            for row in row_nodes:
                vals = row.propValues.anyType if hasattr(row, 'propValues') else []
                if vals:
                    user_id = clean_text(vals[0] if len(vals) > 0 else None)
                    full_name = clean_text(vals[1] if len(vals) > 1 else None)
                    disabled = vals[3] if len(vals) > 3 else None
                    allow_login = vals[4] if len(vals) > 4 else None

                    if user_id:
                        add_member(user_id, full_name, disabled, allow_login)

        # Fallback: Binary Buffer
        if not members and hasattr(data_reply, 'resultSetData') and data_reply.resultSetData:
            container = data_reply.resultSetData
            if hasattr(container, 'resultBuffer') and container.resultBuffer:
                parsed_items = parse_user_result_buffer(container.resultBuffer)
                for p in parsed_items:
                    user_id = clean_text(p.get('user_id'))
                    full_name = clean_text(p.get('full_name'))
                    if user_id:
                        add_member(user_id, full_name)

        try:
            svc_client.service.ReleaseData(call={'resultSetID': result_set_id})
            svc_client.service.ReleaseObject(call={'objectID': result_set_id})
        except:
            pass

        return members

    except Exception as e:
        logging.error(f"Error searching group users: {e}", exc_info=True)
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

        # Process results
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

        # Binary buffer fallback
        if not users and hasattr(data_reply, 'resultSetData') and data_reply.resultSetData:
            container = data_reply.resultSetData
            if hasattr(container, 'resultBuffer') and container.resultBuffer:
                parsed = parse_user_result_buffer(container.resultBuffer)
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

    except Exception as e:
        logging.error(f"Error getting all users: {e}")
        return []

def get_groups_for_user(dst, username, library='RTA_MAIN'):
    """
    Get all groups that a specific user belongs to.
    """
    group_ids = []

    try:
        svc_client = get_soap_client('BasicHttpBinding_IDMSvc')

        # Search v_peoplegroups for this user
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
                    'retProperties': {'string': ['GROUP_ID']},
                    'maxRows': 0
                }
            }
        }

        search_reply = svc_client.service.Search(**search_call)

        if search_reply and search_reply.resultCode == 0 and search_reply.resultSetID:
            result_set_id = search_reply.resultSetID
            data_client = find_client_with_operation('GetDataW') or svc_client
            method_name = 'GetDataW' if hasattr(data_client.service, 'GetDataW') else 'GetData'

            get_data_call = {'call': {'resultSetID': result_set_id, 'requestedRows': 100, 'startingRow': 0}}
            data_reply = getattr(data_client.service, method_name)(**get_data_call)

            row_nodes = getattr(data_reply, 'rowNode', None) or getattr(data_reply, 'RowNode', None)
            if row_nodes:
                for row in row_nodes:
                    vals = row.propValues.anyType if hasattr(row, 'propValues') else []
                    if vals and vals[0]:
                        gid = str(vals[0]).strip()
                        if gid not in group_ids:
                            group_ids.append(gid)

            try:
                svc_client.service.ReleaseData(call={'resultSetID': result_set_id})
                svc_client.service.ReleaseObject(call={'objectID': result_set_id})
            except:
                pass

    except Exception as e:
        logging.debug(f"Error getting groups for user {username}: {e}")

    # Return as list of group dicts
    if group_ids:
        return [{'group_id': gid, 'group_name': gid} for gid in group_ids]

    # Fallback: return all groups
    return get_all_groups(dst, library)