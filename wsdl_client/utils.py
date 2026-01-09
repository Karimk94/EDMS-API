import re
import base64
import zlib
import struct

def parse_dms_buffer(buffer_b64, column_names=None, actual_rows=None, actual_cols=None):
    """
    Parse a DMS binary result buffer from base64-encoded data.
    """
    if not buffer_b64:
        return []

    # Handle case where buffer is already bytes vs base64 string
    if isinstance(buffer_b64, bytes):
        raw = buffer_b64
    else:
        try:
            raw = base64.b64decode(buffer_b64)
        except Exception:
            return []

    if len(raw) < 8:
        return []

    original_raw = raw
    decompressed = False

    # Try multiple decompression strategies
    # Strategy 1: Check for 0xFFFFFFFF marker (custom DMS compression)
    if raw[:4] == b'\xff\xff\xff\xff':
        try:
            compressed_data = raw[8:]
            raw = zlib.decompress(compressed_data, -zlib.MAX_WBITS)
            decompressed = True
        except:
            try:
                raw = zlib.decompress(raw[8:])
                decompressed = True
            except:
                pass

    # Strategy 2: Standard zlib header (78 9c or 78 da)
    if not decompressed and (raw[:2] == b'\x78\x9c' or raw[:2] == b'\x78\xda' or raw[:2] == b'\x78\x01'):
        try:
            raw = zlib.decompress(raw)
            decompressed = True
        except:
            pass

    # Strategy 3: Try raw deflate on entire buffer
    if not decompressed:
        try:
            raw = zlib.decompress(original_raw, -zlib.MAX_WBITS)
            decompressed = True
        except:
            pass

    # Strategy 4: Try standard zlib on entire buffer
    if not decompressed:
        try:
            raw = zlib.decompress(original_raw)
            decompressed = True
        except:
            pass

    # Strategy 5: Skip first 4 bytes and try (some formats have a size prefix)
    if not decompressed and len(original_raw) > 4:
        try:
            raw = zlib.decompress(original_raw[4:], -zlib.MAX_WBITS)
            decompressed = True
        except:
            pass
        if not decompressed:
            try:
                raw = zlib.decompress(original_raw[4:])
                decompressed = True
            except:
                pass

    # Strategy 6: Skip first 8 bytes and try
    if not decompressed and len(original_raw) > 8:
        try:
            raw = zlib.decompress(original_raw[8:], -zlib.MAX_WBITS)
            decompressed = True
        except:
            pass
        if not decompressed:
            try:
                raw = zlib.decompress(original_raw[8:])
                decompressed = True
            except:
                pass

    if not decompressed:
        raw = original_raw

    if len(raw) < 8:
        return []

    # If we have actual dimensions from metadata, use those directly
    if actual_rows is not None and actual_cols is not None and actual_rows > 0 and actual_cols > 0:
        return parse_buffer_no_header(raw, column_names, actual_cols, actual_rows)

    # Parse header: row count and column count
    row_count = struct.unpack('<I', raw[0:4])[0]
    col_count = struct.unpack('<I', raw[4:8])[0]

    # Sanity check dimensions
    if row_count == 0 or col_count == 0:
        return []

    if row_count > 10000 or col_count > 100:
        return parse_buffer_no_header(raw, column_names, None, None)

    return parse_buffer_with_header(raw, row_count, col_count, column_names)

def parse_buffer_no_header(raw, column_names=None, expected_cols=None, max_rows=None):
    """Parse buffer using metadata dimensions."""
    results = []

    strategies = [
        (8, 4, '<'),  # 4-byte LE length, skip 8-byte header
        (0, 4, '<'),  # 4-byte LE length, no header skip
        (8, 2, '<'),  # 2-byte LE length, skip 8-byte header
        (0, 2, '<'),  # 2-byte LE length, no header skip
        (4, 4, '<'),  # 4-byte LE length, skip 4-byte header
        (4, 2, '<'),  # 2-byte LE length, skip 4-byte header
        (0, 2, '>'),  # 2-byte BE length, no header skip
        (2, 2, '>'),  # 2-byte BE length, skip 2 bytes
    ]

    for start_pos, length_bytes, endian in strategies:
        results = _parse_strings_from_position(raw, start_pos, column_names, expected_cols, max_rows, length_bytes,
                                               endian)
        if results:
            break

    return results

def _parse_strings_from_position(raw, start_pos, column_names=None, expected_cols=None, max_rows=None, length_bytes=4, endian='<'):
    """Parse length-prefixed UTF-16LE strings starting at given position.

    Format: [4-byte length][UTF-16LE string][2-byte null terminator]...
    """
    results = []
    pos = start_pos
    current_row = {}
    col_idx = 0
    row_count = 0

    fmt = f'{endian}I' if length_bytes == 4 else f'{endian}H'

    while pos < len(raw) - length_bytes:
        # Stop if we've reached max rows
        if max_rows and row_count >= max_rows:
            break

        # Read length prefix
        str_len = struct.unpack(fmt, raw[pos:pos + length_bytes])[0]
        pos += length_bytes

        # Validate length is reasonable
        if str_len > 5000:
            # Invalid length - this parsing approach isn't working
            if row_count == 0 and col_idx == 0:
                return []  # Failed at start, wrong position
            # Try skipping just one byte and continue
            pos -= (length_bytes - 1)
            continue

        if str_len == 0:
            value = ''
        else:
            byte_len = str_len * 2
            if pos + byte_len > len(raw):
                break
            try:
                value = raw[pos:pos + byte_len].decode('utf-16-le')
                pos += byte_len
            except:
                pos += byte_len
                value = ''

        # Skip 2-byte null terminator (UTF-16LE null)
        if pos + 2 <= len(raw):
            pos += 2

        col_key = column_names[col_idx] if column_names and col_idx < len(column_names) else f'col{col_idx}'
        current_row[col_key] = value
        col_idx += 1

        # If we have expected columns, check if row is complete
        if expected_cols and col_idx >= expected_cols:
            if any(v for v in current_row.values()):
                results.append(current_row)
                row_count += 1
            current_row = {}
            col_idx = 0
        elif not expected_cols and col_idx >= 10:
            # Safety limit without expected cols
            if any(v for v in current_row.values()):
                results.append(current_row)
                row_count += 1
            current_row = {}
            col_idx = 0

    # Save partial row
    if col_idx > 0 and any(v for v in current_row.values()):
        results.append(current_row)

    return results

def parse_buffer_with_header(raw, row_count, col_count, column_names=None):
    """Parse buffer with known row/column header."""
    results = []
    pos = 8  # Start after header

    for row_idx in range(row_count):
        row_data = {}

        for col_idx in range(col_count):
            if pos + 4 > len(raw):
                break

            # Read string length (in UTF-16 characters)
            str_len = struct.unpack('<I', raw[pos:pos + 4])[0]
            pos += 4

            if str_len == 0:
                value = ''
            elif str_len > 5000:
                # Unreasonably long string - likely corrupted
                value = ''
            else:
                byte_len = str_len * 2  # UTF-16LE = 2 bytes per character
                if pos + byte_len > len(raw):
                    value = ''
                else:
                    try:
                        value = raw[pos:pos + byte_len].decode('utf-16-le')
                        pos += byte_len
                    except UnicodeDecodeError:
                        value = ''
                        pos += byte_len

            # Assign to appropriate key
            if column_names and col_idx < len(column_names):
                key = column_names[col_idx]
            else:
                key = f'col{col_idx}'

            row_data[key] = value

        if row_data:
            results.append(row_data)

    return results

def parse_user_result_buffer(buffer_b64, actual_rows=None, actual_cols=None):
    """
    Parse a user/group result buffer, returning standardized fields.

    This is a wrapper around parse_dms_buffer that attempts to identify
    and normalize common field patterns.

    The buffer may contain various column combinations like:
    - USER_ID, FULL_NAME, SYSTEM_ID
    - GROUP_ID, DISABLED
    - USER_ID, FULL_NAME, PEOPLE_SYSTEM_ID, Disabled, ALLOW_LOGIN

    Args:
        buffer_b64: Base64 encoded buffer
        actual_rows: Number of rows from resultSetData.actualRows (if available)
        actual_cols: Number of columns from resultSetData.columns (if available)

    Returns:
        List of dicts with normalized keys (user_id, full_name, system_id, etc.)
    """
    rows = parse_dms_buffer(buffer_b64, None, actual_rows, actual_cols)

    if not rows:
        return []

    results = []

    for row in rows:
        normalized = {}

        # Get values by position (col0, col1, col2, ...)
        col0 = clean_string(row.get('col0', ''))
        col1 = clean_string(row.get('col1', ''))
        col2 = clean_string(row.get('col2', ''))
        col3 = clean_string(row.get('col3', ''))
        col4 = clean_string(row.get('col4', ''))

        # Try to identify what kind of data this is based on patterns
        num_cols = sum(1 for i in range(10) if row.get(f'col{i}'))

        if num_cols == 1:
            # Single column - probably GROUP_ID
            normalized['group_id'] = col0
            normalized['user_id'] = col0

        elif num_cols == 2:
            # Two columns - could be GROUP_ID + DISABLED or USER_ID + FULL_NAME
            if col1.upper() in ('Y', 'N', ''):
                # Likely GROUP_ID + DISABLED
                normalized['group_id'] = col0
                normalized['disabled'] = col1
            else:
                # Likely USER_ID + FULL_NAME or GROUP_NAME + GROUP_ID
                # Determine which is which based on content
                if is_likely_user_id(col0) and not looks_like_full_name(col0):
                    normalized['user_id'] = col0
                    normalized['full_name'] = col1 if col1 else col0
                elif looks_like_full_name(col0) and is_likely_user_id(col1):
                    # Columns swapped
                    normalized['user_id'] = col1
                    normalized['full_name'] = col0
                elif ' ' not in col0 and ' ' in col1:
                    # No space in col0, space in col1 -> col0 is likely ID
                    normalized['user_id'] = col0
                    normalized['full_name'] = col1
                elif ' ' in col0 and ' ' not in col1:
                    # Space in col0, no space in col1 -> col1 is likely ID
                    normalized['user_id'] = col1
                    normalized['full_name'] = col0
                else:
                    # Default order
                    normalized['user_id'] = col0
                    normalized['full_name'] = col1 if col1 else col0

        elif num_cols == 3:
            # Three columns - likely USER_ID, FULL_NAME, SYSTEM_ID
            if is_likely_user_id(col0) and not looks_like_full_name(col0):
                normalized['user_id'] = col0
                normalized['full_name'] = col1 if col1 else col0
                normalized['system_id'] = col2
            elif looks_like_full_name(col0) and is_likely_user_id(col1):
                normalized['full_name'] = col0
                normalized['user_id'] = col1
                normalized['system_id'] = col2
            elif ' ' not in col0 and ' ' in col1:
                normalized['user_id'] = col0
                normalized['full_name'] = col1
                normalized['system_id'] = col2
            elif ' ' in col0 and ' ' not in col1:
                normalized['user_id'] = col1
                normalized['full_name'] = col0
                normalized['system_id'] = col2
            else:
                normalized['user_id'] = col0
                normalized['full_name'] = col1
                normalized['system_id'] = col2

        elif num_cols >= 5:
            # Five columns - likely USER_ID, FULL_NAME, PEOPLE_SYSTEM_ID, Disabled, ALLOW_LOGIN
            if is_likely_user_id(col0) and not looks_like_full_name(col0):
                normalized['user_id'] = col0
                normalized['full_name'] = col1 if col1 else col0
            elif looks_like_full_name(col0) and is_likely_user_id(col1):
                normalized['user_id'] = col1
                normalized['full_name'] = col0
            elif ' ' not in col0 and ' ' in col1:
                normalized['user_id'] = col0
                normalized['full_name'] = col1
            elif ' ' in col0 and ' ' not in col1:
                normalized['user_id'] = col1
                normalized['full_name'] = col0
            else:
                # Default order
                normalized['user_id'] = col0
                normalized['full_name'] = col1

            normalized['system_id'] = col2
            normalized['disabled'] = col3
            normalized['allow_login'] = col4

        else:
            # Unknown format - just map columns directly
            normalized['user_id'] = col0
            normalized['full_name'] = col1 if col1 else col0
            normalized['system_id'] = col2 if col2 else None

        if normalized:
            results.append(normalized)

    return results

def is_likely_user_id(value):
    """
    Determine if a value looks like a USER_ID (vs FULL_NAME).

    USER_IDs are typically:
    - Uppercase or mixed case without spaces
    - May contain underscores or digits
    - No spaces between words
    - Often all uppercase like TEST_USER1, DMALQEDRAH

    FULL_NAMEs are typically:
    - Mixed case with spaces between words
    - Multiple words like "Test User 1" or "Diana Mohammed Al-Qedrah"
    """
    if not value:
        return False

    value = str(value).strip()

    if not value:
        return False

    # CRITICAL: If it has spaces, it's almost certainly a full name, not a user ID
    # Examples: "Test User 1", "Diana Mohammed Al-Qedrah"
    if ' ' in value:
        return False

    # If it's all uppercase, it's very likely a USER_ID
    # Examples: TEST_USER1, DMALQEDRAH, DOCS_SUPERVISORS
    if value.isupper():
        return True

    # If it has underscores with alphanumeric chars, likely a USER_ID
    # Examples: test_user1, Admin_User
    if '_' in value and any(c.isalnum() for c in value):
        return True

    # If it's a single word with mixed case and digits, could be a USER_ID
    # Examples: TestUser1 (though less common)
    if any(c.isdigit() for c in value) and value.replace('_', '').isalnum():
        return True

    # Short single words without spaces might be IDs
    # But be careful - single names like "Diana" could be partial names
    # So only consider very short values or values that look like codes
    if len(value) <= 15 and value.isalnum():
        # Check if it looks like a code (has digits or is all upper)
        if any(c.isdigit() for c in value):
            return True
        if value.isupper():
            return True

    return False

def looks_like_full_name(value):
    """
    Determine if a value looks like a FULL_NAME (vs USER_ID).

    FULL_NAMEs typically:
    - Have spaces between words
    - Use title case or mixed case
    - Contain multiple words
    """
    if not value:
        return False

    value = str(value).strip()

    # If it has spaces, it's likely a full name
    if ' ' in value:
        return True

    # If it has hyphens and mixed case (like Al-Qedrah), could be part of a name
    if '-' in value and not value.isupper():
        return True

    return False

def clean_string(value):
    """
    Clean a string value by removing control characters and trimming.
    """
    if not value:
        return ''

    value = str(value)

    # Remove control characters (ASCII 0-31 and 127)
    import re
    value = re.sub(r'[\x00-\x1f\x7f]', '', value)

    return value.strip()

def parse_groups_buffer(buffer_b64, actual_rows=None, actual_cols=None):
    """
    Parse a buffer specifically for group data.
    Expected columns: GROUP_ID, DISABLED or GROUP_ID alone.
    """
    rows = parse_dms_buffer(buffer_b64, None, actual_rows, actual_cols)

    groups = []
    seen = set()

    for row in rows:
        group_id = clean_string(row.get('col0', ''))
        disabled = clean_string(row.get('col1', '')).upper()

        if group_id and group_id not in seen:
            # Skip disabled groups
            if disabled != 'Y':
                seen.add(group_id)
                groups.append({
                    'group_id': group_id,
                    'group_name': group_id,
                    'disabled': disabled
                })

    return groups

def parse_group_members_buffer(buffer_b64, column_names=None, actual_rows=None, actual_cols=None):
    """
    Parse a buffer for group member data.
    Expected columns: USER_ID, FULL_NAME, PEOPLE_SYSTEM_ID, Disabled, ALLOW_LOGIN

    Args:
        buffer_b64: Base64-encoded buffer
        column_names: Optional list of column names from the query
        actual_rows: Number of rows from resultSetData.actualRows
        actual_cols: Number of columns from resultSetData.columns

    Returns:
        List of member dicts with user_id and full_name
    """
    if column_names is None:
        column_names = ['USER_ID', 'FULL_NAME', 'PEOPLE_SYSTEM_ID', 'Disabled', 'ALLOW_LOGIN']

    rows = parse_dms_buffer(buffer_b64, column_names, actual_rows, actual_cols)

    members = []
    seen = set()

    for row in rows:
        # Get values with fallbacks
        col0 = clean_string(row.get('USER_ID', row.get('col0', '')))
        col1 = clean_string(row.get('FULL_NAME', row.get('col1', '')))
        disabled = clean_string(row.get('Disabled', row.get('col3', ''))).upper()

        # Determine which value is USER_ID and which is FULL_NAME
        # Key insight: USER_IDs don't have spaces, FULL_NAMEs typically do

        user_id = None
        full_name = None

        # Check col0 first (expected to be USER_ID)
        if col0:
            if is_likely_user_id(col0) and not looks_like_full_name(col0):
                user_id = col0
                full_name = col1 if col1 else col0
            elif looks_like_full_name(col0) and is_likely_user_id(col1):
                # Columns appear swapped
                user_id = col1
                full_name = col0
            elif not col1:
                # Only one column has data
                user_id = col0
                full_name = col0
            else:
                # Neither clearly matches - use heuristics
                # Prefer the one without spaces as USER_ID
                if ' ' not in col0 and ' ' in col1:
                    user_id = col0
                    full_name = col1
                elif ' ' in col0 and ' ' not in col1:
                    user_id = col1
                    full_name = col0
                else:
                    # Default: assume original order is correct
                    user_id = col0
                    full_name = col1 if col1 else col0
        elif col1:
            user_id = col1
            full_name = col1

        if user_id and user_id not in seen:
            # Skip disabled users
            if disabled != 'Y':
                seen.add(user_id)
                members.append({
                    'user_id': user_id,
                    'full_name': full_name if full_name else user_id
                })

    return members

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
            # Update: Increased length check to >= 7.
            # This prevents splitting on names containing 5 or 6 digit numbers (e.g. "testing 93993")
            # while still catching valid DocIDs (usually 8 digits, e.g. 19xxxxxx).
            if token.isdigit() and len(token) >= 7:
                if i + 1 < len(tokens):
                    chunk_tokens = []
                    j = i + 1
                    while j < len(tokens):
                        next_token = tokens[j]
                        # Ensure we don't stop on small numbers inside names
                        if next_token.isdigit() and len(next_token) >= 7: break
                        chunk_tokens.append(next_token)
                        j += 1
                    i = j - 1

                    if chunk_tokens:
                        item_type = 'folder'
                        media_type = 'folder'
                        is_folder = False

                        # Prioritize explicit type indicators: 'F' for Folder, 'N' for Node (File)
                        if 'F' in chunk_tokens:
                            is_folder = True
                        elif 'N' in chunk_tokens:
                            is_folder = False
                        else:
                            # Fallback: check against known folder app names if flags are missing
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
                            # 'D' might also appear as a structural token similar to N/F
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