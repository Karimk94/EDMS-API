import zlib
import re
import logging

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

def parse_user_result_buffer(buffer):
    items = []
    try:
        if len(buffer) > 8 and buffer[8:10] == b'\x78\x9c':
            try:
                buffer = zlib.decompress(buffer[8:])
            except Exception:
                pass

        try:
            raw_text = buffer.decode('utf-16-le')
        except:
            raw_text = buffer.decode('utf-8', errors='ignore')

        tokens = [t for t in re.split(r'[\x00\t\r\n]', raw_text) if t.strip()]

        if len(tokens) < 2:
            clean_text = "".join([c if c.isprintable() else '|' for c in raw_text])
            tokens = [t for t in clean_text.split('|') if t.strip()]

        i = 0
        while i < len(tokens):
            if i + 1 < len(tokens):
                u_id = tokens[i]
                f_name = tokens[i + 1]
                if len(u_id) < 50:
                    items.append({'user_id': u_id, 'full_name': f_name})
                i += 3
            else:
                break
    except Exception as e:
        logging.error(f"Error parsing user buffer: {e}")
    return items