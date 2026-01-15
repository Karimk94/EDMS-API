import logging
import re
import json
import api_client
import db_connector
import wsdl_client
from utils.common import clean_repeated_words
from fastapi import HTTPException, Request

async def process_document(doc, dms_session_token):
    docnumber = doc['docnumber']
    logging.info(f"Starting processing for document: {docnumber}")
    original_abstract = doc.get('abstract') or ''
    base_abstract = re.split(r'\s*\n*\s*Caption:', original_abstract, 1, flags=re.IGNORECASE)[0].strip()
    ai_abstract_parts = {}
    results = {
        "docnumber": docnumber,
        "new_abstract": original_abstract,
        "o_detected": doc.get('o_detected', 0),
        "ocr": doc.get('ocr', 0),
        "face": doc.get('face', 0),
        "transcript": '',
        "status": 1,
        "error": '',
        "attempts": doc.get('attempts', 0) + 1
    }
    try:
        # get_image_by_docnumber is still sync in wsdl_client because it doesn't hit DB
        media_bytes, filename = wsdl_client.get_image_by_docnumber(dms_session_token, docnumber)
        if not media_bytes:
            raise Exception(f"Failed to retrieve media for docnumber {docnumber} from WSDL service.")

        # This one IS async in db_connector now
        _, media_type, _ = await db_connector.get_media_info_from_dms(dms_session_token, docnumber)
        logging.info(f"Media for {docnumber} ({filename}) fetched successfully. Type: {media_type}")

        if media_type == 'video':
            video_summary = api_client.summarize_video(media_bytes, filename)
            caption_parts = []
            keywords_to_insert = []
            if video_summary.get('objects'):
                caption_parts.extend(video_summary['objects'])
                results['o_detected'] = 1
                for obj in video_summary['objects']:
                    arabic_translation = api_client.translate_text(obj)
                    keywords_to_insert.append({'english': obj, 'arabic': arabic_translation})
            if video_summary.get('faces'):
                recognized_faces = api_client.recognize_faces_from_list(video_summary['faces'])
                unique_known_faces = {f.get('name').replace('_', ' ').title() for f in recognized_faces if
                                      f.get('name') and f.get('name') != 'Unknown'}
                if unique_known_faces:
                    ai_abstract_parts['VIPS'] = ", ".join(sorted(list(unique_known_faces)))
                results['face'] = 1
            if video_summary.get('transcript'):
                tokenized_json_str = api_client.tokenize_transcript(video_summary['transcript'])
                english_tags = []
                try:
                    tokenized_data = json.loads(tokenized_json_str)
                    english_tags = tokenized_data.get('english_tags', [])
                except json.JSONDecodeError:
                    english_match = re.search(r'"english_tags"\s*:\s*\[([^\]]+)\]', tokenized_json_str, re.IGNORECASE)
                    if english_match:
                        raw_english = english_match.group(1)
                        english_tags = [tag.strip() for tag in raw_english.replace('"', '').split(',') if tag.strip()]
                if english_tags:
                    caption_parts.extend(english_tags)
                    for tag in english_tags:
                        arabic_translation = api_client.translate_text(tag)
                        keywords_to_insert.append({'english': tag, 'arabic': arabic_translation})
            if video_summary.get('ocr_texts'):
                results['ocr'] = 1
                for ocr_text in video_summary['ocr_texts']:
                    if not ocr_text: continue
                    tokenized_json_str = api_client.tokenize_transcript(ocr_text)
                    english_tags = []
                    try:
                        tokenized_data = json.loads(tokenized_json_str)
                        english_tags = tokenized_data.get('english_tags', [])
                    except json.JSONDecodeError:
                        english_match = re.search(r'"english_tags"\s*:\s*\[([^\]]+)\]', tokenized_json_str,
                                                  re.IGNORECASE)
                        if english_match:
                            raw_english = english_match.group(1)
                            english_tags = [tag.strip() for tag in raw_english.replace('"', '').split(',') if
                                            tag.strip()]
                    if english_tags:
                        caption_parts.extend(english_tags)
                        for tag in english_tags:
                            arabic_translation = api_client.translate_text(tag)
                            keywords_to_insert.append({'english': tag, 'arabic': arabic_translation})
            else:
                results['ocr'] = 1
            if keywords_to_insert:
                # ASYNC DB CALL
                await db_connector.insert_keywords_and_tags(docnumber, keywords_to_insert)
            if caption_parts:
                ai_abstract_parts['CAPTION'] = ", ".join(sorted(list(set(caption_parts))))

        elif media_type == 'pdf':
            keywords_to_insert = []
            caption_parts = []
            ocr_text = api_client.get_ocr_text_from_pdf(media_bytes, filename)
            if ocr_text:
                results['ocr'] = 1
                tokenized_json_str = api_client.tokenize_transcript(ocr_text)
                english_tags = []
                try:
                    tokenized_data = json.loads(tokenized_json_str)
                    english_tags = tokenized_data.get('english_tags', [])
                except json.JSONDecodeError:
                    english_match = re.search(r'"english_tags"\s*:\s*\[([^\]]+)\]', tokenized_json_str, re.IGNORECASE)
                    if english_match:
                        raw_english = english_match.group(1)
                        english_tags = [tag.strip() for tag in raw_english.replace('"', '').split(',') if tag.strip()]
                if english_tags:
                    caption_parts.extend(english_tags)
                    for tag in english_tags:
                        arabic_translation = api_client.translate_text(tag)
                        keywords_to_insert.append({'english': tag, 'arabic': arabic_translation})
            else:
                results['ocr'] = 1
            if keywords_to_insert:
                # ASYNC DB CALL
                await db_connector.insert_keywords_and_tags(docnumber, keywords_to_insert)
            results['o_detected'] = 1
            results['face'] = 1
            if caption_parts:
                ai_abstract_parts['CAPTION'] = ", ".join(sorted(list(set(caption_parts))))

        else:
            keywords_to_insert = []
            result = api_client.get_captions(media_bytes, filename)
            if result:
                raw_caption = result.get('caption', '')
                cleaned_caption = clean_repeated_words(raw_caption)
                ai_abstract_parts['CAPTION'] = cleaned_caption
                results['o_detected'] = 1
                tags = result.get('tags', [])
                for tag in tags:
                    arabic_translation = api_client.translate_text(tag)
                    keywords_to_insert.append({'english': tag, 'arabic': arabic_translation})
            else:
                results['o_detected'] = 0
            ocr_text = api_client.get_ocr_text(media_bytes, filename)
            if ocr_text:
                results['ocr'] = 1
                ai_abstract_parts['OCR'] = ocr_text
            else:
                results['ocr'] = 1
            recognized_faces = api_client.recognize_faces(media_bytes, filename)
            if recognized_faces is not None:
                results['face'] = 1
                unique_known_faces = {f.get('name').replace('_', ' ').title() for f in recognized_faces if
                                      f.get('name') and f.get('name') != 'Unknown'}
                if unique_known_faces:
                    ai_abstract_parts['VIPS'] = ", ".join(sorted(list(unique_known_faces)))
            else:
                results['face'] = 0
            if keywords_to_insert:
                # ASYNC DB CALL
                await db_connector.insert_keywords_and_tags(docnumber, keywords_to_insert)

        final_abstract_parts = [base_abstract]
        if ai_abstract_parts.get('CAPTION'): final_abstract_parts.append(f"Caption: {ai_abstract_parts['CAPTION']} ")
        if ai_abstract_parts.get('OCR'): final_abstract_parts.append(f"OCR: {ai_abstract_parts['OCR']} ")
        if ai_abstract_parts.get('VIPS'): final_abstract_parts.append(f"VIPs: {ai_abstract_parts['VIPS']}")
        if len(ai_abstract_parts) > 0:
            results['new_abstract'] = "\n\n".join(filter(None, final_abstract_parts)).strip()
        else:
            results['new_abstract'] = base_abstract

        if media_type == 'pdf':
            results['status'] = 3 if results['ocr'] == 1 else results['status']
        else:
            if results['o_detected'] == 1 and results['ocr'] == 1 and results['face'] == 1:
                results['status'] = 3
            else:
                if results['o_detected'] == 0 or results['ocr'] == 0 or results['face'] == 0:
                    logging.warning(f"One or more AI steps failed for {docnumber}.")

    except Exception as e:
        logging.error(f"Error processing document {docnumber}: {e}", exc_info=True)
        results['status'] = 2
        results['error'] = str(e)[:2000]

    if results['status'] != 3 and results['status'] != 2:
        if results['attempts'] >= 3:
            results['status'] = 2
            results['error'] = results['error'] or "Max processing attempts reached without full success."
        else:
            results['status'] = 1
    return results

def get_security_level_int(role: str) -> int:
    role = str(role).lower()
    if role in ['admin', 'administrator', '9']:
        return 9
    if role in ['editor', 'manager', '5']:
        return 5
    # Default/Reader
    return 0

def get_session_token(request: Request):
    user = request.session.get('user')
    if not user or 'token' not in user:
        token = db_connector.dms_system_login()
        if token: return token
        raise HTTPException(status_code=401, detail="Session required")
    return user['token']

def get_current_username(request: Request):
    user = request.session.get('user')
    return user['username'] if user else None

def determine_security_from_groups(user_groups):
    """
    Determines security level based on DMS group membership.
    Returns: 9 (Admin), 5 (Editor), or 0 (Viewer)
    """
    if not user_groups:
        return 0

    group_ids = [g.get('group_id', '').upper() for g in user_groups]

    # Define your organization's security groups
    ADMIN_GROUPS = {
        'DOCS_ADMINS',
        'DOCS_SUPERVISORS',
        'ADMINISTRATOR',
        'ADMIN',
        'SYSADMIN',
        # Add more admin groups here
    }

    EDITOR_GROUPS = {
        'DOCS_EDITORS',
        'DOCS_USERS',
        'TIBCO_GROUP',
        'CONTRIBUTORS',
        'POWER_USERS'
    }

    # Check for admin privileges
    for group_id in group_ids:
        if group_id in ADMIN_GROUPS:
            return 9

    # Check for editor privileges
    for group_id in group_ids:
        if group_id in EDITOR_GROUPS:
            return 5

    # Default to viewer
    return 0