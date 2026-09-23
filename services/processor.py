import logging
import re
import json
import api_client
import db_connector
import wsdl_client
from utils.common import clean_repeated_words, get_session_token
from fastapi import HTTPException, Request
from starlette.concurrency import run_in_threadpool
import asyncio

# Maximum character limit for the ABSTRACT column in Oracle
MAX_ABSTRACT_LENGTH = 2000
# Target length for caption to leave room for other content
MAX_CAPTION_LENGTH = 500
# Target length for OCR text
MAX_OCR_LENGTH = 1000


# --- Helper functions to reduce duplication ---

def _extract_english_tags(json_str: str) -> list:
    """Extracts english_tags from a JSON string with regex fallback."""
    try:
        data = json.loads(json_str)
        return data.get('english_tags', [])
    except json.JSONDecodeError:
        match = re.search(r'"english_tags"\s*:\s*\[([^\]]+)\]', json_str, re.IGNORECASE)
        if match:
            raw = match.group(1)
            return [tag.strip() for tag in raw.replace('"', '').split(',') if tag.strip()]
        return []

async def _translate_and_collect(tags: list) -> list:
    """Translates tags to Arabic and returns keyword dicts."""
    keywords = []
    for tag in tags:
        arabic = await run_in_threadpool(api_client.translate_text, tag)
        keywords.append({'english': tag, 'arabic': arabic})
    return keywords


def _truncate_text_smart(text: str, max_length: int, suffix: str = "...") -> str:
    """
    Intelligently truncates text to fit within max_length.
    Tries to break at sentence or word boundaries.
    """
    if not text:
        return text
    # Guard against non-positive max_length — return a safe minimal slice
    if max_length <= len(suffix):
        return text[:max(0, max_length)].strip()
    if len(text) <= max_length:
        return text

    # Try to break at sentence boundary (period, question mark, exclamation)
    truncated = text[:max_length - len(suffix)]

    # Look for sentence boundary within last 100 characters
    for i in range(len(truncated) - 1, max(0, len(truncated) - 100), -1):
        if truncated[i] in '.!?':
            return truncated[:i + 1] + suffix

    # Fall back to word boundary
    last_space = truncated.rfind(' ')
    if last_space > max_length // 2:
        return truncated[:last_space] + suffix

    return truncated + suffix


def _ensure_abstract_fits(base_abstract: str, caption: str, ocr: str, vips: str) -> tuple:
    """
    Ensures the final abstract fits within MAX_ABSTRACT_LENGTH.
    Returns tuple of (base_abstract, caption, ocr, vips) with potentially truncated values.

    Priority (highest to lowest):
      1. caption  — core AI result, must always be present if it exists
      2. vips     — named persons, important for search
      3. ocr      — extracted text, trimmed first as it tends to be longest
      4. base_abstract — user description, trimmed last if everything else is tight
    """
    # Minimum guaranteed space for caption so the AI result is never silently lost
    MIN_CAPTION_SPACE = 200

    # Calculate current total length
    parts = [base_abstract]
    if caption:
        parts.append(f"{caption}")
    if ocr:
        parts.append(f"OCR: {ocr}")
    if vips:
        parts.append(f"VIPs: {vips}")

    current_length = len("\n\n".join(filter(None, parts)))

    if current_length <= MAX_ABSTRACT_LENGTH:
        return base_abstract, caption or "", ocr or "", vips or ""

    logging.warning(f"Abstract too long ({current_length} chars), truncating...")

    # --- Step 1: Trim OCR first (usually the longest chunk) ---
    if ocr and len(ocr) > MAX_OCR_LENGTH:
        ocr = _truncate_text_smart(ocr, MAX_OCR_LENGTH)

    # --- Step 2: Recalculate ---
    parts = [base_abstract]
    if caption:
        parts.append(f"{caption}")
    if ocr:
        parts.append(f"OCR: {ocr}")
    if vips:
        parts.append(f"VIPs: {vips}")
    current_length = len("\n\n".join(filter(None, parts)))

    if current_length <= MAX_ABSTRACT_LENGTH:
        return base_abstract, caption or "", ocr or "", vips or ""

    # --- Step 3: Trim OCR further if still over ---
    ai_overhead = 0  # chars used by labels ("Caption: ", "OCR: ", "VIPs: ", separators)
    if caption:
        ai_overhead += len("Caption: ") + 4  # +4 for "\n\n" separator
    if ocr:
        ai_overhead += len("OCR: ") + 4
    if vips:
        ai_overhead += len("VIPs: ") + 4

    base_len = len(base_abstract)
    # Space available for all AI text (caption + ocr + vips content, excluding labels)
    available_for_ai_text = MAX_ABSTRACT_LENGTH - base_len - ai_overhead

    # Guarantee caption always gets at least MIN_CAPTION_SPACE chars.
    # If base_abstract is so long there's no room, trim base_abstract first.
    if available_for_ai_text < MIN_CAPTION_SPACE and base_len > 0:
        # Trim base_abstract to free up space for AI content
        max_base = max(0, MAX_ABSTRACT_LENGTH - MIN_CAPTION_SPACE - ai_overhead)
        base_abstract = _truncate_text_smart(base_abstract, max_base)
        base_len = len(base_abstract)
        available_for_ai_text = MAX_ABSTRACT_LENGTH - base_len - ai_overhead

    # Distribute remaining space: caption gets priority, then vips, then ocr
    vips_budget = min(len(vips) if vips else 0, 200)
    caption_budget = min(available_for_ai_text - (len(ocr) if ocr else 0) - vips_budget,
                         MAX_CAPTION_LENGTH)
    caption_budget = max(caption_budget, MIN_CAPTION_SPACE)

    if caption and len(caption) > caption_budget:
        caption = _truncate_text_smart(caption, caption_budget)

    ocr_budget = available_for_ai_text - len(caption or "") - vips_budget
    if ocr and ocr_budget > 50:
        ocr = _truncate_text_smart(ocr, max(50, ocr_budget))
    elif ocr and ocr_budget <= 50:
        ocr = ""

    return base_abstract, caption or "", ocr or "", vips or ""


def _build_final_abstract(base_abstract: str, ai_abstract_parts: dict) -> str:
    """
    Constructs the final abstract string from base description and AI components,
    applying intelligent truncation to guarantee the text fits within Oracle column limits.
    """
    caption = (ai_abstract_parts.get('CAPTION') or '').strip()
    if caption == "No caption generated.":
        caption = ""
    if ocr == "The image contains no text.":
        ocr = ""
    ocr = (ai_abstract_parts.get('OCR') or '').strip()
    vips = (ai_abstract_parts.get('VIPS') or '').strip()

    if not caption and not ocr and not vips:
        return base_abstract

    base_abstract, caption, ocr, vips = _ensure_abstract_fits(base_abstract, caption, ocr, vips)

    parts = [base_abstract]
    if caption:
        parts.append(f"{caption}")
    if ocr:
        parts.append(f"OCR: {ocr}")
    if vips:
        parts.append(f"VIPs: {vips}")

    final_abstract = "\n\n".join(filter(None, parts)).strip()
    if len(final_abstract) > MAX_ABSTRACT_LENGTH:
        final_abstract = final_abstract[:MAX_ABSTRACT_LENGTH - 3] + "..."
    return final_abstract


# --- Async wrappers for blocking api_client calls ---
# api_client uses synchronous `requests` which blocks the event loop.
# These wrappers offload them to a thread pool.

async def async_summarize_video(video_data, filename):
    return await run_in_threadpool(api_client.summarize_video, video_data, filename)

async def async_get_captions(image_data, filename):
    return await run_in_threadpool(api_client.get_captions, image_data, filename)

async def async_get_ocr_text(image_data, filename):
    return await run_in_threadpool(api_client.get_ocr_text, image_data, filename)

async def async_get_ocr_text_from_pdf(pdf_data, filename):
    return await run_in_threadpool(api_client.get_ocr_text_from_pdf, pdf_data, filename)

async def async_recognize_faces(image_data, filename):
    return await run_in_threadpool(api_client.recognize_faces, image_data, filename)

async def async_recognize_faces_from_list(faces_list):
    return await run_in_threadpool(api_client.recognize_faces_from_list, faces_list)

async def async_translate_text(text):
    return await run_in_threadpool(api_client.translate_text, text)

async def async_tokenize_transcript(transcript):
    return await run_in_threadpool(api_client.tokenize_transcript, transcript)

async def async_get_image_by_docnumber(dms_session_token, docnumber):
    return await run_in_threadpool(wsdl_client.get_image_by_docnumber, dms_session_token, docnumber)

async def process_document(doc, dms_session_token):
    docnumber = doc['docnumber']
    original_abstract = doc.get('abstract') or ''
    # Strip any previous AI labels (Caption, OCR, VIPs) to preserve clean base description
    base_abstract = re.split(r'\s*\n*\s*(?:Caption|OCR|VIPs)\s*:', original_abstract, 1, flags=re.IGNORECASE)[0].strip()
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
        # get_image_by_docnumber is sync in wsdl_client, so offload it to the threadpool.
        media_bytes, filename = await async_get_image_by_docnumber(dms_session_token, docnumber)
        if not media_bytes:
            raise Exception(f"Failed to retrieve media for docnumber {docnumber} from WSDL service.")

        # This one IS async in db_connector now
        _, media_type, _ = await db_connector.get_media_info_from_dms(dms_session_token, docnumber)

        if media_type == 'video':
            caption_parts = []
            keywords_to_insert = []
            try:
                video_summary = await async_summarize_video(media_bytes, filename)
            except Exception as vid_err:
                logging.warning(f"Video summarization failed for doc {docnumber}: {vid_err}")
                video_summary = {}

            if video_summary.get('objects'):
                caption_parts.extend(video_summary['objects'])
                results['o_detected'] = 1
                try:
                    keywords_to_insert.extend(await _translate_and_collect(video_summary['objects']))
                except Exception as tr_err:
                    logging.warning(f"Translation failed for video objects in doc {docnumber}: {tr_err}")
            if video_summary.get('faces'):
                try:
                    recognized_faces = await async_recognize_faces_from_list(video_summary['faces'])
                    unique_known_faces = {f.get('name').replace('_', ' ').title() for f in recognized_faces if
                                          f.get('name') and f.get('name') != 'Unknown'}
                    if unique_known_faces:
                        ai_abstract_parts['VIPS'] = ", ".join(sorted(list(unique_known_faces)))
                    results['face'] = 1
                except Exception as fc_err:
                    logging.warning(f"Face recognition from video failed for doc {docnumber}: {fc_err}")
            if video_summary.get('transcript'):
                try:
                    tokenized_json_str = await async_tokenize_transcript(video_summary['transcript'])
                    english_tags = _extract_english_tags(tokenized_json_str)
                    if english_tags:
                        caption_parts.extend(english_tags)
                        keywords_to_insert.extend(await _translate_and_collect(english_tags))
                except Exception as tok_err:
                    logging.warning(f"Tokenize transcript failed for doc {docnumber}: {tok_err}")
            if video_summary.get('ocr_texts'):
                results['ocr'] = 1
                for ocr_text in video_summary['ocr_texts']:
                    if not ocr_text: continue
                    try:
                        tokenized_json_str = await async_tokenize_transcript(ocr_text)
                        english_tags = _extract_english_tags(tokenized_json_str)
                        if english_tags:
                            caption_parts.extend(english_tags)
                            keywords_to_insert.extend(await _translate_and_collect(english_tags))
                    except Exception:
                        pass
            else:
                results['ocr'] = 1
            if keywords_to_insert:
                try:
                    await db_connector.insert_keywords_and_tags(docnumber, keywords_to_insert)
                except Exception as tag_err:
                    logging.warning(f"Failed to insert keywords/tags for video doc {docnumber}: {tag_err}")
            if caption_parts:
                ai_abstract_parts['CAPTION'] = ", ".join(sorted(list(set(caption_parts))))

        elif media_type == 'pdf':
            keywords_to_insert = []
            caption_parts = []
            try:
                ocr_text = await async_get_ocr_text_from_pdf(media_bytes, filename)
            except Exception as pdf_ocr_err:
                logging.warning(f"PDF OCR failed for doc {docnumber}: {pdf_ocr_err}")
                ocr_text = ""

            if ocr_text:
                results['ocr'] = 1
                ai_abstract_parts['OCR'] = ocr_text.strip()
                try:
                    tokenized_json_str = await async_tokenize_transcript(ocr_text)
                    english_tags = _extract_english_tags(tokenized_json_str)
                    if english_tags:
                        caption_parts.extend(english_tags)
                        keywords_to_insert.extend(await _translate_and_collect(english_tags))
                except Exception as tok_err:
                    logging.warning(f"Tokenizing PDF text failed for doc {docnumber}: {tok_err}")
            else:
                results['ocr'] = 1
            if keywords_to_insert:
                try:
                    await db_connector.insert_keywords_and_tags(docnumber, keywords_to_insert)
                except Exception as tag_err:
                    logging.warning(f"Failed to insert keywords/tags for PDF doc {docnumber}: {tag_err}")
            results['o_detected'] = 1
            results['face'] = 1
            if caption_parts:
                ai_abstract_parts['CAPTION'] = ", ".join(sorted(list(set(caption_parts))))

        else:
            keywords_to_insert = []
            # 1. Caption generation (primary)
            try:
                result = await async_get_captions(media_bytes, filename)
                if result:
                    raw_caption = (
                        result.get('caption')
                        or result.get('description')
                        or result.get('text')
                        or result.get('caption_text')
                        or ''
                    )
                    cleaned_caption = clean_repeated_words(raw_caption)
                    if cleaned_caption and cleaned_caption.strip():
                        ai_abstract_parts['CAPTION'] = cleaned_caption.strip()
                        results['o_detected'] = 1
                    else:
                        results['o_detected'] = 0

                    tags = result.get('tags', [])
                    if isinstance(tags, str):
                        tags = [t.strip() for t in tags.split(',') if t.strip()]
                    elif not isinstance(tags, list):
                        tags = []

                    for tag in tags:
                        arabic_translation = ""
                        try:
                            arabic_translation = await async_translate_text(tag)
                        except Exception as trans_err:
                            logging.warning(f"Tag translation failed for '{tag}' in doc {docnumber}: {trans_err}")
                        keywords_to_insert.append({'english': tag, 'arabic': arabic_translation or tag})
                else:
                    results['o_detected'] = 0
            except Exception as cap_err:
                logging.error(f"Caption API call failed for doc {docnumber}: {cap_err}")
                results['o_detected'] = 0

            # 2. OCR (secondary)
            try:
                ocr_text = await async_get_ocr_text(media_bytes, filename)
                if ocr_text:
                    results['ocr'] = 1
                    ai_abstract_parts['OCR'] = ocr_text.strip()
                else:
                    results['ocr'] = 1
            except Exception as ocr_err:
                logging.warning(f"OCR failed for doc {docnumber}: {ocr_err}")
                results['ocr'] = 0

            # 3. Face recognition (secondary)
            try:
                recognized_faces = await async_recognize_faces(media_bytes, filename)
                if recognized_faces is not None:
                    results['face'] = 1
                    unique_known_faces = {f.get('name').replace('_', ' ').title() for f in recognized_faces if
                                          f.get('name') and f.get('name') != 'Unknown'}
                    if unique_known_faces:
                        ai_abstract_parts['VIPS'] = ", ".join(sorted(list(unique_known_faces)))
                else:
                    results['face'] = 0
            except Exception as face_err:
                logging.warning(f"Face recognition failed for doc {docnumber}: {face_err}")
                results['face'] = 0

            # 4. Keyword / Tag DB insert
            if keywords_to_insert:
                try:
                    await db_connector.insert_keywords_and_tags(docnumber, keywords_to_insert)
                except Exception as tag_db_err:
                    logging.warning(f"Failed to insert keywords/tags for doc {docnumber}: {tag_db_err}")

        # Assemble new abstract from whatever parts were generated
        results['new_abstract'] = _build_final_abstract(base_abstract, ai_abstract_parts)

        # Status determination:
        # If at least one primary AI output succeeded (caption or OCR), mark as completed (3)
        if media_type == 'pdf':
            results['status'] = 3 if results['ocr'] == 1 else 1
        elif media_type == 'video':
            results['status'] = 3 if (results['o_detected'] == 1 or results['ocr'] == 1) else 1
        else:
            if results['o_detected'] == 1 or results['ocr'] == 1:
                results['status'] = 3
            elif results['face'] == 1:
                results['status'] = 3
            else:
                results['status'] = 1

        logging.info(
            f"[processor] Document {docnumber} processed. Status={results['status']}, "
            f"has_caption={bool(ai_abstract_parts.get('CAPTION'))}, "
            f"abstract_len={len(results['new_abstract'])}, "
            f"abstract_preview={repr(results['new_abstract'][:120])}"
        )

        # Best-effort synchronization with OpenText eDocs DM Profile (WSDL UpdateObject)
        if dms_session_token and results.get('new_abstract'):
            try:
                await asyncio.to_thread(
                    wsdl_client.update_document_profile_abstract,
                    dms_session_token,
                    docnumber,
                    results['new_abstract']
                )
            except Exception as wsdl_err:
                logging.warning(f"WSDL abstract sync failed for doc {docnumber}: {wsdl_err}")

    except Exception as e:
        logging.error(f"Error processing document {docnumber}: {e}", exc_info=True)
        results['status'] = 2
        results['error'] = str(e)[:2000]
        # Preserve whatever abstract was generated before the error occurred
        results['new_abstract'] = _build_final_abstract(base_abstract, ai_abstract_parts)

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


# get_session_token is now imported from utils.common


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