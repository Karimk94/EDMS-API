from flask import Blueprint, request, jsonify, session
import db_connector
import api_client
from utils.common import editor_required

tags_bp = Blueprint('tags', __name__)

@tags_bp.route('/api/add_person', methods=['POST'])
def api_add_person():
    data = request.get_json()
    name = data.get('name')
    lang = data.get('lang', 'en')
    if not name or len(name.strip()) < 2:
        return jsonify({'error': 'Invalid data.'}), 400
    try:
        is_arabic = (lang == 'ar') or (not name.strip().isascii())
        if is_arabic:
            name_arabic = name.strip()
            name_english = api_client.translate_text(name_arabic)
            if not name_english: return jsonify({'error': 'Failed to translate.'}), 500
        else:
            name_english = name.strip()
            name_arabic = api_client.translate_text(name_english) or None
    except Exception as e:
        return jsonify({'error': f'Translation error: {e}'}), 500
    success, message = db_connector.add_person_to_lkp(name_english, name_arabic)
    if success: return jsonify({'message': message})
    else: return jsonify({'error': message}), 500

@tags_bp.route('/api/persons')
def api_get_persons():
    page = request.args.get('page', 1, type=int)
    search = request.args.get('search', '', type=str)
    lang = request.args.get('lang', 'en', type=str)
    persons, total_rows = db_connector.fetch_lkp_persons(page=page, search=search, lang=lang)
    return jsonify({'options': persons, 'hasMore': (page * 20) < total_rows})

@tags_bp.route('/api/tags')
def api_get_tags():
    lang = request.args.get('lang', 'en', type=str)
    user = session.get('user')
    security_level = user.get('security_level', 'Viewer') if user else 'Viewer'
    app_source = request.headers.get('X-App-Source', 'unknown')
    tags = db_connector.fetch_all_tags(lang=lang, security_level=security_level, app_source=app_source)
    return jsonify(tags)

@tags_bp.route('/api/tags/<int:doc_id>')
def api_get_tags_for_document(doc_id):
    lang = request.args.get('lang', 'en', type=str)
    user = session.get('user')
    security_level = user.get('security_level', 'Viewer') if user else 'Viewer'
    tags = db_connector.fetch_tags_for_document(doc_id, lang=lang, security_level=security_level)
    return jsonify({"tags": tags})

@tags_bp.route('/api/tags/shortlist', methods=['POST'])
@editor_required
def api_toggle_shortlist():
    data = request.get_json()
    tag = data.get('tag')
    if not tag: return jsonify({'error': 'Tag is required'}), 400
    success, result = db_connector.toggle_tag_shortlist(tag)
    if success: return jsonify(result)
    else: return jsonify({'error': result}), 400

@tags_bp.route('/api/processing_status', methods=['POST'])
def api_processing_status():
    data = request.get_json()
    docnumbers = data.get('docnumbers')
    if not docnumbers: return jsonify({"status": "error", "message": "Invalid data."}), 400
    still_processing = db_connector.check_processing_status(docnumbers)
    return jsonify({"processing": still_processing})

@tags_bp.route('/api/tags/<int:doc_id>', methods=['POST'])
def api_add_tag(doc_id):
    data = request.get_json()
    tag = data.get('tag')
    if not tag or len(tag.strip()) < 2: return jsonify({'error': 'Invalid tag.'}), 400
    try:
        is_arabic = not tag.isascii()
        if is_arabic:
            arabic_keyword = tag
            english_keyword = api_client.translate_text(tag)
        else:
            english_keyword = tag
            arabic_keyword = api_client.translate_text(tag)
        if not english_keyword or not arabic_keyword:
            return jsonify({'error': 'Translation failed.'}), 500
        db_connector.insert_keywords_and_tags(doc_id, [{'english': english_keyword, 'arabic': arabic_keyword}])
        return jsonify({'message': 'Tag added successfully.'}), 201
    except Exception as e:
        return jsonify({'error': f'Server error: {e}'}), 500

@tags_bp.route('/api/tags/<int:doc_id>/<tag>', methods=['DELETE'])
def api_delete_tag(doc_id, tag):
    success, message = db_connector.delete_tag_from_document(doc_id, tag)
    if success: return jsonify({'message': message})
    else: return jsonify({'error': message}), 404