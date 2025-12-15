from flask import Blueprint, request, jsonify, session
import math
import db_connector

favorites_bp = Blueprint('favorites', __name__)

@favorites_bp.route('/api/favorites/<int:doc_id>', methods=['POST'])
def add_favorite_route(doc_id):
    if 'user' not in session: return jsonify({"error": "Unauthorized"}), 401
    user_id = session['user'].get('username')
    success, message = db_connector.add_favorite(user_id, doc_id)
    if success: return jsonify({"message": message}), 201
    else: return jsonify({"error": message}), 500

@favorites_bp.route('/api/favorites/<int:doc_id>', methods=['DELETE'])
def remove_favorite_route(doc_id):
    if 'user' not in session: return jsonify({"error": "Unauthorized"}), 401
    user_id = session['user'].get('username')
    success, message = db_connector.remove_favorite(user_id, doc_id)
    if success: return jsonify({"message": message}), 200
    else: return jsonify({"error": message}), 500

@favorites_bp.route('/api/favorites', methods=['GET'])
def get_favorites_route():
    if 'user' not in session: return jsonify({"error": "Unauthorized"}), 401
    user_id = session['user'].get('username')
    app_source = request.headers.get('X-App-Source', 'unknown')
    page = request.args.get('page', 1, type=int)
    page_size = request.args.get('pageSize', 20, type=int)
    documents, total_rows = db_connector.get_favorites(user_id, page, page_size, app_source=app_source)
    total_pages = math.ceil(total_rows / page_size) if total_rows > 0 else 1
    return jsonify({"documents": documents, "page": page, "total_pages": total_pages, "total_documents": total_rows})