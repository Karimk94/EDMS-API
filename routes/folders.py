from flask import Blueprint, request, jsonify, session
import wsdl_client

folders_bp = Blueprint('folders', __name__)

@folders_bp.route('/api/folders', methods=['GET'])
def api_list_folders():
    if 'user' not in session: return jsonify({"error": "Unauthorized"}), 401
    app_source = request.headers.get('X-App-Source', 'unknown')
    scope = request.args.get('scope')
    media_type = request.args.get('media_type') or None
    search_term = request.args.get('search') or None
    parent_id = request.args.get('parent_id')
    if parent_id in ['null', 'undefined', '']: parent_id = None
    dst = wsdl_client.dms_system_login()
    if not dst: return jsonify({"error": "Failed to authenticate with DMS"}), 500
    try:
        contents = wsdl_client.list_folder_contents(dst, parent_id, app_source, scope=scope, media_type=media_type, search_term=search_term)
        return jsonify({"contents": contents}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@folders_bp.route('/api/folders', methods=['POST'])
def api_create_folder():
    if 'user' not in session: return jsonify({"error": "Unauthorized"}), 401
    data = request.get_json()
    folder_name = data.get('name')
    description = data.get('description', '')
    parent_id = data.get('parent_id')
    if not parent_id or str(parent_id).strip() == "": parent_id = None
    if not folder_name: return jsonify({"error": "Folder name is required"}), 400
    username = session['user'].get('username')
    dst = wsdl_client.dms_system_login()
    if not dst: return jsonify({"error": "Failed to authenticate"}), 500
    try:
        new_folder_id = wsdl_client.create_dms_folder(dst=dst, folder_name=folder_name, description=description, parent_id=parent_id, user_id=username)
        if new_folder_id:
            return jsonify({"message": "Folder created", "folder_id": new_folder_id, "name": folder_name}), 201
        else: return jsonify({"error": "Failed to create folder"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@folders_bp.route('/api/folders/<folder_id>', methods=['PUT'])
def api_rename_folder(folder_id):
    if 'user' not in session: return jsonify({"error": "Unauthorized"}), 401
    data = request.get_json()
    new_name = data.get('name')
    if not new_name: return jsonify({"error": "New name required"}), 400
    dst = wsdl_client.dms_system_login()
    if not dst: return jsonify({"error": "Failed to authenticate"}), 500
    success = wsdl_client.rename_document(dst, folder_id, new_name)
    if success: return jsonify({"message": "Renamed", "id": folder_id}), 200
    else: return jsonify({"error": "Failed to rename"}), 500

@folders_bp.route('/api/folders/<folder_id>', methods=['DELETE'])
def api_delete_folder(folder_id):
    if 'user' not in session: return jsonify({"error": "Unauthorized"}), 401
    force_delete = request.args.get('force', 'false').lower() == 'true'
    dst = wsdl_client.dms_system_login()
    if not dst: return jsonify({"error": "Failed to authenticate"}), 500
    success, message = wsdl_client.delete_document(dst, folder_id, force=force_delete)
    if success: return jsonify({"message": "Deleted", "id": folder_id}), 200
    else: return jsonify({"error": message}), 500