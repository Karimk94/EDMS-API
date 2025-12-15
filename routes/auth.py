from flask import Blueprint, request, jsonify, session
import db_connector
import wsdl_client

auth_bp = Blueprint('auth', __name__)

@auth_bp.route('/api/auth/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')
    if not username or not password:
        return jsonify({"error": "Username and password are required"}), 400
    dst = wsdl_client.dms_user_login(username, password)
    if dst:
        user_details = db_connector.get_user_details(username)
        if user_details is None or 'security_level' not in user_details:
            return jsonify({"error": "User not authorized for this application"}), 401
        session['user'] = user_details
        session.permanent = True
        return jsonify({"message": "Login successful", "user": user_details}), 200
    else:
        return jsonify({"error": "Invalid DMS credentials"}), 401

@auth_bp.route('/api/auth/logout', methods=['POST'])
def logout():
    session.pop('user', None)
    return jsonify({"message": "Logout successful"}), 200

@auth_bp.route('/api/auth/user', methods=['GET'])
def get_user():
    user_session = session.get('user')
    if user_session and 'username' in user_session:
        user_details = db_connector.get_user_details(user_session['username'])
        if user_details:
            session['user'] = user_details
            return jsonify({'user': user_details}), 200
        else:
            session.pop('user', None)
            return jsonify({'error': 'User not found'}), 401
    else:
        return jsonify({'error': 'Not authenticated'}), 401

@auth_bp.route('/api/user/language', methods=['PUT'])
def update_user_language():
    if 'user' not in session:
        return jsonify({"error": "Unauthorized"}), 401
    data = request.get_json()
    lang = data.get('lang')
    if lang not in ['en', 'ar']:
        return jsonify({"error": "Invalid language"}), 400
    success = db_connector.update_user_language(session['user']['username'], lang)
    if success:
        user_session = session['user']
        user_session['lang'] = lang
        session['user'] = user_session
        return jsonify({"message": "Language updated"}), 200
    else:
        return jsonify({"error": "Failed to update language"}), 500

@auth_bp.route('/api/user/theme', methods=['PUT'])
def api_update_user_theme():
    if 'user' not in session:
        return jsonify({"error": "Unauthorized"}), 401
    data = request.get_json()
    theme = data.get('theme')
    if theme not in ['light', 'dark']:
        return jsonify({"error": "Invalid theme"}), 400
    username = session['user']['username']
    success = db_connector.update_user_theme(username, theme)
    if success:
        user_session = session['user']
        user_session['theme'] = theme
        session['user'] = user_session
        return jsonify({"message": "Theme updated"}), 200
    else:
        return jsonify({"error": "Failed to update theme"}), 500