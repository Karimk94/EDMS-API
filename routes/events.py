from flask import Blueprint, request, jsonify
import math
import logging
import db_connector

events_bp = Blueprint('events', __name__)

@events_bp.route('/api/events', methods=['GET'])
def get_events_route():
    page = request.args.get('page', 1, type=int)
    search = request.args.get('search', None, type=str)
    page_size = request.args.get('pageSize', 20, type=int)
    fetch_all = request.args.get('fetch_all', 'false', type=str).lower() == 'true'
    if page < 1: page = 1
    if page_size > 100: page_size = 100
    events_list, total_rows = db_connector.get_events(page=page, page_size=page_size, search=search, fetch_all=fetch_all)
    total_pages = math.ceil(total_rows / page_size) if total_rows > 0 else 1
    has_more = (page * page_size) < total_rows
    return jsonify({"events": events_list, "page": page, "total_pages": total_pages, "hasMore": has_more})

@events_bp.route('/api/events', methods=['POST'])
def create_event_route():
    data = request.get_json()
    event_name = data.get('name')
    if not event_name: return jsonify({"error": "Event name is required."}), 400
    event_id, message = db_connector.create_event(event_name)
    if event_id: return jsonify({"id": event_id, "message": message}), 201
    else: return jsonify({"error": message}), 400

@events_bp.route('/api/events/<int:event_id>/documents', methods=['GET'])
def get_event_documents_route(event_id):
    page = request.args.get('page', 1, type=int)
    page_size = 1
    if page < 1: page = 1
    documents, total_pages, error_message = db_connector.get_documents_for_event(event_id=event_id, page=page, page_size=page_size)
    if error_message:
        status_code = 404 if "not found" in error_message.lower() else 500
        return jsonify({"error": error_message}), status_code
    current_doc = documents[0] if documents else None
    return jsonify({"document": current_doc, "page": page, "total_pages": total_pages})

@events_bp.route('/api/journey', methods=['GET'])
def get_journey_data():
    try:
        journey_data = db_connector.fetch_journey_data()
        return jsonify(journey_data)
    except Exception as e:
        logging.error(f"Error in /api/journey: {e}", exc_info=True)
        return jsonify({"error": "Failed to fetch journey data."}), 500