from flask import Blueprint, request, jsonify
import logging
from datetime import datetime
import db_connector

memories_bp = Blueprint('memories', __name__)

@memories_bp.route('/api/memories', methods=['GET'])
def api_get_memories():
    try:
        current_dt = datetime.now()
        month_str = request.args.get('month')
        month = int(month_str) if month_str and month_str.isdigit() else current_dt.month
        day_str = request.args.get('day')
        day = int(day_str) if day_str and day_str.isdigit() else None
        limit_str = request.args.get('limit', '5')
        limit = max(1, min(int(limit_str) if limit_str.isdigit() else 5, 10))
        if not 1 <= month <= 12: return jsonify({"error": "Invalid month."}), 400
        if day is not None and not 1 <= day <= 31: return jsonify({"error": "Invalid day."}), 400
        memories = db_connector.fetch_memories_from_oracle(month=month, day=day, limit=limit)
        return jsonify({"memories": memories})
    except Exception as e:
        return jsonify({"error": "Failed to fetch memories."}), 500