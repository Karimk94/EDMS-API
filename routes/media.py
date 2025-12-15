from flask import Blueprint, jsonify, Response, send_file, send_from_directory, request, stream_with_context, session
import os
import mimetypes
import db_connector
import wsdl_client
from utils.common import editor_required

media_bp = Blueprint('media', __name__)

@media_bp.route('/api/image/<doc_id>')
def api_get_image(doc_id):
    dst = db_connector.dms_system_login()
    if not dst: return jsonify({'error': 'DMS login failed.'}), 500
    image_data, _ = wsdl_client.get_image_by_docnumber(dst, doc_id)
    if image_data: return Response(bytes(image_data), mimetype='image/jpeg')
    return jsonify({'error': 'Image not found in EDMS.'}), 404

@media_bp.route('/api/pdf/<doc_id>')
def api_get_pdf(doc_id):
    dst = db_connector.dms_system_login()
    if not dst: return jsonify({'error': 'DMS login failed.'}), 500
    pdf_data, _ = wsdl_client.get_image_by_docnumber(dst, doc_id)
    if pdf_data: return Response(bytes(pdf_data), mimetype='application/pdf')
    return jsonify({'error': 'PDF not found in EDMS.'}), 404

@media_bp.route('/api/video/<doc_id>')
def api_get_video(doc_id):
    dst = db_connector.dms_system_login()
    if not dst: return jsonify({'error': 'DMS login failed.'}), 500
    original_filename, media_type, file_ext = db_connector.get_media_info_from_dms(dst, doc_id)
    if not original_filename: return jsonify({'error': 'Video metadata not found.'}), 404
    if media_type != 'video': return jsonify({'error': 'Not a video.'}), 400
    if not file_ext: file_ext = '.mp4'
    cached_video_path = os.path.join(db_connector.video_cache_dir, f"{doc_id}{file_ext}")
    if os.path.exists(cached_video_path):
        return send_file(cached_video_path, as_attachment=False)
    stream_details = db_connector.get_dms_stream_details(dst, doc_id)
    if not stream_details: return jsonify({'error': 'Could not open stream.'}), 500
    stream_generator = db_connector.stream_and_cache_generator(
        obj_client=stream_details['obj_client'], stream_id=stream_details['stream_id'],
        content_id=stream_details['content_id'], final_cache_path=cached_video_path
    )
    mimetype, _ = mimetypes.guess_type(cached_video_path)
    return Response(stream_with_context(stream_generator), mimetype=mimetype or "video/mp4")

@media_bp.route('/cache/<path:filename>')
def serve_cached_thumbnail(filename):
    return send_from_directory(db_connector.thumbnail_cache_dir, filename)

@media_bp.route('/api/clear_cache', methods=['POST'])
@editor_required
def api_clear_cache():
    try:
        db_connector.clear_thumbnail_cache()
        db_connector.clear_video_cache()
        return jsonify({"message": "All caches cleared successfully."})
    except Exception as e:
        return jsonify({"error": f"Failed to clear cache: {e}"}), 500

@media_bp.route('/api/media_counts', methods=['GET'])
def get_media_counts():
    try:
        app_source = request.headers.get('X-App-Source', 'unknown')
        scope = request.args.get('scope')
        counts = db_connector.get_media_type_counts(app_source=app_source, scope=scope)
        if counts: return jsonify(counts), 200
        else: return jsonify({"images": 0, "videos": 0, "files": 0}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500