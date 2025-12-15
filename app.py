from flask import Flask
from flask_cors import CORS
import logging
import os
from datetime import timedelta
from waitress import serve

from routes.auth import auth_bp
from routes.documents import documents_bp
from routes.media import media_bp
from routes.tags import tags_bp
from routes.events import events_bp
from routes.folders import folders_bp
from routes.favorites import favorites_bp
from routes.memories import memories_bp

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=60)
app.secret_key = os.getenv('FLASK_SECRET_KEY')
CORS(app, supports_credentials=True, resources={r"/api/*": {"origins": "*"}})

app.register_blueprint(auth_bp)
app.register_blueprint(documents_bp)
app.register_blueprint(media_bp)
app.register_blueprint(tags_bp)
app.register_blueprint(events_bp)
app.register_blueprint(folders_bp)
app.register_blueprint(favorites_bp)
app.register_blueprint(memories_bp)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    serve(app, host='0.0.0.0', port=port, threads=100)