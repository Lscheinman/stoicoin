import flask, os
from flask_debugtoolbar import DebugToolbarExtension
from werkzeug.utils import secure_filename
from celery import Celery

UPLOAD_FOLDER = '%s/application/services/data/' % (os.getcwd())   # change from application to \ when deploying to unix
ALLOWED_EXTENSIONS = ['txt', 'csv', 'pdf', 'xls', 'xlsx']

# Intitialize and configure Flask app.
app = flask.Flask(__name__)
app.debug = False
app.config.from_object('application.config')
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['CELERY_BROKER_URL'] = ''            # Where is celery broker service running? RabbitMQ? Redis?
app.config['CELERY_RESULT_BACKEND'] = ''
toolbar = DebugToolbarExtension(app)

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)
import application.views





