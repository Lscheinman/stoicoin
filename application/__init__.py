import flask, os
from flask_debugtoolbar import DebugToolbarExtension
from werkzeug.utils import secure_filename

UPLOAD_FOLDER = '%s/application/services/data/' % (os.getcwd())   # change from application to \ when deploying to unix
ALLOWED_EXTENSIONS = ['txt', 'csv', 'pdf', 'xls', 'xlsx']

# Intitialize and configure Flask app.
app = flask.Flask(__name__)
app.debug = False
app.config.from_object('application.config')
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
toolbar = DebugToolbarExtension(app)

import application.views
