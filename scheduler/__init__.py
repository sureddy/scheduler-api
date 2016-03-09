from flask import Flask, jsonify
from .job import blueprint as job
from .cwl import blueprint as cwl
from .resources.cwl import CWLLibrary
from auth import check_user
from errors import APIError
from models.driver import SQLAlchemyDriver

app = Flask(__name__)


def app_init(app):
    app.register_blueprint(job, url_prefix='/job')
    app.register_blueprint(cwl, url_prefix='/cwl')
    app.config.from_object('scheduler.settings')
    app.url_map.strict_slashes = False
    app.db = SQLAlchemyDriver(app.config['DB'])
    app.cwl = CWLLibrary(app.config['ALLOWED_DOCKER_REGISTRIES'])

app_init(app)


@app.route('/')
def root():
    return jsonify({'job endpoint': '/job', 'cwl endpoint': '/cwl'})


@app.before_request
def before_req():
    check_user()


@app.errorhandler(APIError)
def user_error(e):
    if hasattr(e, 'json') and e.json:
        return jsonify(**e.json), e.code
    else:
        return jsonify(message=e.message), e.code
