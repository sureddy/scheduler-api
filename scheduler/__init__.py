from flask import Flask, jsonify
from .job import blueprint as job
from .resources.cwl import CWLLibrary
from logging import log_request, log_response_code
from errors import APIError
from models.driver import SQLAlchemyDriver

app = Flask(__name__)


def app_init(app, settings="scheduler.settings"):
    print settings
    app.register_blueprint(job, url_prefix='/jobs')
    app.config.from_object(settings)
    app.url_map.strict_slashes = False
    app.db = SQLAlchemyDriver(app.config['DB'])
    app.cwl = CWLLibrary(app.config['ALLOWED_DOCKER_REGISTRIES'])


@app.before_request
def before_req():
    log_request()


@app.after_request
def after_req(response):
    return log_response_code(response)


@app.route('/')
def root():
    return jsonify({'job endpoint': '/jobs'})


@app.errorhandler(APIError)
def user_error(e):
    if hasattr(e, 'json') and e.json:
        return jsonify(**e.json), e.code
    else:
        return jsonify(message=e.message), e.code
