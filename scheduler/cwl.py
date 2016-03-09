import flask
from flask import request, jsonify
from flask import current_app as capp

blueprint = flask.Blueprint('cwl', __name__)


@blueprint.route('/', methods=['GET'])
def list_cwl():
    return jsonify(capp.cwl.list_cwl())


@blueprint.route('/<name>', methods=['GET'])
def get_cwl(name):
    if 'url' in request.args:
        return capp.cwl.get_cwl(name)['url']
    else:
        return capp.cwl.get_cwl(name)['document']
