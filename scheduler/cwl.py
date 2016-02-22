import flask
import cwl_util
from flask import request, jsonify


blueprint = flask.Blueprint('cwl', __name__)

@blueprint.route('/', methods=['GET'])
def list_cwl():
    return jsonify(cwl_util.list_cwl())

@blueprint.route('/', methods=['POST'])
def register_cwl():
    payload = request.get_json()
    cwl_util.register_cwl(payload)
    return "", 201
