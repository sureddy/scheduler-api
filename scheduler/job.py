import flask
from pkg_resources import resource_filename
from errors import UserError
import slurm
import cwl_util
from flask import request, jsonify


blueprint = flask.Blueprint('job', __name__)


@blueprint.route("/<jid>", methods=['GET'])
def get_job(jid):
    return jsonify(slurm.get_job(jid))


@blueprint.route("/<jid>", methods=['DELETE'])
def cancel_job(jid):
    slurm.cancel_job(jid)
    return "", 201


@blueprint.route("/", methods=['POST'])
def create_job():
    req_type = request.args.get('type', 'command')
    payload = request.get_json()
    if req_type == 'cwl':
        command = cwl_util.construct_script(payload)
        script = resource_filename('scheduler', 'slurm/scripts/cwl.py')
        print script

    elif req_type == 'command':
        command = [payload.get('command')]
        script = resource_filename('scheduler', 'slurm/scripts/command.sh')

    else:
        raise UserError("{} type not supported".format(req_type))
    return jsonify(slurm.submit_job(script, command, payload.get("args", [])))


@blueprint.route("/<jid>", methods=['PUT'])
def update_job(jid):
    message = request.get_data()
    return jsonify(slurm.update_job(jid, message=message))


@blueprint.route("/", methods=['GET'])
def list_job():
    '''Get active jobs'''
    return jsonify(slurm.list_job())
