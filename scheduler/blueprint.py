import flask
from errors import UserError
import slurm
import cwl
from flask import request


blueprint = flask.Blueprint('base', __name__)


@blueprint.route("/job/<jid>", methods=['GET'])
def get_job(jid):
    return jid


@blueprint.route("/job/<jid>", methods=['DELETE'])
def cancel_job(jid):
    return jid


@blueprint.route("/job", methods=['POST'])
def create_job():
    req_type = request.args.get('type', 'command')
    payload = request.get_json()
    if req_type == 'cwl':
        command = cwl.contruct_script(payload)
    elif req_type == 'command':
        command = payload.get('content')
    else:
        raise UserError("{} type not supported".format(req_type))
    jid = slurm.submit_job(command)
    return jid


@blueprint.route("/job", methods=['LIST'])
def list_job():
    jobs = slurm.list_job()
    return jobs
