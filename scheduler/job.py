import flask
import json
import os
from pkg_resources import resource_filename
from auth import auth_by_munge, basic_auth, assert_admin
from errors import UserError
from resources import slurm
from flask import request, jsonify
from flask import current_app as capp


blueprint = flask.Blueprint('job', __name__)


@blueprint.route("/<jid>", methods=['GET'])
@basic_auth
def get_job(jid):
    return jsonify(slurm.get_job(jid))

@blueprint.route("/<jid>/outputs", methods=['GET'])
@basic_auth
def get_job_outputs(jid):
    job = slurm.get_job(jid)
    out = None
    if job['output']: 
        try:
            out = json.loads(job['output']) 
        except ValueError:
            out = None

    ret = {
      "id": job["id"],
      "state": job['state'],
      "outputs": out
    } 
    return jsonify(ret), 200

@blueprint.route("/<jid>/status", methods=['GET'])
@basic_auth
def get_job_status(jid):
    job = slurm.get_job(jid)
    ret = {
      "id": job["id"],
      "state": job['state']
    } 
    return jsonify(ret), 200

@blueprint.route("/<jid>", methods=['DELETE'])
@basic_auth
def cancel_job(jid):
    slurm.cancel_job(jid)
    return "", 201


@blueprint.route("/", methods=['POST'])
@basic_auth
def create_job():
    """
    Create a job.
    :query type: type of the job, can be bash or cwl, default to command
    ** Example of a bash job: **
    .. code-block:: http
        POST /job/ HTTP/1.1
        Authorization: Basic QWxhZGRpbjpPcGVuU2VzYW1l
    .. code-block:: Javascript
        {
            "command": "echo 'test'"
        }
    ** Example output: **
    .. code-block:: http
        HTTP/1.1 200
        Content-Type: application/json

    .. code-block:: Javascript
        {
            "job": "445"
        }

    ** Example of a cwl job: **
    A cwl job accept a json document of your workflow, and a json input, if
    there are input of type File, you should give another json which has
    the content of your files

    Example workflow:
    .. code-block:: yaml
      class: CommandLineTool
      requirements:
        # DockerRequirement is required for scheduler API
        - class: DockerRequirement
          dockerPull: quay.io/cdis/cwlutils:s3cwl
      inputs:
        - id: "#echo-in"
          type: File
          label: "Message"
          description: "The message to print"
          inputBinding: {}
      outputs:
        - id: "#echo-out"
          type: File
          label: "Printed Message"
          description: "The file containing the message"
          outputBinding:
            glob: messageout.txt

      baseCommand: echo
      stdout: messageout.txt

    Example inputs:
    .. code-block:: yaml
        "echo-in":
            class: File
            # need to be a relative path
            path: filea

    Example input files:
    .. code-block:: yaml
        filea: content of the file

    .. code-block:: http
        POST /job/ HTTP/1.1
        Authorization: Basic QWxhZGRpbjpPcGVuU2VzYW1l

    .. code-block:: Javascript
        {
            "document": cwl json,
            "inputs": input json,
            "input_files": json which specify actual content of input files
        }

    ** Example output: **
    .. code-block:: http
        HTTP/1.1 200
        Content-Type: application/json

    .. code-block:: Javascript
        {
            "job": "445"
        }

    """
    req_type = request.args.get('type', 'bash')
    payload = request.get_json()
    env = os.environ
    if req_type == 'cwl':
        command, env = capp.cwl.construct_script(payload)
        script = resource_filename(
            'scheduler', 'resources/slurm/scripts/cwl.py')

    elif req_type == 'bash':
        assert_admin()
        command = [payload.get('command')]
        script = resource_filename(
            'scheduler', 'resources/slurm/scripts/command.sh')

    else:
        raise UserError("{} type not supported".format(req_type))
    return jsonify(
        slurm.submit_job(script, command, payload.get("args", []), env=env))


@blueprint.route("/<jid>", methods=['PUT'])
@auth_by_munge
def update_job(jid):
    """Update the status of a job.

    Accepted keys in json payload:
    log:            a log message that will be appended to job.log
    running_state:  cwl workflow running step
    output:         output of a job

    """
    try:
        payload = json.loads(request.data)
    except ValueError as e:
        raise UserError("Invalid json: {}".format(e))
    allowed_keys = {"log", "running_state", "output"}
    not_allowed = set(payload.keys()).difference(allowed_keys)
    if not_allowed:
        raise UserError(
            "Keys: {} not allowed, only allow update"
            "on: {}".format(not_allowed, allowed_keys))
    return jsonify(slurm.update_job(jid, payload))


@blueprint.route("/", methods=['GET'])
@basic_auth
def list_job():
    '''Get active jobs'''
    return jsonify(slurm.list_job())
