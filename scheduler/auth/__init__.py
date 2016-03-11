from flask import current_app as capp
from ..models.models import User
from ..errors import UnauthorizedError, UserError
import subprocess
from flask import request
import re


def check_user():
    # job submission, deletion should be done by user,
    # so it's authed by models.User,
    # job update should be done by slurm workers,
    # so it's authed by munge

    update_job_re = re.compile("/job/(.+)")
    # if it's a job update, use munge for auth checking
    if update_job_re.search(request.path) and request.method == 'PUT':
        auth_by_munge()
    elif capp.config['AUTH']:
        basic_auth()


def auth_by_munge():
    p = subprocess.Popen(
        ["unmunge", "-m", "/dev/null"], stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout = p.communicate(input=request.data)[0]
    if p.returncode == 0:
        try:
            request.data = stdout
        except Exception as e:
            raise UserError("Invalid json: {}".format(e))
    else:
        raise UnauthorizedError(
            "Payload for job update should be encoded by munge")


def basic_auth():
    auth = request.authorization
    if not auth:
        raise UnauthorizedError("Basic auth required")
    with capp.db.session as s:
        if not s.query(User).filter(User.username == auth.username)\
                .filter(User.password == auth.password).first():
            raise UnauthorizedError("Invalid user")
