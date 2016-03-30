from flask import current_app as capp
from functools import wraps
from ..models.models import User
from ..errors import UnauthorizedError, UserError
import subprocess
import bcrypt
from flask import request
from flask import g

def assert_admin():
    if not g.user or not g.user.is_admin:
        raise UnauthorizedError("Need admin privilege to execute bash script")


def auth_by_munge(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
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
        return func(*args, **kwargs)
    return wrapper


def basic_auth(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        auth = request.authorization
        if not auth:
            raise UnauthorizedError("Basic auth required")
        with capp.db.session as s:
            user = s.query(User).filter(User.username == auth.username).first()
            if not user:
                raise UnauthorizedError("User {} does not exist".format(auth.username))
            else:
                if not user.password == bcrypt.hashpw(auth.password, user.password.encode('utf-8')):
                    raise UnauthorizedError("Incorrect password")
            g.user = user
        return func(*args, **kwargs)
    return wrapper
