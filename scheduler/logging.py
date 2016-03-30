from models.models import RequestLog
from flask import g
from flask import current_app as capp
from flask import request


def log_request():
    with capp.db.session as s:
        if request.authorization:
            username = request.authorization.username
        else:
            username = None
        log = RequestLog(
            username=username, url=request.url,
            method=request.method, payload=request.data)
        s.add(log)
    g.request_log = log


def log_response_code(response):
    if g.request_log:
        with capp.db.session as s:
            g.request_log.status_code = response.status_code
            s.merge(g.request_log)
    return response
