from flask import current_app as capp
import json
from ..errors import UserError
import uuid
from ..models.models import CWLFile


def register_cwl(name, content):
    if validate(content):
        with capp.db.session as s:
            f = CWLFile(name=name, content=content, id=str(uuid.uuid4()))
            s.merge(f)


def validate(content):
    return True


def construct_script(payload):
    if 'id' in payload:
        cwl_content = get_cwl(id=payload['id'])
    elif 'name' in payload:
        cwl_content = get_cwl(name=payload['name'])
    elif 'content' in payload:
        cwl_content = payload['content']
        validate(cwl_content)
    else:
        raise UserError("Need to provide cwl content or id or name")
    if 'inputs' in payload:
        return ["--inputs", json.dumps(payload['inputs']),
                "--cwl", json.dumps(cwl_content)]
    else:
        raise UserError("Need to provide inputs")


def get_cwl(id=None, name=None):
    with capp.db.session as s:
        query = s.query(CWLFile)
        if id:
            query = query.filter(id=id)
        if name:
            query = query.filter(name=name)
        result = query.first()
        if result:
            return {'id': result.id,
                    'name': result.name, 'content': result.content}


def list_cwl():
    results = {}
    with capp.db.session as s:
        results = [
            {'id': n.id, 'description': n.description}
            for n in s.query(CWLFile)]
    return results
