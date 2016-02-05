from boto import connect_s3
from flask import current_app as capp


class CWLHandler(object):
    def __init__(self):
        self.conn = connect_s3(**capp.config['S3'])
        self.bucket = self.conn.get_bucket(**capp.config["CWL_BUCKET"])

    def register_cwl(self, content):
        if self.validate(content):
            # upload cwl file to s3, register in indexd, return an id
            pass

    def validate(content):
        pass

    def construct_script(self, payload):
        if 'id' in payload:
            cwl_content = self.get_cwl(payload['id'])
        else:
            cwl_content = payload.get('content')
        pass

    def get_cwl(self, cwl_id):
        pass
