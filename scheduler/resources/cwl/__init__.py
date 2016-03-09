import glob
from pkg_resources import resource_filename, resource_string
import json
import os
from scheduler.errors import UserError, NotFound
from scheduler.models.models import CWLFile

GITHUB_CWL_ROOT = (
    "https://raw.githubusercontent.com/"
    "LabAdvComp/cwlutils/master/cwl/")


class CWLLibrary(object):

    def __init__(self, allowed_registries):
        self.load_cwl()
        self.allowed_registries = allowed_registries

    def load_cwl(self):
        cwl_dir = resource_filename('scheduler', 'cwlutils')
        self.workflows = {}
        for f in glob.glob(cwl_dir+'/cwl/*.cwl'):
            filename = os.path.basename(f)
            self.workflows[filename] = resource_string(
                'scheduler', 'cwlutils/cwl/'+filename)

    def validate(self, content):
        try:
            document = yaml.load(content)
        except ParserError as e:
            raise UserError("Invalid cwl: {}".format(str(e)))
        if hasattr(document, 'iteritems'):
            self.validate_single_document(document)
        elif hasattr(document, 'iter'):
            for single_doc in document:
                self.validate_single_document(single_doc)
        else:
            raise UserError("Invalid cwl: workflow has to be a dict or list")
        return True

    def validate_single_document(self, document):
        doc_class = document.get('class')
        if not doc_class:
            raise UserError("Have to provide a class for your workflow")
        if doc_class == 'Workflow':
            self.validate_workflow(document)
        elif doc_class == 'CommandLineTool':
            self.validate_commandline(document)
        else:
            raise UserError("Class {} not supported".format(doc_class))

    def validate_workflow(self, document):
        try:
            for step in document['steps']:
                import_cwl = step['run']['import']
                if import_cwl.startswith('http'):
                    if import_cwl.startswith(GITHUB_CWL_ROOT):
                        raise UserError(
                            "Only allow import from {}"
                            .format(GITHUB_CWL_ROOT))
                else:
                    cwl = get_cwl(import_cwl)
                    # replace a local file reference with a github url
                    step['run']['import'] = cwl['url']
        except KeyError as e:
            raise UserError("Missing {} in document".format(e))

    def validate_commandline(self, document):
        try:
            has_docker_requirement = False
            for req in document['requirements']:
                if req['class'] == 'DockerRequirement':
                    has_docker_requirement = True
                    if req['dockerPull'] not in self.allowed_registries:
                        raise UserError(
                            'Pull docker from {} is not allowed,'
                            'allowed registries are:'
                            .format(req['dockerPull'],
                            self.allowed_registries))

            if not has_docker_requirement:
                raise UserError("Have to use docker for CommandLineTool")
        except KeyError as e:
            raise UserError("Missing {} in document".format(e))

    def construct_script(self, payload):
        if 'document' in payload:
            cwl_content = payload['document']
            validate(cwl_content)
        else:
            raise UserError("Need to provide cwl document")
        if 'inputs' in payload:
            return ["--inputs", json.dumps(payload['inputs']),
                    "--cwl", json.dumps(cwl_content)]
        else:
            raise UserError("Need to provide inputs")

    def get_cwl(self, name):
        if name in self.workflows:
            return {'url': GITHUB_CWL_ROOT+name,
                    'document': self.workflows[name]}
        else:
            raise NotFound("Workflow {} not found".format(name))

    def list_cwl(self):
        return {'workflows': self.workflows.keys()}
