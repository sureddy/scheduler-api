import glob
from flask import current_app as capp
from pkg_resources import resource_filename, resource_string
import json
import os
from scheduler.errors import UserError, NotFound

GITHUB_CWL_ROOT = (
    "https://raw.githubusercontent.com/"
    "LabAdvComp/cwlutils/master/cwl/")


class CWLDoc(dict):
    def __getitem__(self, attr):
        if attr in self:
            value = super(CWLDoc, self).__getitem__(attr)
            if type(value) == dict:
                value = CWLDoc(value)
            return value
        else:
            raise UserError("Missing {} in document".format(attr))


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

    def validate(self, document):
        if hasattr(document, 'iteritems'):
            return self.validate_single_document(document)
        elif hasattr(document, '__iter__'):
            # validate all commandline tools first,
            # then validate Workflow, so that tools import can
            # be resolved
            local_steps = []
            workflow_doc = None
            for single_doc in document:
                if self.is_workflow(single_doc):
                    if workflow_doc:
                        raise UserError(
                            "Multiple workflow found in your document")
                    workflow_doc = single_doc
                    continue
                local_steps.append(self.validate_single_document(single_doc))
            if not workflow_doc:
                raise UserError("Need to have a Workflow in your document")
            return self.validate_single_document(workflow_doc, local_steps)
        else:
            raise UserError("Invalid cwl: workflow has to be a dict or list")

    def is_workflow(self, document):
        return document.get('class') == 'Workflow'

    def validate_single_document(self, document, local_steps=[]):
        document = CWLDoc(document)
        doc_class = document.get('class')
        if not doc_class:
            raise UserError("Have to provide a class for your workflow")
        if doc_class == 'Workflow':
            return self.validate_workflow(document, local_steps)
        elif doc_class == 'CommandLineTool':
            return self.validate_commandline(document)
        else:
            raise UserError("Class {} not supported".format(doc_class))

    def validate_workflow(self, document, local_steps=[]):
        document = CWLDoc(document)
        for step in document['steps']:
            step = CWLDoc(step)
            import_cwl = step['run']['import']
            # only support importing local steps or
            # cwl that's in cdis cwlutils
            if import_cwl in local_steps:
                continue
            if import_cwl.startswith('http'):
                if import_cwl.startswith(GITHUB_CWL_ROOT):
                    raise UserError(
                        "Only allow import from {}"
                        .format(GITHUB_CWL_ROOT))
            else:
                cwl = self.get_cwl(import_cwl)
                # replace a local file reference with a github url
                step['run']['import'] = cwl['url']
        return document['id']

    def validate_commandline(self, document):
        document = CWLDoc(document)
        has_docker_requirement = False
        for req in document['requirements']:
            req = CWLDoc(req)
            if req['class'] == 'DockerRequirement':
                has_docker_requirement = True
                if not any(map(
                        req['dockerPull'].startswith,
                        self.allowed_registries)):
                    raise UserError(
                        'Pull docker from {} is not allowed,'
                        'allowed registries are: {}'
                        .format(req['dockerPull'],
                                self.allowed_registries))

        if not has_docker_requirement:
            raise UserError("Have to use docker for CommandLineTool")
        return document['id']

    def construct_script(self, payload):
        """
        construct a command to be executed by subprocess
        input_files is dumped to environment variable so that
        when slurm run the command in worker, credential files
        won't be displayed when you list running processes

        """
        if 'document' in payload:
            cwl_content = payload['document']
            doc_id = self.validate(cwl_content)
        else:
            raise UserError("Need to provide cwl document")
        if 'inputs' in payload:
            if 'input_files' in payload:
                os.environ['input_files'] = json.dumps(payload['input_files'])

            script = ["--inputs", json.dumps(payload['inputs']),
                      "--cwl", json.dumps(cwl_content),
                      "--workflow-id", doc_id]
            if capp.config['PROXIES']:
                script.append("--proxies")
            return script, os.environ
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
