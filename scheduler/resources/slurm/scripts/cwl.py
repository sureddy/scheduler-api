#!/home/ubuntu/.virtualenvs/p2/bin/python
import re
import json
import requests
import shutil
import os
import hashlib
import subprocess
import argparse
import time
from threading import Thread
import logging

logger = logging.Logger(__name__)
logger.addHandler(logging.StreamHandler())

ROOT_DIR = "/mnt/SCRATCH"


def run():
    parser = argparse.ArgumentParser(description="CWL runner")
    parser.add_argument('--cwl', type=str, help='cwl file')
    parser.add_argument('--workflow-id', type=str, help='id of your workflow')
    parser.add_argument('--inputs', type=str, help='input json')
    parser.add_argument('--proxies',
                        action='store_true', help='use http proxy')

    args = parser.parse_args()
    run_cwl(args.cwl, args.inputs, args.workflow_id, args.proxies)


def write_input_files(input_files):
    input_files = json.loads(input_files)
    for filename, content in input_files.iteritems():
        with open(filename, 'wb') as f:
            f.write(content)


def set_workdir(identifier):
    workdir = os.path.join(ROOT_DIR, identifier)
    os.makedirs(workdir)
    os.chdir(workdir)
    return workdir


def setup_files(cwl, inputs, identifier):
    if 'input_files' in os.environ:
        write_input_files(os.environ['input_files'])
    tmp_cwl = identifier + '.cwl'
    input_json = identifier + '.json'
    with open(tmp_cwl, 'w') as f:
        f.write(cwl)
    with open(input_json, 'w') as f:
        f.write(inputs)
    return tmp_cwl, input_json


def listen_output(stderr, report_url):
    for line in iter(stderr.readline, b''):
        logger.info(line)
        status = {'log': line}
        job_match_regex = re.compile('\[job (.+)\].*')
        match = job_match_regex.search(line)
        if match:
            status['running_state'] = match.group(1)
        report_to_scheduler(report_url, status)
    stderr.close()


def run_cwl(cwl, inputs, workflow_id, proxies):
    identifier = os.getenv('SLURM_JOB_ID', hashlib.sha1(str(time.time())))
    workdir = set_workdir(identifier)
    try:
        cwl_file, input_file = setup_files(
            cwl, inputs, identifier)
        environ = dict(os.environ)
        environ['http_proxy'] = 'http://cloud-proxy:3128'
        environ['https_proxy'] = 'http://cloud-proxy:3128'

        p = subprocess.Popen(["cwl-runner", cwl_file+workflow_id, input_file],
                             stderr=subprocess.PIPE,
                             stdout=subprocess.PIPE,
                             env=environ,
                             cwd=workdir)
        report_url = ("http://{}:5000/job/{}"
                      .format(os.environ['SLURM_SUBMIT_HOST'], identifier))

        t = Thread(target=listen_output, args=(p.stderr, report_url))
        # was trying to set deamon=True but that will lose some of the last outputs
        # as the program exit before last request is sent
        # t.daemon = True
        t.start()
        p.wait()
        stdout = p.stdout.read()
        logger.info(stdout)

        if p.returncode == 0:
            report_to_scheduler(report_url, {"output": stdout})
            exit(0)
        else:
            exit(p.returncode)
    except Exception as e:
        logger.exception("Workflow failed")
        report_to_scheduler(report_url, {"log": str(e)})
    finally:
        shutil.rmtree(workdir)


def report_to_scheduler(report_url, data):
    try:
        # use munge to encode data
        p = subprocess.Popen(
            ["munge", "-s", json.dumps(data)], stdout=subprocess.PIPE)
        r = requests.put(report_url, data=p.stdout.read())
        if r.status_code != 200:
            print r.text
        time.sleep(5)
    except Exception as e:
        print "Fail to report to scheduler", e


if __name__ == "__main__":
    run()
