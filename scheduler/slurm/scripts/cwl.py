#!/home/ubuntu/.virtualenvs/p2/bin/python
import requests
import os
import hashlib
import subprocess
import argparse
import time


def run():
    parser = argparse.ArgumentParser(description="CWL runner")
    parser.add_argument('--cwl', type=str, help='cwl file')
    parser.add_argument('--inputs', type=str, help='input json')

    args = parser.parse_args()
    run_cwl(args.cwl, args.inputs)


def run_cwl(cwl, inputs):
    identifier = os.getenv('SLURM_JOB_ID', hashlib.sha1(str(time.time())))
    tmp_cwl = identifier + '.cwl'
    input_json = identifier + '.json'
    with open(tmp_cwl, 'w') as f:
        f.write(cwl)
    with open(input_json, 'w') as f:
        f.write(inputs)
    p = subprocess.Popen(["cwl-runner", tmp_cwl, input_json],
                         stderr=subprocess.PIPE,
                         stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()
    if p.returncode == 0:
        print stdout
        print requests.put(
            "http://{}:5000/job/{}"
            .format(os.environ['SLURM_SUBMIT_HOST'], identifier),
            data=stdout).text

        exit(0)
    else:
        print "Output:", stdout
        print "Error", stderr
        requests.put(
            "http://{}:5000/job/{}"
            .format(os.environ['SLURM_SUBMIT_HOST'], identifier),
            data=stderr)

        exit(p.returncode)

if __name__ == "__main__":
    run()
