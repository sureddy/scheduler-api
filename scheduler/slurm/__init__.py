from subprocess import Popen, PIPE
from flask import current_app as capp
from ..models.models import Job
from ..errors import UserError, InternalError, JobNotFound


class SysResult(object):
    def __init__(self, success, stdout, stderr):
        self.success = success
        self.stdout = stdout
        self.stderr = stderr


def parse_output(output):
    lines = output.strip().split('\n')
    result = []
    fields = lines[0].split()
    for l in lines[1:]:
        if l.startswith('--------'):
            # sacct has a segregation line
            continue
        l = l.split()
        print l
        print fields
        result.append({k: l[i] for i, k in enumerate(fields)})
    return result


def sys_call(args, **kwargs):
    try:
        p = Popen(args, stderr=PIPE,
                  stdout=PIPE,  cwd='/home/ubuntu/', **kwargs)
    except Exception as e:
        raise InternalError(e)

    output, err = p.communicate()
    if p.returncode == 0:
        return output
    else:
        raise UserError(err)


def list_job():
    return {'jobs': parse_output(sys_call("squeue"))}


def submit_job(script, command, args=[]):
    # args should be given before the script
    command = ["sbatch"] + args + [script] + command
    result = sys_call(command)
    job_id = result.split()[-1]
    return {'job': job_id}


def cancel_job(jid):
    # returns True even if jid does not exist
    sys_call(["scancel", str(jid)])


def get_job(jid):
    result = parse_output(
        sys_call(["sacct", "-j", str(jid),
                 "--format", "JobID%10,nodelist%30,State%10,ExitCode%8"]))
    if len(result) >= 1:
        r = result[0]
        job = {'id': r['JobID'], 'exit_code': r['ExitCode'],
               'nodelist': r['NodeList'], 'state': r['State']}
        with capp.db.session as s:
            j = s.query(Job).get(r['JobID'])
            if j:
                j.update(**job)
            else:
                j = Job(**job)
            s.merge(j)
            return j.todict()
    else:
        raise JobNotFound(jid)


def update_job(jid, **kwargs):
    get_job(jid)
    with capp.db.session as s:
        j = s.query(Job).get(jid)
        if not j:
            raise JobNotFound(jid)
        else:
            j.update(**kwargs)
            s.merge(j)
            return j.todict()
