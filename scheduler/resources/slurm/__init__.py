from subprocess import Popen, PIPE
from flask import g
from flask import current_app as capp
from scheduler.models.models import Job
from scheduler.errors import UserError, InternalError, JobNotFound


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


def submit_job(script, command, args=[], env={}):
    # args should be given before the script
    command = ["sbatch"] + args + [script] + command
    result = sys_call(command, env=env)
    job_id = result.split()[-1]
    with capp.db.session as s:
        j = Job(id=job_id)
        s.add(j)
        if g.request_log:
            g.request_log.job_id = job_id
        s.merge(g.request_log)

    return {'job': job_id}


def cancel_job(jid):
    # make sure job exist
    with capp.db.session as s:
        _find_job(jid, s)
    sys_call(["scancel", str(jid)])


def get_job(jid):
    with capp.db.session as s:
        j = _find_job(jid, s)
        return j.todict()


# find job, sync with slurm, and also link to request log
def _find_job(jid, session):
    job_obj = session.query(Job).get(jid)
    result = parse_output(
        sys_call(["sacct", "-j", str(jid),
                 "--format", "JobID%10,nodelist%30,State%10,ExitCode%8"]))

    if not job_obj:
        if len(result) == 0:
            raise JobNotFound(jid)
        else:
            job_obj = Job(id=jid)

    if len(result) > 0:
        r = result[0]
        job = {'id': r['JobID'], 'exit_code': r['ExitCode'],
               'nodelist': r['NodeList'], 'state': r['State']}
        job_obj.update(**job)
        session.merge(job_obj)

    if g.request_log:
        g.request_log.job_id = job_obj.id
        session.merge(g.request_log)
    return job_obj


def update_job(jid, update={}):
    if not update:
        return
    with capp.db.session as s:
        j = _find_job(jid, s)
        if 'log' in update:
            j.log += update.get('log', '')
            del update['log']
        j.update(**update)
        s.merge(j)
        return j.todict()
