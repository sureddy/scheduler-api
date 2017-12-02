import datetime
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
    fields = lines[0].split('|')
    for l in lines[1:]:
        if l.startswith('--------'):
            # sacct has a segregation line
            continue
        l = l.split('|')
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
    job_name = None
    # TODO: make hash of input payload as the job name and assign it
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
## KMH: change to get more data and output using the --parsable2 flag
def _find_job(jid, session):
    job_obj = session.query(Job).get(jid)
    job_elements = ["JobID", "JobName", "NodeList", "State", "ExitCode", "ElapsedRaw", 
        "AveDiskRead", "MaxDiskRead", "AveDiskWrite", "MaxDiskWrite",
        "AveVMSize", "MaxVMSize", "AveRSS", "MaxRSS", "Start", "State", 
        "Submit", "End"]
    result = parse_output(
        sys_call(["sacct", "-j", str(jid),
                 "--format", ",".join(job_elements),
                 "--parsable2",
                 "--units=M"]))

    if not job_obj:
        if len(result) == 0:
            raise JobNotFound(jid)
        else:
            job_obj = Job(id=jid)

    if len(result) > 0:
        # If any of the returned job data are from the slurm 'batch' job,
        # we need to basically keep everything except for the JobID and 
        # JobName
        bajlst = filter(lambda x: 'batch' in x['JobID'], result)
        if not bajlst:
            r = result[0]

        elif bajlst and len(bajlst) == 1: 
            baj = bajlst[0]
            # Get the first non batch job
            r = filter(lambda x: 'batch' not in x['JobID'], result)[0]
            # Update 
            for key in job_elements:
                if key not in ("JobID", "JobName"):
                    r[key] = baj[key]
        else:
            UserError("Problem finding record for jobid {0}".format(jid))
             
        job = {
            'id': r['JobID'], 
            'job_name': r['JobName'],
            'exit_code': r['ExitCode'],
            'nodelist': r['NodeList'],
            'state': r['State'],
            'elapsed_seconds': int(r['ElapsedRaw']) if r['ElapsedRaw'] else None,
            'ave_disk_read': format_slurm_mb_size(r['AveDiskRead']),
            'max_disk_read': format_slurm_mb_size(r['MaxDiskRead']),
            'ave_disk_write': format_slurm_mb_size(r['AveDiskWrite']),
            'max_disk_write': format_slurm_mb_size(r['MaxDiskWrite']),
            'ave_rss': format_slurm_mb_size(r['AveRSS']),
            'max_rss': format_slurm_mb_size(r['MaxRSS']),
            'ave_vm_size': format_slurm_mb_size(r['AveVMSize']),
            'max_vm_size': format_slurm_mb_size(r['MaxVMSize']),
            'submit_time': format_slurm_datetime(r['Submit']), 
            'start_time': format_slurm_datetime(r['Start']), 
            'end_time': format_slurm_datetime(r['End']) 
        }
        job_obj.update(**job)
        session.merge(job_obj)

    if g.request_log:
        g.request_log.job_id = job_obj.id
        session.merge(g.request_log)
    return job_obj

def format_slurm_datetime(val):
    # YYYY-MM-DDTHH:MM:SS
    if val and val != 'Unknown':
        # Should prob capture err
        #try: 
        fmt = datetime.datetime.strptime(val, '%Y-%m-%dT%H:%M:%S')
        #except ValueError:
        return fmt
    else:
        return None

def format_slurm_mb_size(val):
    #NOTE: You must use --units=M in sacct! 
    if val and val != 'Unknown':
        # Should prob capture err
        assert val[-1] == 'M', "Expect MB but got {}".format(val)
        #try: 
        fmt = float(val[:-1]) 
        #except ValueError:
        return fmt
    else:
        return None
 
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
