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


def submit_job(script, command, job_uuid, payload_hash, 
               args=[], inputs=None, workflow=None, env={}):
    # args should be given before the script
    command  = ["sbatch"] + args + [script] + command

    job_name = None
    # TODO: Should we standardize the job name? 
    with capp.db.session as s:
        j = Job(
            job_uuid=job_uuid, 
            #engine_id=slurm_id,
            checksum=payload_hash,
            input=inputs,
            workflow=workflow
        )
        s.add(j)

        result   = sys_call(command, env=env)
        slurm_id = result.split()[-1]
        j.update(engine_id=slurm_id)
        s.merge(j)

        if g.request_log:
            g.request_log.job_id = job_uuid 
        s.merge(g.request_log)

    return {'job_uuid': job_uuid, "checksum": payload_hash}


def cancel_job(jid, id_type='job_uuid'):
    # make sure job exist
    slurm_id = None
    with capp.db.session as s:
        filter_column = Job.job_uuid if id_type == 'job_uuid' else Job.checksum
        j = _find_job(jid, s, filter_column)
        slurm_id = j.engine_id
    sys_call(["scancel", str(slurm_id)])


def get_job(jid, id_type='job_uuid'):
    filter_column = Job.job_uuid if id_type == 'job_uuid' else Job.checksum
    with capp.db.session as s:
        j = _find_job(jid, s, filter_column)
        return j.todict()


# find job, sync with slurm, and also link to request log
## KMH: change to get more data and output using the --parsable2 flag
def _find_job(sid, session, filter_column=Job.job_uuid):
    job_obj = session.query(Job).filter(filter_column == sid).first()
    if not job_obj:
        raise UserError("Problem finding record for job {0}".format(sid))

    # Get slurm id
    jid = job_obj.job_uuid
    slurm_id = job_obj.engine_id

    job_elements = ["JobID", "JobName", "NodeList", "State", "ExitCode", "ElapsedRaw", 
        "AveDiskRead", "MaxDiskRead", "AveDiskWrite", "MaxDiskWrite",
        "AveVMSize", "MaxVMSize", "AveRSS", "MaxRSS", "Start", "State", 
        "Submit", "End"]
    result = parse_output(
        sys_call(["sacct", "-j", str(slurm_id),
                 "--format", ",".join(job_elements),
                 "--parsable2",
                 "--units=M"]))

    # NOTE: I don't see how this will work after changing to hash identifier
    #if not job_obj:
    #    if len(result) == 0:
    #        raise JobNotFound(jid)
    #    else:
    #        job_obj = Job(id=jid)

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
            raise UserError("Problem finding record for jobid {0}".format(jid))
             
        job = {
            'job_uuid': jid, 
            'engine_id': r['JobID'],
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
        g.request_log.job_id = job_obj.job_uuid
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
        if val == '0':
            return float(val)

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
