from sqlalchemy import Integer, String, Column, Boolean, text, DateTime, Float, BIGINT
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import ForeignKey
from sqlalchemy.orm import relationship

Base = declarative_base()


class DictMixin(object):
    def update(self, **kwargs):
        [setattr(self, k, v)
         for k, v in kwargs.iteritems() if hasattr(self, k)]

    def todict(self):
        return {column.name: getattr(self, column.name)
                for column in self.__table__.columns}


class Job(DictMixin, Base):
    __tablename__ = 'job'

    # main id
    id = Column(Integer, primary_key=True)

    # generated UUID for job
    job_uuid = Column(String(36), nullable=False, unique=True, index=True)
 
    # job id created by the downstream scheduler/task/engine handler (e.g., SLURM, rabix, etc) 
    engine_id = Column(String, nullable=False, index=True) 

    # checksum (SHA256)
    checksum = Column(String(64), nullable=False, index=True)

    # optionally set by user as an identifier
    job_name = Column(String)

    # state of a job, get from slurm sacct
    state = Column(String)

    # log stream of a job, reported real-time by
    # resources/slurm/script/cwl.py on slurm workers
    log = Column(String, default="")

    # job input as JSON
    input = Column(JSON(none_as_null=True))

    # job output, reported by
    # resources/slurm/script/cwl.py
    output = Column(JSON(none_as_null=True))

    # cwl workflow running step,
    # reported by reousrces/slurm/script/cwl.py
    running_state = Column(String)

    # slurm exit code, get from slurm sacct
    exit_code = Column(String)

    # nodelist of a job, get from slurm sacct
    nodelist = Column(String)

    ## Job statistics from sacct
    ## YYYY-MM-DDTHH:MM:SS
    submit_time = Column(DateTime)

    start_time = Column(DateTime)

    end_time = Column(DateTime)

    elapsed_seconds = Column(BIGINT)

    ## All of these should be MB as set in the sacct --units=M
    ave_disk_read = Column(Float)
    max_disk_read = Column(Float)
    
    ave_disk_write = Column(Float)
    max_disk_write = Column(Float)

    ave_rss = Column(Float)
    max_rss = Column(Float)

    ave_vm_size = Column(Float)
    max_vm_size = Column(Float)

    #TODO: cpu times
     
class RequestLog(DictMixin, Base):
    # request payload
    __tablename__ = "request_log"

    id = Column(Integer, primary_key=True)
    job_id = Column(String(36), ForeignKey("job.job_uuid"))
    job = relationship("Job", backref="requests")

    # might be a user does not exist, so it's not a foreignkey
    username = Column(String)

    payload = Column(String)
    url = Column(String)
    method = Column(String)
    status_code = Column(Integer)

    timestamp = Column(DateTime(timezone=True), nullable=False, server_default=text('now()'))


class User(DictMixin, Base):
    __tablename__ = "user"
    username = Column(String, primary_key=True)
    password = Column(String)
    is_admin = Column(Boolean, default=False)
