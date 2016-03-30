from sqlalchemy import Integer, String, Column, Boolean, DateTime, text
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
    # job id created by slurm
    id = Column(String, primary_key=True)

    # optionally set by user as an identifier
    job_name = Column(String)

    # state of a job, get from slurm sacct
    state = Column(String)

    # log stream of a job, reported real-time by
    # resources/slurm/script/cwl.py on slurm workers
    log = Column(String, default="")

    # job output, reported by
    # resources/slurm/script/cwl.py
    output = Column(String)

    # cwl workflow running step,
    # reported by reousrces/slurm/script/cwl.py
    running_state = Column(String)

    # slurm exit code, get from slurm sacct
    exit_code = Column(String)

    # nodelist of a job, get from slurm sacct
    nodelist = Column(String)


class RequestLog(DictMixin, Base):
    # request payload
    __tablename__ = "request_log"

    id = Column(Integer, primary_key=True)
    job_id = Column(String, ForeignKey("job.id"))
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
