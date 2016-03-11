from sqlalchemy import Integer, String, Column
from sqlalchemy.ext.declarative import declarative_base


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
    exit_code = Column(Integer)

    # nodelist of a job, get from slurm sacct
    nodelist = Column(String)


class User(DictMixin, Base):
    __tablename__ = "user"
    username = Column(String, primary_key=True)
    password = Column(String)
