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
    id = Column(String, primary_key=True)
    job_name = Column(String)
    state = Column(String)
    message = Column(String)
    exit_code = Column(Integer)
    nodelist = Column(String)


class CWLFile(DictMixin, Base):
    __tablename__ = "CWL_file"
    id = Column(String, primary_key=True)
    name = Column(String, unique=True)
    description = Column(String)
    content = Column(String)
