#!/usr/bin/env python2
from docopt import docopt, DocoptExit
from cdispyutils.log import get_logger
from scheduler.utils import create_user
from scheduler.models.driver import SQLAlchemyDriver
from scheduler.settings import DB


logger = get_logger('scheduler-user')

doc = """Usage: scheduler-user.py create USERNAME PASSWORD"""

if __name__ == "__main__":
    try:
        arguments = docopt(doc)
        db = SQLAlchemyDriver(DB)
        if arguments['create']:
            create_user(db, arguments['USERNAME'], arguments['PASSWORD'])
            print 'User {} created'.format(arguments['USERNAME'])

    except DocoptExit as e:
        print "Invalid arguments"
        print e.message
