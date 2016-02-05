from flask import current_app as capp


def check_user():
    if capp.config['AUTH']:
        return 'user'
