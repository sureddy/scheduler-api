from scheduler import app as application
from base64 import b64encode

from addict import Dict
from scheduler.models.models import User
from scheduler.utils import create_user
from scheduler import app_init
import pytest


AUTH_HEADER = {
    'Authorization': 'Basic %s' % b64encode(b"test:test").decode("ascii")
}


@pytest.fixture(scope="module")
def app():
    app_init(application, settings="test_settings")
    return application


@pytest.fixture(scope="module")
def test_user(app):
    create_user(app.db, 'test', 'test')
    yield AUTH_HEADER
    with app.db.session as s:
        u = s.query(User).filter(User.username == 'test').first()
        s.delete(u)
