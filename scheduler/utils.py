from .models.models import User
from cdispyutils.log import get_logger
import bcrypt

logger = get_logger('utils')

def create_user(db, username, password):
    try:
        with db.session as s:
            u = User(username=username, password=bcrypt.hashpw(password, bcrypt.gensalt()))
            s.add(u)
            return True
    except:
        logger.exception("Fail to create user")
        return False