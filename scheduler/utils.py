from .models.models import User
from cdispyutils.log import get_logger
import bcrypt
import hashlib
import json
import uuid

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

def get_payload_hash(payload):
    pay_str = json.dumps(payload, sort_keys=True) 
    sha = hashlib.sha256(pay_str).hexdigest()
    return sha

def uuidstr(): 
    return str(uuid.uuid4()) 
