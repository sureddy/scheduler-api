from flask import Flask
from .blueprint import base 
from auth import check_user 


app = Flask(__name__)
app.register_blueprint(base)

@app.before_request
def before_req():
    check_user()
