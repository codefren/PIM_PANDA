from flask import Blueprint
#from flask_cors import CORS

create_attributes_bp = Blueprint('create_attributes', __name__, template_folder='templates')

#CORS(create_attributes_bp)
from . import routes