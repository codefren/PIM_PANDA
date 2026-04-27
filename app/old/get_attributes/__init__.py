from flask import Blueprint
#from flask_cors import CORS

get_attributes_bp = Blueprint('get_attributes', __name__, template_folder='templates')

#CORS(get_attributes_bp)
from . import routes