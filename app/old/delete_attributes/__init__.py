from flask import Blueprint
#from flask_cors import CORS

delete_attributes_bp = Blueprint('delete_attributes', __name__, template_folder='templates')

#CORS(delete_attributes_bp)
from . import routes