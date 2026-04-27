from flask import Blueprint
#from flask_cors import CORS

edit_attributes_bp = Blueprint('edit_attributes', __name__, template_folder='templates')

#CORS(edit_attributes_bp)
from . import routes