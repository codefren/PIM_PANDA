from flask import Blueprint
#from flask_cors import CORS

main_pim_bp = Blueprint('main_pim', __name__, template_folder='templates')

#CORS(main_pim_bp)
from . import routes