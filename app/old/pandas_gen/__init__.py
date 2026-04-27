from flask import Blueprint
#from flask_cors import CORS

pandas_gen_bp = Blueprint('pandas_gen', __name__, template_folder='templates')

#CORS(stats_bp)
from . import routes
