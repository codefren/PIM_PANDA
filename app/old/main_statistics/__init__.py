from flask import Blueprint
#from flask_cors import CORS

stats_bp = Blueprint('stats', __name__, template_folder='templates')

#CORS(stats_bp)
from . import routes
