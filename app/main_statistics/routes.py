from datetime import datetime

from flask import request, current_app
from flask_jwt_extended import jwt_required,get_jwt_identity
from ..model_attribute import Attribute
from . import stats_bp
from .. import set_db


@stats_bp.route('/get_main_stats/', methods=['GET'])
@jwt_required()
def get_main_stats():
    db = current_app.config.get('DB_CLIENTS')
    if not db:
        user_id = get_jwt_identity()
        
        db = set_db(user_id)
        if not db:
            
            current_app.logger.error(
                f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Error with db, connection not setted \ {current_app.config.get('DB_CLIENTS','None')}")
            return {'status': 'error', 'message': 'No database connection'},500
    db.connect()
    try:
        return {'status':'ok','message':list(Attribute.get_main_stats(db).values())}
    except Exception as e:
        current_app.logger.error(
            f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Error with main_stats: {str(e)}")
        return {'status':'error','message':str(e)},500
