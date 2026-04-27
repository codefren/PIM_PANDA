from ast import Delete
from datetime import datetime

from flask import request, current_app
from flask_jwt_extended import jwt_required, get_jwt_identity
from ..model_attribute import Attribute
from . import delete_attributes_bp
from .. import set_db

@delete_attributes_bp.route('/delete_attribute/', methods=['POST'])
@jwt_required()
def delete_attribute():
    db = current_app.config.get('DB_CLIENTS')
    if not db:
        user_id = get_jwt_identity()
        db = set_db(user_id)
        if not db:
            current_app.logger.error(
                f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Error with db, connection not setted \ {current_app.config.get('DB_CLIENTS','None')}")
            return {'status': 'error', 'message': 'No database connection'},500
    try:
        
        attr = Attribute.get_attribute_by_id(db, request.json['id'])
        
        if not attr:
            return {'status':'error', 'message': 'Attribute not found'}
        attr.delete_attribute()
        return {'status':'ok','message':'Attribute deleted'}
    except Exception as e:
        current_app.logger.error(str(e))
        current_app.logger.info("papa" + str(request.json.get('id')))
        return {'status':'error','message':f'Error deleting attribute, {e}'}



