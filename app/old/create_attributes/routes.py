from flask import request, current_app
from flask_jwt_extended import jwt_required, get_jwt_identity
from ..model_attribute import Attribute
from . import create_attributes_bp
from .forms import CreateAttributeForm
from .. import set_db
from datetime import datetime



@create_attributes_bp.route('/create_attribute/',methods=['POST'])
@jwt_required()
def create_attribute():
    db = current_app.config.get('DB_CLIENTS')
    if not db:
        user_id = get_jwt_identity()
        db = set_db(user_id)
        if not db:
            current_app.logger.error(
                f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Error with db, connection not setted \ {current_app.config.get('DB_CLIENTS','None')}")
            return {'status': 'error', 'message': 'No database connection'},500
    message = {'valid_id_obtained': False, 'attribute_properties_obtained': False, 'attribute_created': False,
               'attribute_saved': False}
    #try:
    id_attr = Attribute.obtain_valid_id(db)
    message['valid_id_obtained'] = True
    attr_form = CreateAttributeForm(request.json)
    message['attribute_properties_obtained'] = True
    
    ismultiselectable = False if not attr_form.get_ismultiselectable() else True
    BulletPoint = False if not attr_form.get_BulletPoint() else True

    ass_families = []
    for family in attr_form.get_associated_families(): ass_families.append((family[0],family[1]))
    attr_form.associated_families = ass_families
    
    attr = Attribute(db,id_attr,attr_form.get_name(),attr_form.get_description(),attr_form.get_categories(),
                        ismultiselectable,attr_form.get_associated_marketplaces(),
                        attr_form.get_associated_families(),BulletPoint=attr_form.get_BulletPoint())
    message['attribute_created'] = True
    attr.save_overwrite_attribute()
    message['attribute_saved'] = True
    #except:
    #    return {'status':'error','message':message}
    return {'status':'ok','message':attr.get_json_attribute()}


@create_attributes_bp.route('/create_marketplace/',methods=['POST'])
@jwt_required()
def create_marketplace():
    db = current_app.config.get('DB_CLIENTS')
    if not db:
        user_id = get_jwt_identity()
        db = set_db(user_id)
        if not db:
            current_app.logger.error(
                f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Error with db, connection not setted \ {current_app.config.get('DB_CLIENTS','None')}")
            return {'status': 'error', 'message': 'No database connection'},500

    marketname = request.json.get('name')

    q = "insert into tbdmarketplaces(fldmarketplace) values (?)"
    db.execute(q,[marketname])
    db.commit()
    return {'status':'ok','message':'Marketplace inserted'}



