from flask import request, current_app, send_file
from flask_jwt_extended import jwt_required, get_jwt_identity
from ..model_attribute import Attribute
from . import edit_attributes_bp
from ..create_attributes.forms import CreateAttributeForm
from datetime import datetime
from .. import set_db


@edit_attributes_bp.route('/edit_attribute/', methods=['POST'])
@jwt_required()
def edit_attribute():
    db = current_app.config.get('DB_CLIENTS')
    if not db:
        user_id = get_jwt_identity()
        db = set_db(user_id)
        if not db:
            current_app.logger.error(
                f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Error with db, connection not setted \ {current_app.config.get('DB_CLIENTS','None')}")
            return {'status': 'error', 'message': 'No database connection'},500
    current_app.logger.info(f"{datetime.now().strftime('%d/%m/%Y')} Se ha pedido editar el atributo {request.json.get('id')}")
    old_attr = Attribute.get_attribute_by_id(db, request.json.get('id'))
    if not old_attr:
        return {'status':'error', 'message': 'Attribute not found'}
    attr_form = CreateAttributeForm(request.json)
    message = {'message': {'name_set':False, 'description_set':False, 'categories_set':False, 'ismultiselectable_set':False,
                 'associated_marketplaces_set':False, 'associated_families_set':False, 'attribute_saved':False}}
    try:
        old_attr.set_name(attr_form.get_name())
        message['message']['name_set'] = True
        old_attr.set_description(attr_form.get_description())
        message['message']['description_set'] = True
        old_attr.set_categories(attr_form.get_categories())
        message['message']['categories_set'] = True
        old_attr.set_multiselectability(attr_form.get_ismultiselectable())
        message['message']['ismultiselectable_set'] = True
        old_attr.set_associated_marketplaces(attr_form.get_associated_marketplaces())
        message['message']['associated_marketplaces_set'] = True
        families = attr_form.get_associated_families()
        for i,family in enumerate(families):
           families[i] = (str(family).split(',')[0].strip(),str(family).split(',')[1].strip())
        old_attr.set_associated_families(families)
        message['message']['associated_families_set'] = True
        old_attr.set_BulletPoint(attr_form.get_BulletPoint())
        message['message']['BulletPoint_set'] = True
        old_attr.save_overwrite_attribute()
        message['message']['attribute_saved'] = True
    except Exception as e:
        current_app.logger.error(f"{datetime.now().strftime('%d/%m/%Y')} Error editando atributo {request.json.get('id')}, message: {message} con formulario {attr_form} \n Error {e}")
        message_error = ''
        for cat,r in message['message'].items():
            if r is False:
                message_error += f"{cat} "
        return {'status':'error','message':'Error in the next objects: ' + message_error}
    current_app.logger.info(f"{datetime.now().strftime('%d/%m/%Y')} Exito editando el atributo {request.json.get('id')}")
    return {'status':'ok','message':'Attribute edited succesfully'}

@edit_attributes_bp.route('/export_attributes/', methods=['POST'])
@jwt_required()
def export_attributes():
    db = current_app.config.get('DB_CLIENTS')
    if not db:
        user_id = get_jwt_identity()
        db = set_db(user_id)
        if not db:
            current_app.logger.error(
                f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Error with db, connection not setted \ {current_app.config.get('DB_CLIENTS', 'None')}")
            return {'status': 'error', 'message': 'No database connection'}, 500

    current_app.logger.info(f"{datetime.now().strftime('%d/%m/%Y')} Se ha pedido exportar los atributos")
    attributes = Attribute.get_all_attributes(db)
    if not attributes:
        return {'status':'error', 'message': 'No attributes found'}

    try:
        with open('attributes_export.csv', 'w') as f:
            f.write('id,name,description,categories,ismultiselectable,associated_marketplaces,associated_families,BulletPoint\n')
            for attr in attributes:
                f.write(f"{attr.get_id()},{attr.get_name()},{attr.get_description()},{attr.get_categories()},{attr.get_multiselectability()},{attr.get_associated_marketplaces()},{attr.get_associated_families()},{attr.get_BulletPoint()}\n")
    except Exception as e:
        current_app.logger.error(f"{datetime.now().strftime('%d/%m/%Y')} Error exportando atributos, message: {e}")
        return {'status':'error','message':'Error exporting attributes'}

    current_app.logger.info(f"{datetime.now().strftime('%d/%m/%Y')} Exito exportando los atributos")
    return send_file('attributes_export.csv', as_attachment=True, download_name='attributes_export.csv')
