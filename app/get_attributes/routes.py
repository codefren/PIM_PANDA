from flask import request, current_app
from flask_jwt_extended import jwt_required, get_jwt_identity
from ..model_attribute import Attribute
from . import get_attributes_bp
from datetime import datetime
from .. import set_db

@get_attributes_bp.route('/get_families/', methods=['GET'])
@jwt_required()
def get_families():
    db = current_app.config.get('DB_CLIENTS')
    if not db:
        user_id = get_jwt_identity()
        db = set_db(user_id)
        if not db:
            current_app.logger.error(
                f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Error with db, connection not setted \ {current_app.config.get('DB_CLIENTS','None')}")
            return {'status': 'error', 'message': 'No database connection'},500
    families = Attribute.get_families(db)
    current_app.logger.info(f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Families obtained succesfully")
    return {'status':'ok','message':families}

@get_attributes_bp.route('/get_marketplaces/', methods=['GET'])
@jwt_required()
def get_marketplaces():
    db = current_app.config.get('DB_CLIENTS')
    if not db:
        user_id = get_jwt_identity()
        db = set_db(user_id)
        if not db:
            current_app.logger.error(
                f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Error with db, connection not setted \ {current_app.config.get('DB_CLIENTS','None')}")
            return {'status': 'error', 'message': 'No database connection'},500
    marketplaces = Attribute.get_marketplaces(db)
    current_app.logger.info(f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Markets obtained succesfully")
    return {'status':'ok','message':marketplaces}

@get_attributes_bp.route('/get_families_and_marketplaces/',methods=['GET'])
@jwt_required()
def get_families_and_markets():
    db = current_app.config.get('DB_CLIENTS')
    if not db:
        user_id = get_jwt_identity()
        db = set_db(user_id)
        if not db:
            current_app.logger.error(
                f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Error with db, connection not setted \ {current_app.config.get('DB_CLIENTS','None')}")
            return {'status': 'error', 'message': 'No database connection'},500
    marketplaces = Attribute.get_marketplaces(db)
    families = Attribute.get_families(db)
    current_app.logger.info(f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Families and markets obtained succesfully")
    return {'status':'ok','message':{'families':families,'marketplaces':marketplaces}}

@get_attributes_bp.route('/get_all_attributes/', methods=['GET','POST'])
@jwt_required()
def get_attributes():
    db = current_app.config.get('DB_CLIENTS')
    if not db:
        user_id = get_jwt_identity()
        db = set_db(user_id)
        if not db:
            current_app.logger.error(
                f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Error with db, connection not setted \ {current_app.config.get('DB_CLIENTS','None')}")
            return {'status': 'error', 'message': 'No database connection'},500
    attributes = Attribute.get_all_attributes(db)
    current_app.logger.info(f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} All attributes obtained succesfully")
    return {'status':'ok','message':[attr.get_json_attribute() for attr in attributes]}


@get_attributes_bp.route('/get_attribute_by_id/', methods=['POST'])
@jwt_required()
def get_attribute_by_id():
    db = current_app.config.get('DB_CLIENTS')
    if not db:
        user_id = get_jwt_identity()
        db = set_db(user_id)
        if not db:
            current_app.logger.error(
                f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Error with db, connection not setted \ {current_app.config.get('DB_CLIENTS','None')}")
            return {'status': 'error', 'message': 'No database connection'},500
    attrid = str(request.json.get('id')).zfill(8)
    attr = Attribute.get_attribute_by_id(db, attrid)
    current_app.logger.info(f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Attributes by id {attrid} obtained succesfully")
    return {'status':'ok','message':[attr.get_json_attribute()]}


@get_attributes_bp.route('/get_attribute_by_name/', methods=['POST'])
@jwt_required()
def get_attribute_by_name():
    db = current_app.config.get('DB_CLIENTS')
    if not db:
        user_id = get_jwt_identity()
        db = set_db(user_id)
        if not db:
            current_app.logger.error(
                f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Error with db, connection not setted \ {current_app.config.get('DB_CLIENTS','None')}")
            return {'status': 'error', 'message': 'No database connection'},500
    attributes = Attribute.get_attributes_by_name(db, request.json.get('id'))
    current_app.logger.info(f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Attributes by name {request.json.get('id')} obtained succesfully")
    return {'status':'ok','message':[attr.get_json_attribute() for attr in attributes]}


@get_attributes_bp.route('/get_attribute_by_productid/', methods=['POST'])
@jwt_required()
def get_attributes_by_productid():
    db = current_app.config.get('DB_CLIENTS')
    if not db:
        user_id = get_jwt_identity()
        db = set_db(user_id)
        if not db:
            current_app.logger.error(
                f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Error with db, connection not setted \ {current_app.config.get('DB_CLIENTS','None')}")
            return {'status': 'error', 'message': 'No database connection'},500

    productid = request.json.get('id')
    attributes = Attribute.get_attributes_by_product(db, productid)

    for attr in attributes: attr.select_categories(productid=productid)
    current_app.logger.info(f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Attributes by product id {productid} obtained succesfully")
    return {'status':'ok','message':[attr.get_json_attribute() for attr in attributes]}


@get_attributes_bp.route('/get_attribute_by_familyid/', methods=['POST'])
@jwt_required()
def get_attributes_by_familyid():
    db = current_app.config.get('DB_CLIENTS')
    if not db:
        user_id = get_jwt_identity()
        db = set_db(user_id)
        if not db:
            current_app.logger.error(
                f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Error with db, connection not setted \ {current_app.config.get('DB_CLIENTS','None')}")
            return {'status': 'error', 'message': 'No database connection'},500
    familyid = str(request.json.get('id')).zfill(4)
    attributes = Attribute.get_attributes_by_familyid(db, familyid)
    current_app.logger.info(f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Attributes by family id {familyid} obtained succesfully")
    return {'status':'ok','message':[attr.get_json_attribute() for attr in attributes]}

@get_attributes_bp.route('/get_attribute_by_familyname/', methods=['POST'])
@jwt_required()
def get_attributes_by_familyname():
    db = current_app.config.get('DB_CLIENTS')
    if not db:
        user_id = get_jwt_identity()
        db = set_db(user_id)
        if not db:
            current_app.logger.error(
                f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Error with db, connection not setted \ {current_app.config.get('DB_CLIENTS','None')}")
            return {'status': 'error', 'message': 'No database connection'},500
    attributes = Attribute.get_attributes_by_familyname(db, request.json.get('id'))
    if attributes:
        current_app.logger.info(f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Attributes by family name {request.json.get('id')} obtained succesfully")
        return {'status':'ok','message':[attr.get_json_attribute() for attr in attributes]}
    return {'status':'ok','message':[]}


@get_attributes_bp.route('/get_attribute_by_marketplaceid/', methods=['POST'])
@jwt_required()
def get_attributes_by_marketplaceid():
    db = current_app.config.get('DB_CLIENTS')
    if not db:
        user_id = get_jwt_identity()
        db = set_db(user_id)
        if not db:
            current_app.logger.error(
                f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Error with db, connection not setted \ {current_app.config.get('DB_CLIENTS','None')}")
            return {'status': 'error', 'message': 'No database connection'},500
    attributes = Attribute.get_attributes_by_marketplace(db, request.json.get('id'))
    current_app.logger.info(f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Attributes by marketplace {request.json.get('id')} obtained succesfully")
    return {'status':'ok','message':[attr.get_json_attribute() for attr in attributes]}

'''
@get_attributes_bp.route('/find_attributes', methods=['POST'])
@jwt_required()
def find_attributes(): 
    db = current_app.config.get('DB_CLIENTS')
    if not db:
        return {'status':'error','message':'No database connection'}
    
    search_field = request.json['id']
'''