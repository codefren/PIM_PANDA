from flask import request, current_app, send_file
from flask_jwt_extended import jwt_required, get_jwt_identity
from ..model_attribute import Attribute
from ..model_product import Product
from . import main_pim_bp
from .. import set_db
from datetime import datetime

from app.pandas_gen.model_panda import PandasGen


@main_pim_bp.route('/obtain_product_info/', methods=['GET','POST'])
@jwt_required()
def obtain_product_info():
    """
        :type productid: string
        :return: status and message of the request
        Obtains all the products and its attributes information from the database based on the productid
        """

    db = current_app.config.get('DB_CLIENTS')
    if not db:
        user_id = get_jwt_identity()
        db = set_db(user_id)
        if not db:
            current_app.logger.error(
                f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Error with db, connection not setted \ {current_app.config.get('DB_CLIENTS','None')}")
            return {'status': 'error', 'message': 'No database connection'},500
    productid = request.json.get('id')
    colorid = request.json.get('color') if request.json.get('color') != 'null' else None
    product = Product.get_by_id(db, productid)
    if not product:
        return {'status': 'error', 'message': 'Product not found'}
    if not colorid:
        if not product.get_colors():
            return {'status': 'error', 'message': 'Product has no colors'}
        product_attributes = Attribute.get_attribute_by_product_and_all_colors(db, productid,[color[0] for color in product.get_colors()])
    else:
        product_attributes = Attribute.get_attributes_by_product_and_color(db, productid, colorid)
    for attr in product_attributes: attr.select_categories(productid=productid,colorid=colorid,colors=[color[0] for color in product.get_colors()])
    product.set_attributes(product_attributes)

    family_attributes = Attribute.get_attributes_by_familyid(db, product.get_familyid())
    for attr in family_attributes: attr.select_categories(productid=productid,colorid=colorid,colors=[color[0] for color in product.get_colors()])

    json_product = product.get_json()
    selected_attributes = {}
    for attr in product_attributes:
        if attr.get_selected_categories():
            selected_attributes[attr.get_id()] = attr.get_json_attribute()

    candidate_attributes = {}
    for attr in family_attributes:
        print(attr.get_id().strip(),selected_attributes.keys())
        if attr.get_id().strip() not in selected_attributes.keys():
            candidate_attributes[attr.get_id().strip()] = attr.get_json_attribute()

    message = {productid: {
        'name': json_product['description'],
        'family': json_product['family'],
        'idfamily':json_product['idfamily'],
        'gender': json_product['gender'],
        'colors': json_product['colors'],
        'sizes': json_product['sizes'],
        'selected_attributes': selected_attributes,
        'candidated_attributes': candidate_attributes,
        'images': json_product['images']
    }}

    return {'status': 'ok', 'message': message}


@main_pim_bp.route('/edit_product_attributes/', methods=['POST'])
@jwt_required()
def edit_product_attributes():
    """
    :type productid: string / colorid: string / attributes_added: dictionary / attributes_deleted: dictionary / attributes_modified: dictionary
    :return: status and message of the request
    Edits the attributes of a product in the database
    """
    current_user = get_jwt_identity()
    db = current_app.config.get('DB_CLIENTS')
    if not db:
        user_id = get_jwt_identity()
        db = set_db(user_id)
        if not db:
            current_app.logger.error(
                f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Error with db, connection not setted \ {current_app.config.get('DB_CLIENTS','None')}")
            return {'status': 'error', 'message': 'No database connection'},500
    productid = request.json.get('idarticulo')
    colorid = request.json.get('color') if request.json.get('color') != 'null' else None
    product = Product.get_by_id(db, productid)
    if not product:
        return {'status': 'error', 'message': 'Product not found'}

    product_attributes = Attribute.get_attributes_by_product(db, productid)
    for attr in product_attributes: attr.select_categories(productid=productid,colorid=colorid,colors=[color[0] for color in product.get_colors()]), attr.set_selected_color(colorid)
    product.set_attributes(product_attributes)

    attributes_added = request.json.get('atributes_added') if request.json.get('atributes_added') else {}
    attributes_deleted = request.json.get('atributes_deleted') if request.json.get('atributes_deleted') else {}
    attributes_modified = request.json.get('atributes_modified') if request.json.get('atributes_modified') else {}

    for idattr,attr in attributes_deleted.items():
        attribute = Attribute.get_attribute_by_id(db, idattr)
        attribute.set_selected_color(colorid)
        for cat in attr.get('categories_eliminated'):
            attribute.deselect_category(category=cat,productid=productid)

    for idattr,attr in attributes_added.items():
        attribute = Attribute.get_attribute_by_id(db, idattr)
        attribute.set_selected_color(colorid)
        for cat in attr.get('categories_added'):
            attribute.select_category_product(category=cat,productid=productid,colors=product.get_colors())

    for idattr,attr in attributes_modified.items():
        attribute = Attribute.get_attribute_by_id(db, idattr)
        attribute.set_selected_color(colorid)
        for cat in attr.get('categories_eliminated'):
            attribute.deselect_category(category=cat,productid=productid)
        for cat in attr.get('categories_added'):
            attribute.select_category_product(category=cat,productid=productid,colors=product.get_colors())

    return {'status': 'ok', 'message': 'Product attributes updated successfully'}

@main_pim_bp.route('/export_attributes_products/', methods=['POST'])
@jwt_required()
def export_attributes_products():
    db = current_app.config.get('DB_CLIENTS')
    if not db:
        user_id = get_jwt_identity()

        db = set_db(user_id)
        if not db:
            current_app.logger.error(
                f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Error with db, connection not setted \ {current_app.config.get('DB_CLIENTS', 'None')}")
            return {'status': 'error', 'message': 'No database connection'}, 500
    db.connect()
    ahora = datetime.now().strftime('%d/%m/%Y - %H:%M:%S')
    current_app.logger.info(f'{ahora} - Exportando atributos con productos')

    pag = PandasGen(db)
    proveedor = request.json.get('Proveedor') if request.json.get('Proveedor') != '' else None
    familia = request.json.get('Familia') if request.json.get('Familia') != '' else None
    subfamilia = request.json.get('Subfamilia') if request.json.get('Subfamilia') != '' else None
    temporada = request.json.get('Temporada') if request.json.get('Temporada') != '' else None
    subtemporada = request.json.get('Subtemporada') if request.json.get('Subtemporada') != '' else None
    genero = request.json.get('Genero') if request.json.get('Genero') != '' else None
    marca = request.json.get('Marca') if request.json.get('Marca') != '' else None
    articulo_inicial = request.json.get('Articulo_inicial') if request.json.get('Articulo_inicial') != '' else None
    articulo_final = request.json.get('Articulo_final') if request.json.get('Articulo_final') != '' else None
    #try:

    ahora = datetime.now().strftime('%d/%m/%Y - %H:%M:%S')
    current_app.logger.info(f'{ahora} - Cargando temporal')

    file = pag.export_products_pim_attributes(articulo_inicial=articulo_inicial, articulo_final=articulo_final, proveedor=proveedor,
                       familia=familia, subfamilia=subfamilia, temporada=temporada, subtemporada=subtemporada,
                       genero=genero, marca=marca)

    csv_name = 'pandas_generated\\attributes_products.csv'
    file.to_csv('app\\'+csv_name, encoding='UTF-8', sep=';', index=False)

    return send_file(csv_name, download_name='pandas_aboutyou.csv')
    '''except Exception as e:
        current_app.logger.error(f'{ahora} - Error en la exportación de atributos con productos {str(e)}')
        return {'status': 'error', 'message': 'Error exporting attributes with products', 'err': str(e)}, 500'''






