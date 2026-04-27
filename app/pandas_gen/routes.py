import json
import traceback
from datetime import datetime

import pandas as pd
from flask import request, current_app, send_file, session
from flask_jwt_extended import jwt_required, get_jwt_identity
from .model_panda import PandasGen
from ..model_traductor import Traductor
from . import pandas_gen_bp
from .. import set_db


@pandas_gen_bp.route('/get_header_data/', methods=['GET'])
@jwt_required()
def get_header_data():
    current_app.logger.info(f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} GET /get_header_data/ - user: {get_jwt_identity()}")
    db = current_app.config.get('DB_CLIENTS')
    if not db:
        user_id = get_jwt_identity()
        db = set_db(user_id)
        if not db:
            current_app.logger.error(
                f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Error with db, connection not setted - {current_app.config.get('DB_CLIENTS', 'None')}")
            return {'status': 'error', 'message': 'No database connection'}, 500
    db.connect()
    try:
        pag = PandasGen(db)
        data = {
            'Proveedor': pag.get_proveedores(),
            'Familia': pag.get_familias(),
            'Subfamilia': pag.get_subfamilias(),
            'Temporada': pag.get_temporadas(),
            'Subtemporada': pag.get_subtemporadas(),
            'Genero': pag.get_generos(),
            'Marca': pag.get_marcas()
        }
        current_app.logger.info(f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} GET /get_header_data/ - completed successfully")
        return {'status': 'ok', 'message': data}, 200
    except Exception as e:
        current_app.logger.error(f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} GET /get_header_data/ - error: {e}")
        return {'status': 'error', 'message': str(e)}, 500


@pandas_gen_bp.route('/get_csv_pandas_aboutyou/', methods=['POST'])
@jwt_required()
def generate_pandas_aboutyou():
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
    current_app.logger.info(f'{ahora} - Generating pandas')
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
    try:
        ahora = datetime.now().strftime('%d/%m/%Y - %H:%M:%S')
        current_app.logger.info(f'{ahora} - Cargando temporal')
        pag.carga_temporal(articulo_inicial=articulo_inicial, articulo_final=articulo_final, proveedor=proveedor,
                           familia=familia, subfamilia=subfamilia, temporada=temporada, subtemporada=subtemporada,
                           genero=genero, marca=marca)
        ahora = datetime.now().strftime('%d/%m/%Y - %H:%M:%S')
        current_app.logger.info(f'{ahora} - Generando pandas')
        df = pag.generate_pandas_aboutyou(current_app)
        if type(df) == str:
            ahora = datetime.now().strftime('%d/%m/%Y - %H:%M:%S')
            current_app.logger.info(f'{ahora} - Error generando pandas - {df}')
            return {'status': 'err', 'message': df}
        ahora = datetime.now().strftime('%d/%m/%Y - %H:%M:%S')
        current_app.logger.info(f'{ahora} - Panda Generado y guardando en csv')
        csv_name = f"pandas_generated\pandas_aboutyou.csv"
        df.to_csv('app\\' + csv_name, encoding='UTF-8', sep=';', index=False)

        if request.json.get('FTP'):
            with open("config_sftp.json", "r") as read_file:
                config = json.load(read_file)
            fa = datetime.now()
            sent = PandasGen.send_ftp_file('app\\' + csv_name, config['hostname'], config['username'],
                                           config['password'],
                                           f'/in/PANDA_{fa.strftime("%Y%m%d")}_{fa.strftime("%H%M%S")}.csv',
                                           config['port'])
            if not sent:
                return {'status': 'err', 'message': 'Error sending file to FTP'}

        return send_file(csv_name, download_name='pandas_aboutyou.csv')

    except Exception as e:
        current_app.logger.error(
            f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Error generating pandas: {str(e)} \n traceback: {traceback.format_exc()}")
        return {'status': 'error', 'message': str(e)}, 500


@pandas_gen_bp.route('/get_csv_pandas_zalando/', methods=['POST'])
@jwt_required()
def generate_pandas_zalando():
    db = current_app.config.get('DB_CLIENTS')
    if not db:
        user_id = get_jwt_identity()

        db = set_db(user_id)
        if not db:
            current_app.logger.error(
                f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Error with db, connection not setted \ {current_app.config.get('DB_CLIENTS', 'None')}")
            return {'status': 'error', 'message': 'No database connection'}, 500
    db.connect()
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
    try:
        pag.carga_temporal(articulo_inicial=articulo_inicial, articulo_final=articulo_final, proveedor=proveedor,
                           familia=familia, subfamilia=subfamilia, temporada=temporada, subtemporada=subtemporada,
                           genero=genero, marca=marca)
        df = pag.generate_pandas_zalando(current_app)
        if type(df) == str:
            return {'status': 'err', 'message': df}
        csv_name = f"pandas_generated\pandas_zalando.csv"
        df.to_csv('app\\' + csv_name, encoding='UTF-8', sep=';', index=False)

        if request.json.get('FTP'):
            with open("config_sftp.json", "r") as read_file:
                config = json.load(read_file)
            fa = datetime.now()
            sent = PandasGen.send_ftp_file('app\\' + csv_name, config['hostname'], config['username'],
                                           config['password'],
                                           f'/in/PANDA_{fa.strftime("%Y%m%d")}_{fa.strftime("%H%M%S")}.csv',
                                           config['port'])
            if not sent:
                return {'status': 'err', 'message': 'Error sending file to FTP'}

        return send_file(csv_name, download_name='pandas_zalando.csv')

    except Exception as e:
        current_app.logger.error(
            f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Error generating pandas: {str(e)} \n traceback: {traceback.format_exc()}")
        return {'status': 'error', 'message': str(e)}, 500


@pandas_gen_bp.route('/get_csv_pandas_limango/', methods=['POST'])
@jwt_required()
def generate_pandas_limango():
    db = current_app.config.get('DB_CLIENTS')
    if not db:
        user_id = get_jwt_identity()

        db = set_db(user_id)
        if not db:
            current_app.logger.error(
                f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Error with db, connection not setted \ {current_app.config.get('DB_CLIENTS', 'None')}")
            return {'status': 'error', 'message': 'No database connection'}, 500
    db.connect()
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
    try:
        pag.carga_temporal(articulo_inicial=articulo_inicial, articulo_final=articulo_final, proveedor=proveedor,
                           familia=familia, subfamilia=subfamilia, temporada=temporada, subtemporada=subtemporada,
                           genero=genero, marca=marca)
        df = pag.generate_pandas_aboutyou(current_app)
        if type(df) == str:
            return {'status': 'err', 'message': df}
        csv_name = f"pandas_generated\pandas_limango.csv"
        df.to_csv('app\\' + csv_name, encoding='UTF-8', sep=';', index=False)

        if request.json.get('FTP'):
            with open("config_sftp.json", "r") as read_file:
                config = json.load(read_file)
            fa = datetime.now()
            sent = PandasGen.send_ftp_file('app\\' + csv_name, config['hostname'], config['username'],
                                           config['password'],
                                           f'/in/PANDA_{fa.strftime("%Y%m%d")}_{fa.strftime("%H%M%S")}.csv',
                                           config['port'])
            if not sent:
                return {'status': 'err', 'message': 'Error sending file to FTP'}

        return send_file(csv_name, download_name='pandas_limango.csv')

    except Exception as e:
        current_app.logger.error(
            f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Error generating pandas: {str(e)} \n traceback: {traceback.format_exc()}")
        return {'status': 'error', 'message': str(e)}, 500


@pandas_gen_bp.route('/save_translation/', methods=['POST'])
@jwt_required()
def save_translation():
    db = current_app.config.get('DB_CLIENTS')
    if not db:
        user_id = get_jwt_identity()

        db = set_db(user_id)
        if not db:
            current_app.logger.error(
                f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Error with db, connection not setted \ {current_app.config.get('DB_CLIENTS', 'None')}")
            return {'status': 'error', 'message': 'No database connection'}, 500
    db.connect()
    traductor = Traductor(db=db)
    saved = traductor.save_translation(l_ori=request.json.get('I_ori'), l_dest=request.json.get('I_dest'),
                                       text_ori=request.json.get('text_ori'), text_dest=request.json.get('text_dest'))
    return {'status': 'ok', 'message': 'Translation saved'} if saved else {'status': 'err',
                                                                           'message': 'Error saving translation, Translation duplicated'}


@pandas_gen_bp.route('/overwrite_translation/', methods=['POST'])
@jwt_required()
def overwrite_translation():
    db = current_app.config.get('DB_CLIENTS')
    if not db:
        user_id = get_jwt_identity()

        db = set_db(user_id)
        if not db:
            current_app.logger.error(
                f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Error with db, connection not setted \ {current_app.config.get('DB_CLIENTS', 'None')}")
            return {'status': 'error', 'message': 'No database connection'}, 500
    db.connect()
    traductor = Traductor(db=db)
    saved = traductor.overwrite_translation(l_ori=request.json.get('idioma_origen'),
                                            l_dest=request.json.get('idioma_destino'),
                                            text_ori=request.json.get('texto_origen'),
                                            text_dest=request.json.get('texto_destino'))

    if not saved:
        return {'status': 'err', 'message': 'Error saving translation'}, 500

    if session.get('translations'):
        translations = traductor.query_translations(session.get('translations'))
        return {'status': 'ok', 'message': 'Translation saved', 'data': translations}, 200
    return {'status': 'ok', 'message': 'Translation saved'}, 200

@pandas_gen_bp.route('/delete_translation/', methods=['POST'])
@jwt_required()
def delete_translation():
    db = current_app.config.get('DB_CLIENTS')
    if not db:
        user_id = get_jwt_identity()

        db = set_db(user_id)
        if not db:
            current_app.logger.error(
                f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Error with db, connection not setted \ {current_app.config.get('DB_CLIENTS', 'None')}")
            return {'status': 'error', 'message': 'No database connection'}, 500
    db.connect()
    traductor = Traductor(db=db)
    try: dele = traductor.delete_translation(l_ori=request.json.get('idioma_origen'),
                                            l_dest=request.json.get('idioma_destino'),
                                            text_ori=request.json.get('texto_origen'),
                                            text_dest=request.json.get('texto_destino'))
    except Exception as e: dele = str(e)

    if dele is not True:
        return {'status': 'err', 'message': f'Error deleting translation: {dele}'}, 500

    if session.get('texts_translated'):
        register = {
            'idioma_origen': request.json.get('idioma_origen'),
            'idioma_destino': request.json.get('idioma_destino'),
            'texto_origen': request.json.get('texto_origen'),
            'texto_destino': request.json.get('texto_destino')
        }
        texts_translated = session.get('texts_translated')
        if register in texts_translated:
            texts_translated.remove(register)
        session['texts_translated'] = texts_translated

    return {'status': 'ok', 'message': 'Translation deleted'}, 200


@pandas_gen_bp.route('/save_batch_translation/', methods=['POST'])
@jwt_required()
def save_batch_translation():
    # Data must be a list of lists like [l_ori,l_dest,text_ori,text_dest]
    db = current_app.config.get('DB_CLIENTS')
    if not db:
        user_id = get_jwt_identity()

        db = set_db(user_id)
        if not db:
            current_app.logger.error(
                f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Error with db, connection not setted \ {current_app.config.get('DB_CLIENTS', 'None')}")
            return {'status': 'error', 'message': 'No database connection'}, 500
    db.connect()
    traductor = Traductor(db=db)
    data = request.json
    errors = []
    for d in data:
        saved = traductor.save_translation(l_ori=d['l_ori'],l_dest=d['l_dest'],
                               text_ori=d['text_ori'],text_dest=d['text_dest'])
        if not saved:
            errors.append([d['l_ori'],d['l_dest'],d['text_ori'],d['text_dest'],"Translation duplicated"])
    errors = [[None,None,None,None,None]] if not errors else errors
    df = pd.DataFrame(errors,columns=['l_ori','l_dest','text_ori','text_dest','error_reason'])
    csv_name = f"pandas_generated\errors_translations.csv"
    df.to_csv('app\\' + csv_name,encoding='UTF-8', sep=';', index=False)
    return send_file(csv_name,download_name='errors.csv')


@pandas_gen_bp.route('/get_translations/', methods=['GET','POST'])
@jwt_required()
def get_translations():
    if request.method == 'GET':
        return {'status': 'ok', 'message': 'Translations found', 'data': session.get('texts_translated')} if session.get('texts_translated') else {'status': 'err', 'message': 'No translations found'}
    db = current_app.config.get('DB_CLIENTS')
    if not db:
        user_id = get_jwt_identity()

        db = set_db(user_id)
        if not db:
            current_app.logger.error(
                f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Error with db, connection not setted \ {current_app.config.get('DB_CLIENTS', 'None')}")
            return {'status': 'error', 'message': 'No database connection'}, 500
    db.connect()
    traductor = Traductor(db=db)

    translations = traductor.query_translations(request.json.get('text'))
    session['translations'] = request.json.get('text') if translations else None
    session['texts_translated'] = translations if translations else None
    return {'status': 'ok', 'message': 'Translations found', 'data': translations} if translations else {
        'status': 'err', 'message': 'No translations found'}


@pandas_gen_bp.route('/upload_csv/', methods=['POST'])
@jwt_required()
def intelligent_translation():
    db = current_app.config.get('DB_CLIENTS')
    if not db:
        user_id = get_jwt_identity()

        db = set_db(user_id)
        if not db:
            current_app.logger.error(
                f"{datetime.now().strftime('%d/%m/%Y - %H:%M:%S')} Error with db, connection not setted \ {current_app.config.get('DB_CLIENTS', 'None')}")
            return {'status': 'error', 'message': 'No database connection'}, 500
    db.connect()
    traductor = Traductor(db=db)
    data = request.json.get('text')
    data = [r.replace('\r','') for r in data]
    l_ori = request.json.get('idioma_origen').upper()
    l_dest = [l.upper() for l in request.json.get('idioma_destino')]
    if not data:
        return {'status': 'err', 'message': 'No data found'}
    if not l_ori:
        return {'status': 'err', 'message': 'No origin language found'}
    if not l_dest:
        return {'status': 'err', 'message': 'No destination language found'}

    traductions = []
    print(data,l_ori,l_dest)
    for line in data:
        if line in ('',None,' '):
            continue
        traducts = {}
        traducts['text_ori'] = line
        traducts['l_ori'] = l_ori
        for l in l_dest:
            tr = traductor.translate_save(l_ori,l,line)
            traducts[l] = tr
        traductions.append(traducts)
    print(traductions)
    df = pd.DataFrame(traductions)
    csv_name = f"pandas_generated\intelligent_translation.csv"
    df.to_csv('app\\' + csv_name,encoding='UTF-8', sep=';', index=False)
    return send_file(csv_name,download_name='intelligent_translation.csv')
