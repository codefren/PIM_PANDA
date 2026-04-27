import os
from turtle import home
import pyodbc, logging
from flask import Flask, render_template, g, request, redirect, url_for, current_app, session, abort, jsonify
from flask_jwt_extended import (
    JWTManager, jwt_required, create_access_token,
    get_jwt_identity
)
from flask_jwt_extended import get_jwt
from flask_mail import Mail
#from flask_cors import CORS
from .config import server, bddatos, user, pwd, Secret_key, Mail_username, Mail_server, Mail_port, Mail_password, \
    Mail_use_tls, Mail_use_ssl, LANGUAGES, API_URL, server_clients,bddatos_clients
from .forms import LoginForm
from .model_db import Database
from logging.handlers import RotatingFileHandler
import datetime
from .model_user import User

username_db = user

def create_app():
    app = Flask(__name__)

    from .main_pim import main_pim_bp
    app.register_blueprint(main_pim_bp)
    from .get_attributes import get_attributes_bp
    app.register_blueprint(get_attributes_bp)
    from .edit_attributes import edit_attributes_bp
    app.register_blueprint(edit_attributes_bp)
    from .create_attributes import create_attributes_bp
    app.register_blueprint(create_attributes_bp)
    from .delete_attributes import delete_attributes_bp
    app.register_blueprint(delete_attributes_bp)
    from .main_statistics import stats_bp
    app.register_blueprint(stats_bp)
    from app.pandas_gen import pandas_gen_bp
    app.register_blueprint(pandas_gen_bp)


    # Setup logging
    handler = RotatingFileHandler('info_log.log', maxBytes=10000, backupCount=3)
    handler.setLevel(logging.INFO)
    app.logger.addHandler(handler)

    handler2 = RotatingFileHandler('error_log.log', maxBytes=10000, backupCount=3)
    handler2.setLevel(logging.ERROR)
    app.logger.addHandler(handler2)

    handler3 = RotatingFileHandler('critical_log.log', maxBytes=10000, backupCount=3)
    handler3.setLevel(logging.CRITICAL)
    app.logger.addHandler(handler3)

    #CORS(app)
    # JWT setup
    jwt = JWTManager(app)

    # App configuration
    app.config['SECRET_KEY'] = Secret_key
    app.config['MAIL_SERVER'] = Mail_server
    app.config['MAIL_PORT'] = Mail_port
    app.config['MAIL_USERNAME'] = Mail_username
    app.config['MAIL_PASSWORD'] = Mail_password
    app.config['MAIL_USE_TLS'] = Mail_use_tls
    app.config['MAIL_USE_SSL'] = Mail_use_ssl
    app.config[
        'JWT_SECRET_KEY'] = 'dc1eef81a751379480222f45ec26361ef543a6205e0f2f4a401a6f0d3076800b5053a128628ca6d14230f48c2e6d41c92dd98e5202f9fa3e35ab55d4e4ce8cce'
    app.config['JWT_BLACKLIST_ENABLED'] = True
    app.config['JWT_BLACKLIST_TOKEN_CHECKS'] = ['access']
    app.config['DB'] = Database(driver='ODBC Driver 18 for SQL Server', srv_name=server, db_name=bddatos, username=user,
                                password=pwd, trusted_connection=False)
    app.config['JWT'] = jwt
    app.config['JWT_ACCESS_TOKEN_EXPIRES'] = False

    app.template_folder = '.'

    app.config['UPLOAD_FOLDER'] = 'uploads'
    if not os.path.exists(app.config['UPLOAD_FOLDER']):
        os.makedirs(app.config['UPLOAD_FOLDER'])

    @app.route('/', defaults={'path': ''})
    @app.route('/<path:path>')
    def initialise(path):
        return render_template('index.html')
    

    @app.route('/login/', methods=['POST'])
    def login():
        db = current_app.config['DB']
        cursor = db.get_cursor()
        form = LoginForm(request.json)
        try:
            form.username = int(form.username)
        except:
            str(form.username)
        if type(form.username) is int:
            current_app.logger.info(f"{datetime.datetime.now().strftime('%d/%m/%Y')} Usuario con ID: {form.username} ha introducido credenciales")
            user_obj = User.get_by_id(cursor, form.username)
        else:
            current_app.logger.info(f"Usuario con Nombre: {form.username} ha introducido credenciales")
            if form.username is not None and User.query_id(cursor, str(form.username)) is not False:
                user_obj = User(name=form.username, password=form.password, cursor=cursor,
                            id=User.query_id(cursor, str(form.username)))
            else:
                user_obj = None

        if user_obj is not None and user_obj.exists() and user_obj.check_password(str(form.password)):
            access_token = create_access_token(identity=user_obj.get_id())
            user_obj.set_clients_db(server,user,pwd)
            app.config['DB_CLIENTS'] = user_obj.get_clients_db()
            #app.config['DB_CLIENTS'] = Database(driver='ODBC Driver 18 for SQL Server', srv_name=server, db_name='S4T_KOROSHI_2', username=username_db,
            #                    password=pwd, trusted_connection=False)
            if user_obj.is_admin():
                current_app.logger.info(f"{datetime.datetime.now().strftime('%d/%m/%Y')} Usuario con ID: {user_obj.get_id()} ha iniciado sesión como administrador")
                return jsonify(status='ok',access_token=access_token,is_admin=True,username=form.username), 200
            current_app.logger.info(f"{datetime.datetime.now().strftime('%d/%m/%Y')} Usuario con ID: {user_obj.get_id()} ha iniciado sesión como usuario base")
            return jsonify(status='ok',access_token=access_token,is_admin=False,username=form.username), 200

        current_app.logger.info(f"{datetime.datetime.now().strftime('%d/%m/%Y')} Usuario con username {form.username} ha introducido credenciales incorrectas")
        return jsonify({"status":"error","message": "Bad username or password"}), 401



    @app.route('/protected/', methods=['GET'])
    @jwt_required()
    def protected():
        # Access the identity of the current user with get_jwt_identity
        current_user = get_jwt_identity()
        app.logger.info(f'Access attempt by user: {current_user}')
        return jsonify(logged_in_as=current_user,message='exit'), 200



    return app


def set_db(user_id):
    try:
        current_app.config['DB_CLIENTS'] = Database(driver='ODBC Driver 18 for SQL Server', srv_name=server, db_name='S4T_KOROSHI', username=username_db,
                                    password=pwd, trusted_connection=False)
        db = current_app.config.get('DB_CLIENTS')
        db.connect()
        #user_obj = User.get_by_id(db.get_cursor(),user_id)
        #app.config['DB_CLIENTS'] = user_obj.get_clients_db()
        return db
    except Exception as e:
        current_app.logger.error(str(e))
        return None

    
