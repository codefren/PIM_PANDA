import os
from dotenv import load_dotenv

load_dotenv()

server = os.environ['DB_SERVER']
bddatos = os.environ['DB_NAME']
user = os.environ['DB_USER']
pwd = os.environ['DB_PASSWORD']

server_clients = os.environ['DB_SERVER_CLIENTS']
bddatos_clients = os.environ['DB_NAME_CLIENTS']
user_clients = os.environ['DB_USER_CLIENTS']
pwd_clients = os.environ['DB_PASSWORD_CLIENTS']

Secret_key = os.environ['SECRET_KEY']
JWT_secret_key = os.environ['JWT_SECRET_KEY']

LANGUAGES = {
    'en': 'English',
    'fr': 'French',
    'es': 'Español',
    'pt': 'Portuguese',
    'ca': 'Català'
}
VERIFY_METHOD = ['EMAIL']
PARAMS = {
    'VERIFY_METHOD': 'EMAIL'
}
API = [False]
API_URL = "http://82.223.50.229:2022"
TOKEN = os.environ.get('API_TOKEN', '')

Mail_server = os.environ['MAIL_SERVER']
Mail_port = int(os.environ['MAIL_PORT'])
Mail_username = os.environ['MAIL_USERNAME']
Mail_password = os.environ['MAIL_PASSWORD']
Mail_use_tls = os.environ['MAIL_USE_TLS'].lower() == 'true'
Mail_use_ssl = os.environ['MAIL_USE_SSL'].lower() == 'true'
Mail_remitente = os.environ['MAIL_USERNAME']

Link_verifier = 'http://clientreg.soft4tex.net:9092'
LINK_LOGO = [" "]
