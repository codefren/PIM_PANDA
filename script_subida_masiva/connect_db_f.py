import json
from model_db import Database

with open("config.json",encoding='UTF-8') as file:
    config = json.load(file)

server=config['server']
bddatos=config['bddatos']
user=config['user']
pwd=config['pwd']

def connect_db(server=server,bddatos=bddatos,user=user,pwd=pwd):
    if user == None and pwd == None:
        DB = Database(driver="{ODBC Driver 18 for SQL Server}", srv_name=server, db_name=bddatos,
                      username=user,
                      password=pwd, trusted_connection=True, encrypt=False)
    elif user != None or pwd != None:
        DB = Database(driver="{ODBC Driver 18 for SQL Server}", srv_name=server, db_name=bddatos,
                      username=user,
                      password=pwd, trusted_connection=False, encrypt=False)
    else:
        return False
    return DB
