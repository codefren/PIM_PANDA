from flask_login import UserMixin
from werkzeug.exceptions import InternalServerError
from .model_db import Database
import bcrypt


class User(UserMixin):

    def __init__(self,name,password,cursor,id):
        self.url_api = ' '
        self.vm = 'EMAIL'
        self.api = False
        self.mail_attrs = {
            'SERVER': ' ',
            'PORT': ' ',
            'USERNAME':' ',
            'PASSWORD':' ',
            'TLS':False,
            'SSL':True,
            'SENDER':' '
        }
        self.password = self.hash_password(password)
        self.id = id
        self.name = name
        self.cursor = cursor
        self.mail = None
        self.admin = False
        self.link_logo = ' '
        self.image1 = ' '
        self.image2 = ' '
        self.clients_db = None
        self.set_mail_attrs()
        if id == None:
            self._get_set_id()

    def search_location(self,location:str):
        """
        :param location: a string with the location to search as a province, city or autonomous community
        :return:
        """
        q = f"select codpostal, lat, lon, poblacion, provincia, pais from codspostales where lower(poblacion) like '%{location.lower()}%'" \
            f" or lower(comunidad) like '%{location.lower()}%'"
        try:
            self.cursor.execute(q)
            return self.cursor.fetchall()
        except:
            return []

    def is_authenticated(self):
        if self.id is not None:
            q = f'Select is_authenticated as auth from users where id = {self.id}'
            self.cursor.execute(q)
            return self.cursor.fetchone()[0]

        q = f'Select is_authenticated as auth from users where nombre = {self.name}'
        self.cursor.execute(q)
        return self.cursor.fetchone()[0]
    
    def get_recompensas(self):
        q = f"select link_recompensas from Empresas where IDEmpresa = (select IDEmpresa from users where ID = {self.id})"
        try:
            self.cursor.execute(q)
            i = self.cursor.fetchone()
            if i == None:
                return ' '
            return i[0]
        except:
            return ' '
            
    def get_image(self, image=1,end=False):
        if not end:
            if image == 2:
                if self.image2 != ' ':
                    return self.image2
                q = f"select link_image_right from Empresas where IDEmpresa = (select IDEmpresa from users where ID = {self.id})"
            else:
                if self.image1 != ' ':
                    return self.image1
                q = f"select link_image_left from Empresas where IDEmpresa = (select IDEmpresa from users where ID = {self.id})"
        elif end:
            if image == 2:
                if self.image2 != ' ':
                    return self.image2
                q = f"select link_image_final_right from Empresas where IDEmpresa = (select IDEmpresa from users where ID = {self.id})"
            else:
                if self.image1 != ' ':
                    return self.image1
                q = f"select link_image_final_left from Empresas where IDEmpresa = (select IDEmpresa from users where ID = {self.id})"
        self.cursor.execute(q)
        i = self.cursor.fetchone()
        if i == None:
            return ' '
        if image == 1:
            self.image1 = i[0]
        else:
            self.image2 = i[0]
        return i[0]

    def get_api_url(self):
        q = f"select api from empresas where idempresa = (select idempresa from users where id = {self.id})"
        self.cursor.execute(q)
        res = self.cursor.fetchone()
        if res is not None:
            return res[0]
        return None
    def set_clients_db(self, server, user, pwd):
        q=f"select Database_ID from Empresas where IDEmpresa = (select IDEmpresa from users where ID = {self.id})"
        self.cursor.execute(q)
        bddatos = self.cursor.fetchone()[0]
        if user == None and pwd == None:
            DB = Database(driver="{ODBC Driver 18 for SQL Server}", srv_name=server, db_name=bddatos,
                                  username=user,
                                  password=pwd, trusted_connection=True, encrypt=False)
        elif user != None or pwd != None:
            DB = Database(driver="{ODBC Driver 18 for SQL Server}", srv_name=server, db_name=bddatos,
                                  username=user,
                                  password=pwd, trusted_connection=False, encrypt=False)
        else:
            raise InternalServerError('Malformed SQL server database values')
        self.clients_db = DB

    def get_clients_db(self):
        return self.clients_db

    def get_clients_cursor(self, server=None,user=None,pwd=None):
        if self.clients_db is None:
            if server is not None:
                self.set_clients_db(server=server,user=user,pwd=pwd)
            else:
                return None
        return self.clients_db.get_cursor()

    def get_logo(self):
        if self.link_logo == ' ':
            q = f"select Link_logo from Empresas where IDEmpresa = (select IDEmpresa from users where ID = {self.id})"
            self.cursor.execute(q)
            return self.cursor.fetchone()[0]
        return self.link_logo

    def change_auth(self,T):
        if T == 'True':
            s = 1
        else:
            s = 0
        if self.id is not None:
            q = f"Update users set is_authenticated = {s} where id = {self.id}"
        else:
            q = f"Update users set is_authenticated = {s} where Nombre = {self.name}"
        self.cursor.execute(q)
        self.cursor.commit()
        return True


    def _get_set_id(self):
        q = f"select ID from users where Nombre = '{self.name}'"
        self.cursor.execute(q)
        i = self.cursor.fetchone()
        if i[0] is not None:
            self.id = i[0]
            return None
        raise InternalServerError('Usuario inexistente')


    def hash_password(self, password):
        pwd = password.encode('utf-8')
        hashed_pwd = bcrypt.hashpw(pwd, bcrypt.gensalt())
        hashed_pwd = str(hashed_pwd)
        hashed_pwd = hashed_pwd[2:len(hashed_pwd) - 1]
        return hashed_pwd

    def check_password(self, password):
        '''
        password must be a string with pwd
        '''
        password = password.encode('utf-8')
        if self.id != None:
            q = f"select a.PWD from users a where a.ID = {self.id}"
            self.cursor.execute(q)
            i = self.cursor.fetchone()
            if i[0] is not None:
                i = i[0].encode('utf-8')
                return bcrypt.checkpw(password, i)
        else:
            q = f"select a.PWD from users a where a.Nombre = '{self.name}'"
            self.cursor.execute(q)
            i = self.cursor.fetchone()
            if i[0] is not None:
                i = i[0].encode('utf-8')
                return bcrypt.checkpw(password, i)
        raise InternalServerError(f'{self} no encontrado')

    def set_verify_method(self,vm):
        if vm not in ['TEL','EMAIL']:
            raise Exception('Se ha intentado modificar el método de verificación a uno no valido')
        self.vm = vm

    def set_api(self,api):
        if api:
            self.set_url_api(self.get_api_url())
        else:
            self.set_url_api('')
        self.api = api

    def set_url_api(self,url):
        if type(url) is not str:
            raise Exception('Se ha intentado poner una url de uan api en format no string')
        self.url_api = url

    def set_mail(self,mail):
        self.mail = mail

    def set_mail_attrs(self):
        q = f"select Mail_server as server, Mail_port as port, Mail_username as username, Mail_password as pwd," \
            f"tls as tls, ssl as ssl, remitente as remitente" \
            f" from correos_confirmacion where IDEmpresa = (select IDEmpresa from users where ID = {self.id})"
        self.cursor.execute(q)
        i = self.cursor.fetchone()
        if i is None:
            return False
        self.mail_attrs['SERVER'] = i[0]
        self.mail_attrs['PORT'] = i[1]
        self.mail_attrs['USERNAME'] = i[2]
        self.mail_attrs['PASSWORD'] = i[3]
        self.mail_attrs['TLS'] = i[4]
        self.mail_attrs['SSL'] = i[5]
        self.mail_attrs['SENDER'] = i[6]

    def set_id(self,id):
        try: int(id)
        except: return False
        self.id = int(id)

    def get_name(self):
        return self.name


    def set_admin(self,bol:bool):
        if bol == True:
            self.admin = 'yes'
        else:
            self.admin = 'no'

    def save(self):
        if id == None:
            q = f"Insert into users(Nombre,Mail,PWD,Is_admin) values('{self.name}','{self.mail}','{self.password}','{self.admin}')"
            self.cursor.execute(q)
            self.cursor.commit()
        else:
            q = f"Insert into users(ID,Nombre,Mail,PWD,Is_admin) values({self.id},'{self.name}','{self.mail}','{self.password}','{self.admin}')"
            self.cursor.execute(q)
            self.cursor.commit()

    @staticmethod
    def get_by_id(cursor,id:int):
        if type(id) is int:
            q = f"select * from users a where a.Id = {id}"
            cursor.execute(q)
            i = cursor.fetchone()
            if i is None:
                return None
            use = User(name=i[1], password=i[3], cursor=cursor,id=i[0])
            use.set_mail(i[2])
            if i[4] == 'yes':
                use.set_admin(True)
            else:
                use.set_admin(False)
            return use

        else:
            q = f"select * from users a where a.Nombre = '{id}'"
            cursor.execute(q)
            i = cursor.fetchone()
            if i is None:
                return None
            use = User(name=i[1], password=i[3], cursor=cursor, id=i[0])
            use.set_mail(i[2])
            if i[4] == 'yes':
                use.set_admin(True)
            else:
                use.set_admin(False)
            return use

    @staticmethod
    def get_by_email(cursor,email):
        q = f"select * from users a where a.Mail = '{email}'"
        cursor.execute(q)
        i = cursor.fetchone()
        if i is not None and i[0] is not None:
            return User(name=i[1], password=i[3], cursor=cursor,id=i[0])
        return None

    @staticmethod
    def query_id(cursor,nombre:str="",email:str=""):
        if email != "":
            q = f"select ID from users where Mail='{email}'"
            cursor.execute(q)
            i = cursor.fetchone()
            if i is not None and i[0] is not None:
                return i[0]
        if nombre != "":
            q=f"select ID from users where Nombre='{nombre}'"
            try:
                cursor.execute(q)
                i = cursor.fetchone()
                if i is not None and i[0] is not None:
                    return i[0]
            except:
                return False

        return False

    def exists(self):
        if self.id != None:
            q = f"select * from users a where a.ID = {self.id}"
            self.cursor.execute(q)
            i = self.cursor.fetchone()
            if i[0] is not None:
                return True
            return False
        else:
            q = f"select * from users a where a.Nombre = '{self.name}'"
            self.cursor.execute(q)
            i = self.cursor.fetchone()
            if i[0] is not None:
                return True
            return False

    def get_id(self):
        return self.id

    def is_admin(self):
        q = f"select Is_admin from users where Id='{self.id}'"
        self.cursor.execute(q)
        i = self.cursor.fetchone()
        if i[0] == 'yes':
            return True
        else:
            return False

    def __repr__(self):
        return f"<User nº{self.id}  with name: {self.name} and mail: {self.mail}>"

    def __str__(self):
        return f"<User nº{self.id}  with name: {self.name} and mail: {self.mail}>"