import pyodbc


class Database():
    def __init__(self,driver:str,srv_name:str, db_name:str, username:str=None,password:str=None, trusted_connection:bool=True,encrypt:bool=False):
        self._db = None
        self._cursor = None
        self._username = username
        self._password = password
        self._driver = driver
        self._srv_name = srv_name
        self._db_name = db_name
        self._conn_str = None
        if trusted_connection: self._trusted_connection='yes'
        else: self._trusted_connection='no'
        if encrypt: self._encrypt='yes'
        else: self._encrypt='no'

    def set_db_name(self,n:str):
        self._db_name = n


    def create_conn_str(self):
        if self._username == None and self._password == None:
            self._conn_str = f'DRIVER={self._driver}; SERVER=' + self._srv_name + ';DATABASE=' \
                       + self._db_name + ';Trusted_connection='+ self._trusted_connection + ';Encrypt=' + self._encrypt
        else:
            self._conn_str = f'DRIVER={self._driver}; SERVER=' + self._srv_name + ';DATABASE=' \
                       + self._db_name + ';UID=' + self._username + ';PWD=' + self._password + ';Trusted_connection=' \
                              + self._trusted_connection + ';Encrypt=' + self._encrypt

    def connect(self):
        if self._conn_str is None:
            self.create_conn_str()
        try:
            self._db = pyodbc.connect(self._conn_str)
        except Exception as e:
            print(f'Error connecting clients database with {self._conn_str} ', str(e))
            raise Exception(f'Error connecting dabatase {str(e)}')
        print('Clients database connection stablished')

    def commit(self):
        if self._cursor is not None:
            try:
                self._cursor.commit()
            except:
                self._cursor.rollback()
                raise Exception('Error commiting')

    def autocommit(self):
        try:
            self._db.autocommit = True
        except:
            raise Exception('Error creating database autocommit')

    def create_cursor(self):
        try:
            self.connect()
            self._cursor = self._db.cursor()
        except Exception as e:
            print('Error creating clients database cursor ', str(e))
            raise Exception(f'Error creating database cursor {str(e)}')

        print('Clients database cursor created')


    def execute(self,inst:str,params:list=None):
        if self._cursor is None:
            self.create_cursor()
        try:
            if params is not None:
                return self._cursor.execute(inst,params)
            else:
                return self._cursor.execute(inst)
        except Exception as e:
            raise Exception(f'Error executing query\n {inst}, \n with parameters\n {params}, error: {e}')

    def execute_many(self,inst:str,params:list=None):
        if self._cursor is None:
            self.create_cursor()
        '''try:'''
        if params is not None:
            self._cursor.executemany(inst,params)
        else:
            self._cursor.executemany(inst)
        '''except:
            raise Exception(f'Error executing query\n {inst}, \n with parameters\n {params}')'''

    def fetchone(self):
        try:
            return self._cursor.fetchone()
        except:
            return None

    def fetchall(self):
        try:
            return self._cursor.fetchall()
        except:
            return None

    def fetchmany(self,size:int):
        try:
            return self._cursor.fetchmany(size)
        except:
            return None


    def get_cursor(self):
        if self._cursor is None:
            self.create_cursor()
        return self._cursor

    def table_exists(self,table:str):
        if self._cursor is None:
            self.create_cursor()
        if self._cursor.tables(table=table).fetchone():
            return True
        return False

    def exists(self,table:str,column:str,ide:str):
        if self.execute(f"select * from {table} where {column} = ?",[str(ide)]) is not False:
            if self._cursor.fetchone() is not None:
                return True
        return False

    def close_cursor(self):
        try:
            self._cursor.close()
        except:
            self._cursor = None
        self._cursor = None

    def close_connection(self):
        try:
            self._db.close()
        except:
            self.connect()
            self.create_cursor()
            self._db.rollback()
            self.close_cursor()
            self._db.close()
            raise Exception('Error closing connection, rollback made')

    def __repr__(self):
        return f" {self._db_name} in server {self._srv_name}"
    def __str__(self):
        return f" {self._db_name} in server {self._srv_name}"