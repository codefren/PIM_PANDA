import logging
import pyodbc

logger = logging.getLogger(__name__)


class Database():
    def __init__(self, driver: str, srv_name: str, db_name: str, username: str = None, password: str = None, trusted_connection: bool = True, encrypt: bool = False):
        self._db = None
        self._cursor = None
        self._username = username
        self._password = password
        self._driver = driver
        self._srv_name = srv_name
        self._db_name = db_name
        self._conn_str = None
        if trusted_connection: self._trusted_connection = 'yes'
        else: self._trusted_connection = 'no'
        if encrypt: self._encrypt = 'yes'
        else: self._encrypt = 'no'
        logger.debug(f"Database instance created: {db_name} on {srv_name}")

    def set_db_name(self, n: str):
        self._db_name = n

    def create_conn_str(self):
        if self._username is None and self._password is None:
            self._conn_str = (
                f'DRIVER={self._driver}; SERVER={self._srv_name};DATABASE={self._db_name}'
                f';Trusted_connection={self._trusted_connection};Encrypt={self._encrypt}'
            )
        else:
            self._conn_str = (
                f'DRIVER={self._driver}; SERVER={self._srv_name};DATABASE={self._db_name}'
                f';UID={self._username};PWD=***;Trusted_connection={self._trusted_connection};Encrypt={self._encrypt}'
            )
        logger.debug(f"Connection string created for {self._db_name} on {self._srv_name}")

    def connect(self):
        if self._db is not None:
            logger.debug(f"Already connected to {self._db_name} on {self._srv_name}, skipping reconnect")
            return
        if self._conn_str is None:
            self.create_conn_str()
        # Build the real conn string with actual password for pyodbc (never logged)
        if self._username is not None and self._password is not None:
            real_conn_str = (
                f'DRIVER={self._driver}; SERVER={self._srv_name};DATABASE={self._db_name}'
                f';UID={self._username};PWD={self._password};Trusted_connection={self._trusted_connection};Encrypt={self._encrypt}'
            )
        else:
            real_conn_str = self._conn_str
        try:
            self._db = pyodbc.connect(real_conn_str)
            logger.info(f"Connected to {self._db_name} on {self._srv_name}")
        except Exception as e:
            logger.error(f"Failed to connect to {self._db_name} on {self._srv_name}: {e}")
            raise Exception(f'Error connecting database {e}')

    def commit(self):
        if self._cursor is not None:
            try:
                self._cursor.commit()
                logger.debug(f"Commit successful on {self._db_name}")
            except Exception as e:
                logger.error(f"Commit failed on {self._db_name}, rolling back: {e}")
                self._cursor.rollback()
                raise Exception('Error commiting')

    def autocommit(self):
        try:
            self._db.autocommit = True
            logger.debug(f"Autocommit enabled on {self._db_name}")
        except Exception as e:
            logger.error(f"Failed to enable autocommit on {self._db_name}: {e}")
            raise Exception('Error creating database autocommit')

    def create_cursor(self):
        try:
            self.connect()
            self._cursor = self._db.cursor()
            logger.info(f"Cursor created for {self._db_name} on {self._srv_name}")
        except Exception as e:
            logger.error(f"Failed to create cursor for {self._db_name} on {self._srv_name}: {e}")
            raise Exception(f'Error creating database cursor {e}')

    def execute(self, inst: str, params: list = None):
        if self._cursor is None:
            self.create_cursor()
        try:
            if params is not None:
                return self._cursor.execute(inst, params)
            else:
                return self._cursor.execute(inst)
        except Exception as e:
            logger.error(f"Query failed on {self._db_name} — {inst!r} params={params}: {e}")
            raise Exception(f'Error executing query\n {inst}, \n with parameters\n {params}, error: {e}')

    def execute_many(self, inst: str, params: list = None):
        if self._cursor is None:
            self.create_cursor()
        try:
            if params is not None:
                self._cursor.executemany(inst, params)
            else:
                self._cursor.executemany(inst)
        except Exception as e:
            logger.error(f"executemany failed on {self._db_name} — {inst!r}: {e}")
            raise Exception(f'Error executing many query\n {inst}, \n with parameters\n {params}, error: {e}')

    def fetchone(self):
        try:
            return self._cursor.fetchone()
        except Exception as e:
            logger.error(f"fetchone failed on {self._db_name}: {e}")
            return None

    def fetchall(self):
        try:
            return self._cursor.fetchall()
        except Exception as e:
            logger.error(f"fetchall failed on {self._db_name}: {e}")
            return None

    def fetchmany(self, size: int):
        try:
            return self._cursor.fetchmany(size)
        except Exception as e:
            logger.error(f"fetchmany({size}) failed on {self._db_name}: {e}")
            return None

    def get_cursor(self):
        if self._cursor is None:
            self.create_cursor()
        return self._cursor

    def table_exists(self, table: str):
        if self._cursor is None:
            self.create_cursor()
        if self._cursor.tables(table=table).fetchone():
            return True
        return False

    def exists(self, table: str, column: str, ide: str):
        if self.execute(f"select * from {table} where {column} = ?", [str(ide)]) is not False:
            if self._cursor.fetchone() is not None:
                return True
        return False

    def close_cursor(self):
        try:
            self._cursor.close()
            logger.debug(f"Cursor closed for {self._db_name}")
        except Exception as e:
            logger.warning(f"Error closing cursor for {self._db_name}: {e}")
        self._cursor = None

    def close_connection(self):
        try:
            self._db.close()
            logger.info(f"Connection closed for {self._db_name} on {self._srv_name}")
        except Exception as e:
            logger.error(f"Error closing connection for {self._db_name}, attempting rollback: {e}")
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
