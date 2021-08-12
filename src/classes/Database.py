import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
import psycopg2.sql
import time

from ..utils.main import get_elapsed_time

class Database:
  def _init_(self,client=None,cursor=None):
    self._client = client
    self._cursor = cursor

  def connect(self,host,port,user,password,database,application_name=None,schema='public',timeout=0,ssl_mode=None,ssl_server_ca=None,ssl_client_cert=None,ssl_client_key=None):
    try:
      options = f'-c search_path={schema} -c statement_timeout={timeout}'
      self._client = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        application_name=application_name,
        sslmode=ssl_mode,
        sslrootcert=ssl_server_ca,
        sslcert=ssl_client_cert,
        sslkey=ssl_client_key,
        options=options,
        connect_timeout=3,
        keepalives=1,
        keepalives_idle=5,
        keepalives_interval=2,
        keepalives_count=2
      )
    except Exception as e:
      print('error:',repr(e))

  def disconnect(self):
    if self._client: self._client.close()
    if self._cursor: self._cursor.close()

  def execute_sql(self,sql,p=None):
    try:
      self._cursor.execute(sql,p)
      self._client.commit()
      return self._cursor.fetchall() if self._cursor.description else None
    except Exception as e:
      print('error:',repr(e))

  def execute_sql_file(self,filename,p={},status=0):
    try:
      sql = open(filename,'r').read()
      sqls = sql.split(';\n')
      sqls.pop()
      sqls = sqls[status:]

      for sql in sqls:
        begin_time = time.time()
        self.__logger.info('')
        self.__logger.info('============== Executing script: ==============')
        self.__logger.info(sql.format(**p))
        self._cursor.execute(sql.format(**p))
        self._client.commit()
        self.__logger.info(f'elapsed time: {get_elapsed_time(begin_time)}')
        status += 1
        self.__logger.info(f'status: {status}')
    except psycopg2.OperationalError as e:
      self.__logger.error(e)
      self.execute_sql_file(filename,p,status)
    except Exception as e:
      self.__logger.error(e)

  def get_client(self):
    return self._client

  def get_cursor(self):
    return self._cursor

  def load_config(self):
    self._client.autocommit = False

  def set_cursor(self):
    self._cursor = self._client.cursor(cursor_factory=RealDictCursor)

  def set_logger(self,logger):
    self.__logger = logger