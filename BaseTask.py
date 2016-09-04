import ConfigParser
from db_helper import *
from config import *
from datetime import datetime
from luigi.contrib.sqla import SQLAlchemyTarget
from datetime import datetime, timedelta
from luigi.contrib import mysqldb
import luigi
import luigi.mock
import logging
import time

class BaseTask(luigi.Task):

    cf = ConfigParser.ConfigParser()
    cf.read(config_file_path)
    data_base0 = DBDriver(cf, 'DBServer0')
    data_base1 = DBDriver(cf, 'DBServer1')
    data_base2 = DBDriver(cf, 'DBServer2')
    data_base3 = DBDriver(cf, 'DBServer3')

    user = 'caofei'
    password = 'caofei'
    ip = '172.16.8.1'

    date = (datetime.now() + timedelta(days=-1)).strftime('%Y-%m-%d')
    # user = 'root'
    # password = '138128'
    # ip = '127.0.0.1'
    port = 3306
    db = 'zzlab'
    table = ''

    def __init__(self, table=''):
        super(BaseTask, self).__init__()
        if table != '':
            self.table = table
        # self.mysql_target = mysqldb.MySqlTarget(host=self.ip, database=self.db, user=self.user,
        #                                         password=self.password, table=self.table,
        #                                         update_id=datetime.now().strftime('{table}_%Y-%m-%d').format(table=table))
        self.sqla_target = SQLAlchemyTarget(
                connection_string=self.connection_string,
                target_table=self.table,
                update_id=self.update_id(),
                connect_args=self.connect_args,
                echo=self.echo)

    # sqla
    connection_string = 'mysql+mysqldb://%s:%s@%s:%s/%s?charset=utf8' % \
                        (user, password, ip, port, db)
    echo = False
    connect_args = {}
    target = None

    _logger = logging.getLogger('luigi-interface')

    def output(self):
        return self.sqla_target

    def update_id(self):
        return '_'.join([self.table, datetime.now().strftime('%Y-%m-%d')])

    def save_to_db(self, data_frame, table_=None, method='replace'):
        target = self.output()
        engine = target.engine
        conn = engine.connect()

        if table_ is not None:
            table_s = table_
        else:
            table_s = self.table

        print 'save to sql....'
        dtypes = {
            'id': sqlalchemy.types.Integer,
            'rename': sqlalchemy.types.String(length=100),
            'movie_id': sqlalchemy.types.BigInteger,
            'movie_name': sqlalchemy.types.String(length=255),
            'director': sqlalchemy.types.String(length=255),
            'movie_time': sqlalchemy.types.String(length=20),
            'release_date': sqlalchemy.types.Date,
            'release_site': sqlalchemy.types.String(length=100),
            'movie_code': sqlalchemy.types.String(length=100),
            'movie_id1': sqlalchemy.types.BigInteger,
            'movie_name1': sqlalchemy.types.String(length=255),
            'movie_id2': sqlalchemy.types.BigInteger,
            'movie_name2': sqlalchemy.types.String(length=255),
            'source1': sqlalchemy.types.String(length=100),
            'source2': sqlalchemy.types.String(length=100),
            'unique_code': sqlalchemy.types.String(length=100)
        }

        chunk_size = 10000
        with engine.connect() as conn:
            data_frame.to_sql(table_s, conn, if_exists=method, index=False,
                              index_label='id', dtype=dtypes, chunksize=chunk_size)
        print 'done.'

    @staticmethod
    def save_to_sqlite(conn_sqlite, table_name, frame):
        dtypes = {
            'id': 'INTEGER',
            'rename': 'TEXT',
            'director': 'TEXT',
            'release_date': 'TEXT',
            'movie_time': 'TEXT',
            'release_site': 'TEXT',
            'movie_id_cbooo': 'INTEGER',
            'movie_name_cbooo': 'TEXT',
            'movie_id_douban': 'INTEGER',
            'movie_name_douban': 'TEXT',
            'movie_id_maoyan': 'INTEGER',
            'movie_name_maoyan': 'TEXT',
            'movie_id_1905': 'INTEGER',
            'movie_name_1905': 'TEXT',
            'movie_id_mtime': 'INTEGER',
            'movie_name_mtime': 'TEXT',
            'movie_id_cn': 'INTEGER',
            'movie_name_cn': 'TEXT',
            'rename_1905': 'TEXT',
            'rename_douban': 'TEXT',
            'rename_cbooo': 'TEXT',
            'rename_mtime': 'TEXT',
            'rename_maoyan': 'TEXT',
        }
        frame.to_sql(table_name, conn_sqlite, if_exists='replace', index=False, dtype=dtypes, chunksize=5000)

    def add_timestamp(self):
        engine = self.output().engine
        conn = engine.connect()
        try:
            sql = '''
                ALTER TABLE {table_name}
                ADD COLUMN updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                '''.format(table_name=self.table)
            conn.execute(sql)
        finally:
            conn.close()

    def add_primary_key(self, key_word):
        engine = self.output().engine
        with engine.connect() as conn:
            sql = '''
                ALTER TABLE {table_name}
                ADD PRIMARY KEY({key_word})
                '''.format(table_name=self.table, key_word=key_word)
            conn.execute(sql)

    def get_data_pd(self, table_name):
        engine = create_engine(self.connection_string)
        conn = engine.connect()
        try:
            data = pd.read_sql('select * from {table_}'.format(table_=table_name), conn)
        finally:
            conn.close()
        return data

    def get_data_sql(self, sql):
        engine = create_engine(self.connection_string)
        conn = engine.connect()
        try:
            data = pd.read_sql(sql, conn)
        finally:
            conn.close()
        return data

