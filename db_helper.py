# -*- coding=utf-8 -*-
from sqlalchemy import create_engine
import pandas as pd
import sqlalchemy
import mysql.connector
from mysql.connector import errorcode


class DBDriver(object):
    def __init__(self, cf, section):
        self.ip = cf.get(section, 'ip')
        self.port = cf.get(section, 'port')
        self.user = cf.get(section, 'user')
        self.password = cf.get(section, 'key')

    def get_data_pd(self, db, sql):
        path = 'mysql+mysqldb://%s:%s@%s:%s/%s?charset=utf8' % (self.user, self.password, self.ip, self.port, db)
        engine = create_engine(path, encoding='utf-8')
        conn = engine.connect()
        try:
            data = pd.read_sql(sql, conn)
        finally:
            conn.close()
        return data

    def get_data_sql(self, db, sql):
        path = 'mysql+mysqldb://%s:%s@%s:%s/%s?charset=utf8' % (self.user, self.password, self.ip, self.port, db)
        engine = create_engine(path, encoding='utf-8')
        conn = engine.connect()
        try:
            data = conn.execute(sql).fetchall()
        finally:
            conn.close()
        return data

    def get_all_data(self, db, table_name, sql=None):
        if sql is None:
            sql = '''SELECT * FROM %s''' % table_name
        data = self.get_data_pd(db, sql)
        return data

    def save_to_db(self, db, table_name, data_frame):
        path = 'mysql+mysqldb://%s:%s@%s:%s/%s?charset=utf8' % (self.user, self.password, self.ip, self.port, db)
        engine = create_engine(path, encoding='utf-8')
        conn = engine.connect()
        
        print 'save to sql....'
        dtypes = {
            'id': sqlalchemy.types.Integer,
            'rename': sqlalchemy.types.String(length=100),
            'movie_id': sqlalchemy.types.BigInteger,
            'movie_name': sqlalchemy.types.String(length=100),
            'director': sqlalchemy.types.String(length=200),
            'movie_time': sqlalchemy.types.String(length=20),
            'release_date': sqlalchemy.types.Date,
            'release_site': sqlalchemy.types.String(length=100),
            'movie_code': sqlalchemy.types.String(length=100),
            'unique_code':sqlalchemy.types.String(length=100),
        }

        # max_line_num = 10000
        # frame = data_frame.iloc[:max_line_num]
        try:
            chunk_size = 10000
            data_frame.to_sql(table_name, conn, if_exists='replace', index=False,
                              index_label='id', dtype=dtypes, chunksize=chunk_size)

            # start = max_line_num
            # for _ in range(len(data_frame)/max_line_num):
            #     frame = data_frame.iloc[start: start+max_line_num]
            #     start += max_line_num
            #     frame.to_sql(table_name, conn, if_exists='append', index=False, index_label='id', dtype=dtypes)
        finally:
            conn.close()
        print 'done.'

    def save_to_db_merge(self, db, table_name, data_frame):
        path = 'mysql+mysqldb://%s:%s@%s:%s/%s?charset=utf8' % (self.user, self.password, self.ip, self.port, db)
        # path = 'mysql+mysqldb://caofei:caofei@10.10.1.19/%s?charset=utf8' %db
        engine = create_engine(path, encoding='utf-8')
        conn = engine.connect()

        print 'save to sql....'
        dtypes = {
            'id': sqlalchemy.types.Integer,
            'rename': sqlalchemy.types.String(length=100),
            'director': sqlalchemy.types.String(length=100),
            'release_date': sqlalchemy.types.Date,
            'movie_time': sqlalchemy.types.String(length=20),
            'release_site': sqlalchemy.types.String(length=100),
            'movie_id_cbooo': sqlalchemy.types.BigInteger,
            'movie_name_cbooo': sqlalchemy.types.String(length=100),
            'movie_id_douban': sqlalchemy.types.BigInteger,
            'movie_name_douban': sqlalchemy.types.String(length=100),
            'movie_id_maoyan': sqlalchemy.types.BigInteger,
            'movie_name_maoyan': sqlalchemy.types.String(length=100),
            'movie_id_1905': sqlalchemy.types.BigInteger,
            'movie_name_1905': sqlalchemy.types.String(length=100),
            'movie_id_mtime': sqlalchemy.types.BigInteger,
            'movie_name_mtime': sqlalchemy.types.String(length=100),
            'movie_id_cn': sqlalchemy.types.BigInteger,
            'movie_name_cn': sqlalchemy.types.String(length=100),
            'rename_1905': sqlalchemy.types.String(length=100),
            'rename_douban': sqlalchemy.types.String(length=100),
            'rename_cbooo': sqlalchemy.types.String(length=100),
            'rename_mtime': sqlalchemy.types.String(length=100),
            'rename_maoyan': sqlalchemy.types.String(length=100),
        }

        try:
            chunk_size = 10000
            data_frame.to_sql(table_name, conn, if_exists='append', index=False,
                              index_label='id', dtype=dtypes, chunksize=chunk_size)
        finally:
            conn.close()
        print 'done.'

    def save_to_db_link(self, db, table_name, data_frame):
        path = 'mysql+mysqldb://%s:%s@%s:%s/%s?charset=utf8' % (self.user, self.password, self.ip, self.port, db)
        # path = 'mysql+mysqldb://caofei:caofei@10.10.1.19/%s?charset=utf8' %db
        engine = create_engine(path, encoding='utf-8')
        conn = engine.connect()

        print 'save to sql....'
        dtypes = {
            'movie_id1': sqlalchemy.types.BigInteger,
            'movie_name1': sqlalchemy.types.String(length=100),
            'movie_id2': sqlalchemy.types.BigInteger,
            'movie_name2': sqlalchemy.types.String(length=100),
            'source1': sqlalchemy.types.String(length=100),
            'source2': sqlalchemy.types.String(length=100),
        }

        try:
            chunk_size = 10000
            data_frame.to_sql(table_name, conn, if_exists='replace', index=False,
                              index_label='id', dtype=dtypes, chunksize=chunk_size)
        finally:
            conn.close()
        print 'done.'


def save_to_disk(df, save_path, method=None):
    if method is None or method == 'excel':
        writer = pd.ExcelWriter(save_path)
        df.to_excel(writer)
        writer.save()
    elif method == 'csv':
        df.to_csv(save_path, encoding='utf-8')
    else:
        print 'invalid save format.'


def drop_if_exist(conn, table):
    if conn is None:
        return False
    cursor = conn.cursor()
    try:
        cursor.execute(
            'SELECT COUNT(1) FROM {table_name} LIMIT 1'.format(table_name=table),
        )
        row = cursor.fetchone()
    except mysql.connector.Error as e:
        if e.errno == errorcode.ER_NO_SUCH_TABLE:
            row = None
        else:
            raise
    if row is not None:
        cursor.execute(
            'DROP TABLE {table_name}'.format(table_name=table),
        )
    return


