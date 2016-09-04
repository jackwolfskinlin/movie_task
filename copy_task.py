# -*- coding=utf-8 -*-
from BaseTask import *
import build_link
import MySQLdb
import MySQLdb.cursors
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


class CopyTask(BaseTask):
    table = 'copy_task'

    def requires(self):
        return [build_link.BuildLinkTask(), build_link.ModifyLinkTask()]

    def run(self):
        self._logger.info('CopyTask starts.')
        conn1 = MySQLdb.connect(host='172.16.8.1', user='caofei', passwd='caofei',
                                db='zzlab', charset='utf8', cursorclass=MySQLdb.cursors.DictCursor)

        conn2 = MySQLdb.connect(host='10.10.1.164', user='dev', passwd='123456',
                                db='labdb', charset='utf8')

        sql = '''
                    SELECT * FROM link_table
                    WHERE DATE(updated)=DATE(now())
                '''

        try:
            cursor = conn1.cursor()
            cursor.execute(sql)
            results = cursor.fetchall()
            cursor.close()
            conn1.commit()
        finally:
            conn1.close()

        # sql_update = '''
        #         update link_table
        #         set movie_id_1905   = if({movie_id_1905} is null, movie_id_1905, {movie_id_1905}),
        #             movie_id_cbooo  = if({movie_id_cbooo} is null, movie_id_cbooo, {movie_id_cbooo}),
        #             movie_id_mtime  = if({movie_id_mtime} is null, movie_id_mtime, {movie_id_mtime}),
        #             movie_id_douban = if({movie_id_douban} is null, movie_id_douban, {movie_id_douban}),
        #             movie_id_maoyan = if({movie_id_maoyan} is null, movie_id_maoyan, {movie_id_maoyan})
        #         where link_table.movie_code = {movie_code}
        #         '''
        sql_update = '''
            update link_table
            set movie_id_1905   = {movie_id_1905},
                movie_id_cbooo  = {movie_id_cbooo},
                movie_id_mtime  = {movie_id_mtime},
                movie_id_douban = {movie_id_douban},
                movie_id_maoyan = {movie_id_maoyan}
            where link_table.movie_code = '{movie_code}'
            '''

        sql_insert = '''
                insert into link_table(movie_id_cn, movie_code, movie_name, movie_id_1905, movie_id_cbooo, movie_id_mtime, movie_id_douban, movie_id_maoyan, release_date)
                value({movie_id_cn}, '{movie_code}', '{movie_name}', {movie_id_1905}, {movie_id_cbooo}, {movie_id_mtime}, {movie_id_douban}, {movie_id_maoyan}, '{release_date}')
                '''

        count = 0
        try:
            cursor = conn2.cursor()
            for row in results:
                #print row['movie_code']
                count += 1
                if count < 150:
                    continue
                print count
                if row['movie_id_1905'] is None:
                    row['movie_id_1905'] = 'NULL'
                if row['movie_id_cbooo'] is None:
                    row['movie_id_cbooo'] = 'NULL'
                if row['movie_id_mtime'] is None:
                    row['movie_id_mtime'] = 'NULL'
                if row['movie_id_douban'] is None:
                    row['movie_id_douban'] = 'NULL'
                if row['movie_id_maoyan'] is None:
                    row['movie_id_maoyan'] = 'NULL'

                cursor.execute("select 1 from link_table where movie_code='%s'" % row['movie_code'])
                data = cursor.fetchone()
                if data is None:
                    cursor.execute(sql_insert.format(movie_id_cn=row['movie_id_cn'],
                                                     movie_name=row['movie_name'],
                                                     movie_id_1905=row['movie_id_1905'],
                                                     movie_id_cbooo=row['movie_id_cbooo'],
                                                     movie_id_mtime=row['movie_id_mtime'],
                                                     movie_id_douban=row['movie_id_douban'],
                                                     movie_id_maoyan=row['movie_id_maoyan'],
                                                     release_date=row['release_date'],
                                                     movie_code=row['movie_code']
                                                     ))
                else:
                    cursor.execute(sql_update.format(movie_id_1905=row['movie_id_1905'],
                                                     movie_id_cbooo=row['movie_id_cbooo'],
                                                     movie_id_mtime=row['movie_id_mtime'],
                                                     movie_id_douban=row['movie_id_douban'],
                                                     movie_id_maoyan=row['movie_id_maoyan'],
                                                     movie_code=row['movie_code']
                                                     ))

            conn2.commit()
        except Exception as e:
            logger.exception(e)
            conn2.rollback()
            raise Exception(e.message)
        finally:
            cursor.close()
            conn2.close()

        self.output().touch()
        self._logger.info('CopyTask is done.')