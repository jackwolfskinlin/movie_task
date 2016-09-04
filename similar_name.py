# -*- coding=utf-8 -*-
from BaseTask import *
import movie_1905
import movie_cbooo
import movie_mtime
import movie_cn
import movie_douban
import movie_maoyan
import merge
import time
import sqlite3
import Levenshtein
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


class SimilarNameTask(BaseTask):
    table = 'similar_name'

    def requires(self):
        #return [merge.MergeWebTask()]
        return [movie_1905.Movie1905Task(), movie_cbooo.MovieCboooTask(), movie_mtime.MovieMTimeTask(),
                movie_douban.MovieDoubanTask(), movie_maoyan.MovieMaoyanTask(), movie_cn.MovieCNTask(),
                merge.MergeWebTask(), merge.MergeCNTask()]
        #return None

    def run(self):
        self._logger.info('SimilarName starts.')
        tables = ['lqy_1905_filminfo', 'lqy_cbooo_filminfo', 'lqy_mtime_filminfo', 'lqy_douban_filminfo',
                 'lqy_maoyan_filminfo']
        sources = ['1905', 'cbooo', 'mtime', 'douban', 'maoyan']

        # tables = ['lqy_cbooo_filminfo', 'lqy_1905_filminfo']
        # sources = ['cbooo', '1905']

        start = time.clock()
        conn_sqlite = sqlite3.connect(':memory:')
        try:
            sql7 = '''
                select
                t1.movie_id as movie_id1, t1.movie_name as movie_name1, '{film_source1}' as source1,
                t2.movie_id as movie_id2, t2.movie_name as movie_name2, '{film_source2}' as source2, 7 as `status`
                from {table_name1} t1, {table_name2} t2
                where t1.rename!=t2.rename and t1.director=t2.director and t1.release_date=t2.release_date
                     and t1.release_site=t2.release_site and t1.movie_time=t2.movie_time
            '''

            sql8 = '''
                select
                t1.movie_id as movie_id1, t1.movie_name as movie_name1, '{film_source1}' as source1,
                t2.movie_id as movie_id2, t2.movie_name as movie_name2, '{film_source2}' as source2, 8 as `status`
                from {table_name1} t1, {table_name2} t2
                where t1.rename!=t2.rename
                and  (
                      (t1.director is not null and  t2.director is not null and t1.director=t2.director)
                      and
                      (t1.release_date is not null and t2.release_date is not null and t1.year=t2.year)
                      )
                 and (
                      (t1.release_date is null or t2.release_date is null or t1.release_date !=t2.release_date) or
                      (t1.movie_time is null or t2.movie_time is null or t1.movie_time != t2.movie_time) or
                      (t1.release_site is null or t2.release_site is null or t1.release_site != t2.release_site)
                      )
                 and levenshtein_ratio(t1.rename, t2.rename)>0.8
            '''

            sql9 = '''
                select
                t1.movie_id as movie_id1, t1.movie_name as movie_name1, '{film_source1}' as source1,
                t2.movie_id as movie_id2, t2.movie_name as movie_name2, '{film_source2}' as source2, 9 as `status`
                from {table_name1} t1, {table_name2} t2
                where t1.rename!=t2.rename
                and  (
                      (t1.director is not null and  t2.director is not null and t1.director=t2.director)
                      and
                      (t1.release_date is not null and t2.release_date is not null and t1.year=t2.year)
                      )
                 and (
                      (t1.release_date is null or t2.release_date is null or t1.release_date !=t2.release_date) or
                      (t1.movie_time is null or t2.movie_time is null or t1.movie_time != t2.movie_time) or
                      (t1.release_site is null or t2.release_site is null or t1.release_site != t2.release_site)
                      )
                 and levenshtein_ratio(t1.rename, t2.rename)<0.8
            '''

            sql11 = '''
                select
                t1.movie_id as movie_id1, t1.movie_name as movie_name1, '{film_source1}' as source1,
                t2.movie_id as movie_id2, t2.movie_name as movie_name2, '{film_source2}' as source2, t2.release_date,
                11 as `status`, t2.unique_code, t2.movie_code
                from {table_name1} t1, {table_name2} t2
                where t1.rename!=t2.rename and t1.tag=t2.tag
                and levenshtein_ratio(t1.rename, t2.rename) > 0.8
            '''

            conn_sqlite.create_function('levenshtein_ratio', 2, Levenshtein.ratio)

            temp_list = tables + ['lqy_cn_filminfo']
            for table_name in temp_list:
                sql = '''
                    SELECT t.* , SUBSTR(movie_name,1,2) AS tag, YEAR(release_date) as `year`
                    FROM {table_name} t
                '''.format(table_name=table_name)
                frame = self.get_data_sql(sql)
                self.save_to_sqlite(conn_sqlite, table_name, frame)

            frame = None
            for i in range(len(tables)):
                for j in range(i+1, len(tables)):
                    print 'SimilarNameTask: step %d,%d ......' % (i, j)
                    for sql0 in [sql7, sql8, sql9]:
                        sql = sql0.format(film_source1=sources[i], film_source2=sources[j],
                                          table_name1=tables[i], table_name2=tables[j])
                        frame = pd.concat([frame, pd.read_sql(sql, conn_sqlite)], ignore_index=True)
            self.save_to_db(frame, table_='merge_link_web', method='append')

            frame = None
            for i in range(len(tables)):
                print 'SimilarNameTask: step %d,%d ......' % (i, 5)
                sql = sql11.format(film_source1=sources[i], film_source2='cn',
                                   table_name1=tables[i], table_name2='lqy_cn_filminfo')
                frame = pd.concat([frame, pd.read_sql(sql, conn_sqlite)], ignore_index=True)
            self.save_to_db(frame, table_='merge_link_cn', method='append')
        finally:
            conn_sqlite.close()

        self.output().touch()
        self._logger.info('SimilarNameTask is done.')
        print 'time used by SimilarNameTask: ', time.clock() - start