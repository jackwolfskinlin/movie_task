# -*- coding=utf-8 -*-
from BaseTask import *
import movie_1905
import movie_cbooo
import movie_mtime
import movie_cn
import movie_douban
import movie_maoyan

import time



class MergeWebTask(BaseTask):
    def __init__(self):
        super(MergeWebTask, self).__init__(table='merge_link_web')

    # def output(self):
    #     return self.mysql_target

    def requires(self):
        #return [movie_cbooo.MovieCboooTask(), movie_maoyan.MovieMaoyanTask()]
        return [movie_1905.Movie1905Task(), movie_cbooo.MovieCboooTask(), movie_mtime.MovieMTimeTask(),
                movie_douban.MovieDoubanTask(), movie_maoyan.MovieMaoyanTask()]
        #return None

    def run(self):
        self._logger.info('MergeWebTask starts.')

        tables = ['lqy_1905_filminfo', 'lqy_cbooo_filminfo', 'lqy_mtime_filminfo', 'lqy_douban_filminfo',
                  'lqy_maoyan_filminfo']
        sources = ['1905', 'cbooo', 'mtime', 'douban', 'maoyan']
        # tables = ['lqy_1905_filminfo', 'lqy_cbooo_filminfo']
        # sources = ['1905', 'cbooo']

        start = time.clock()
        engine = self.output().engine
        conn = engine.connect()
        try:
            conn.execute("drop table if exists {table_name}".format(table_name='merge_link_web'))

            conn.execute('''
                            CREATE TABLE `merge_link_web` (
                             `movie_id1` bigint(20) DEFAULT NULL,
                             `movie_name1` varchar(255) DEFAULT NULL,
                             `source1` varchar(100) DEFAULT NULL,
                             `movie_id2` bigint(20) DEFAULT NULL,
                             `movie_name2` varchar(255) DEFAULT NULL,
                             `source2` varchar(100) DEFAULT NULL,
                             `status` bigint(20) DEFAULT NULL,
                             `updated` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                           ) ENGINE=InnoDB DEFAULT CHARSET=utf8
                           ''')



            sql0 = '''
                insert into merge_link_web(movie_id1, movie_name1, source1, movie_id2, movie_name2, source2, `status`)
                select t.* from
                (
                    select t1.movie_id as movie_id1, t1.movie_name as movie_name1, '{film_source1}' as source1,
                           t2.movie_id as movie_id2, t2.movie_name as movie_name2, '{film_source2}' as source2,

                           if(t1.release_date=t2.release_date,
                              if(instr(t1.release_site,t2.release_site) or instr(t2.release_site,t1.release_site), 1, 2),
                              if(t1.director is null or t2.director is null,
                                  if(ABS(TIMESTAMPDIFF(DAY, t1.release_date, t2.release_date))<=100, 3, 4),
                                  if(instr(t1.director, t2.director)>0 or instr(t2.director, t1.director)>0,
                                    if(t1.movie_time=t2.movie_time, 5, 6),
                                    10
                                  )
                              )
                           ) as `status`
                    from {table_name1} t1, {table_name2} t2
                    where t1.rename=t2.rename
                  )t where(t.status<10)
            '''

            for i in range(len(tables)):
                for j in range(i+1, len(tables)):
                    print 'MergeWebTask: step %d,%d ......' % (i, j)
                    sql = sql0.format(film_source1=sources[i], film_source2=sources[j],
                                      table_name1=tables[i], table_name2=tables[j])
                    conn.execute(sql)
        finally:
            conn.close()

        self.output().touch()
        self._logger.info('MergeWebTask is done.')
        print 'time used by MergeWebTask: ', time.clock() - start


class MergeCNTask(BaseTask):
    def __init__(self):
        super(MergeCNTask, self).__init__(table='merge_link_cn')

    # def output(self):
    #     return self.mysql_target

    def requires(self):
        return [movie_1905.Movie1905Task(), movie_cbooo.MovieCboooTask(), movie_mtime.MovieMTimeTask(),
                movie_douban.MovieDoubanTask(), movie_maoyan.MovieMaoyanTask(), movie_cn.MovieCNTask()]
        #return [movie_1905.Movie1905Task(), movie_cbooo.MovieCboooTask(), movie_cn.MovieCNTask()]
        #return None

    def run(self):
        self._logger.info('MergeCNTask starts.')

        tables = ['lqy_1905_filminfo', 'lqy_cbooo_filminfo', 'lqy_mtime_filminfo', 'lqy_douban_filminfo',
                 'lqy_maoyan_filminfo']
        sources = ['1905', 'cbooo', 'mtime', 'douban', 'maoyan']
        # tables = ['lqy_1905_filminfo', 'lqy_cbooo_filminfo', 'lqy_mtime_filminfo']
        # sources = ['1905', 'cbooo', 'mtime']

        start = time.clock()
        engine = self.output().engine
        with engine.connect() as conn:
            conn.execute("drop table if exists {table_name}".format(table_name='merge_link_cn'))

            conn.execute('''
                            CREATE TABLE `merge_link_cn` (
                             `movie_id1` bigint(20) DEFAULT NULL,
                             `movie_name1` varchar(255) DEFAULT NULL,
                             `source1` varchar(100) DEFAULT NULL,
                             `movie_id2` bigint(20) DEFAULT NULL,
                             `movie_name2` varchar(255) DEFAULT NULL,
                             `source2` varchar(100) DEFAULT NULL,
                             `release_date` date DEFAULT NULL,
                             `status` bigint(20) DEFAULT NULL,
                             `unique_code` varchar(100) NOT NULL,
                             `movie_code` varchar(100) NOT NULL,
                             `updated` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                           ) ENGINE=InnoDB DEFAULT CHARSET=utf8
                           ''')


            sql0 = '''
                insert into merge_link_cn(movie_id1, movie_name1, source1, movie_id2, movie_name2, source2, release_date, `status`, unique_code, movie_code)
                select t.* from
                (
                  select t1.movie_id as movie_id1, t1.movie_name as movie_name1, '{film_source1}' as source1,
                       t2.movie_id as movie_id2, t2.movie_name as movie_name2, 'cn' as source2, t2.release_date as release_date,

                       if(t1.release_date=t2.release_date,
                          1,
                          if(ABS(TIMESTAMPDIFF(DAY, t1.release_date, t2.release_date))<=100, 3, 4)
                       )as `status`,
                       t2.unique_code,
                       t2.movie_code

                  from {table_name1} t1, lqy_cn_filminfo t2
                  where t1.rename=t2.rename
                ) t where(t.status<>10)

            '''

            for i in range(len(tables)):
                print 'step (%d, %d) ......' % (i, 5)
                sql = sql0.format(film_source1=sources[i], table_name1=tables[i])
                conn.execute(sql)

        self.output().touch()
        self._logger.info('MergeCNTask is done.')
        print 'time used by MergeCNTask: ', time.clock() - start
