# -*- coding=utf-8 -*-
from BaseTask import *
from movie import *
import time


class MovieCboooTask(BaseTask):
    table = 'lqy_cbooo_filminfo'

    # def output(self):
    #     return luigi.mock.MockTarget('temp_cboo')

    def requires(self):
        return None

    def run(self):
        self._logger.info('MovieCboooTask starts.')

        sql = '''
            select film_id_cbooo as movie_id, max(movie_name) as movie_name, max(director) as director,
            max(movie_time) as movie_time, max(releasedate) as release_date
            from (
                select * from ztl_test_spider_cbooo_filminfo
                where date(createed)<=str_to_date('{specified_date}', '%%Y-%%m-%%d')
                order by `createed` desc
            ) t
            group by film_id_cbooo

        '''.format(specified_date=self.date)
        frame_cbooo = self.data_base1.get_data_pd('cine1', sql)

        # 数据处理
        frame_cbooo.ix[:, 'rename'] = frame_cbooo.ix[:, 'movie_name'].apply(rewrite)
        frame_cbooo.ix[:, 'release_site'] = frame_cbooo.ix[:, 'release_date'].apply(get_release_site)
        frame_cbooo.ix[:, 'release_date'] = frame_cbooo.ix[:, 'release_date'].apply(parse_release_date)
        frame_cbooo.ix[:, 'director'] = frame_cbooo.ix[:, 'director'].apply(get_director)
        frame_cbooo.ix[:, 'movie_time'] = frame_cbooo.ix[:, 'movie_time'].apply(deal_with_movie_time)

        # 去重
        frame_cbooo = frame_cbooo.groupby(['movie_id']).first().reset_index()
        frame_cbooo = frame_cbooo.groupby(['rename', 'release_date']).first().reset_index()
        frame_cbooo = frame_cbooo.groupby(['director', 'release_date']).first().reset_index()

        frame_cbooo = frame_cbooo[frame_cbooo['movie_name'].notnull()]
        #frame_cbooo = frame_cbooo[frame_cbooo['release_date'].notnull()]
        frame_cbooo = frame_cbooo.reindex(
                columns=['rename', 'movie_id', 'movie_name', 'director', 'movie_time', 'release_date', 'release_site'])

        self.save_to_db(frame_cbooo)
        self.add_timestamp()
        self.add_primary_key('movie_id')
        self.output().touch()
        self._logger.info('MovieCboooTask done.')
