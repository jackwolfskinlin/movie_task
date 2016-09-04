# -*- coding=utf-8 -*-
from BaseTask import *
from movie import *
import time


class MovieCNTask(BaseTask):
    table = 'lqy_cn_filminfo'

    # def output(self):
    #     return luigi.mock.MockTarget('temp_cn')

    def requires(self):
        return None

    def run(self):
        self._logger.info('MovieCNTask starts.')

        sql = '''
            select movie_cn_id as movie_id, movie_name, null as director, movie_length as movie_time,
                   movie_premiere_date as release_date, nation as release_site, movie_code
            from t_d_movie
            where date(etl_time)<=str_to_date('{specified_date}', '%%Y-%%m-%%d')

        '''.format(specified_date=self.date)

        frame_cn = self.data_base2.get_data_pd('labdb', sql)

        frame_cn.ix[:, 'rename'] = frame_cn.ix[:, 'movie_name'].apply(rewrite)
        movie_id_counts = get_counts(frame_cn, column='movie_id')

        frame_cn.ix[:, 'release_date'] = frame_cn.ix[:, 'release_date'].apply(parse_release_date)
        frame_cn.ix[:, 'movie_time'] = frame_cn.ix[:, 'movie_time'].apply(deal_with_movie_time)
        frame_cn.ix[:, 'release_site'] = frame_cn.ix[:, 'release_site'].apply(deal_with_site)
        frame_cn['movie_name'] = frame_cn.apply(add_suffix, axis=1, counts=movie_id_counts)
        frame_cn['unique_code'] = frame_cn.apply(get_unique_code, axis=1)
        frame_cn = frame_cn.groupby(['unique_code']).first().reset_index()
        frame_cn = frame_cn[frame_cn['movie_name'].notnull()]
        #frame_cn = frame_cn.groupby(['movie_code']).first().reset_index()
        #frame_cn = frame_cn.drop(['unique_code'], axis=1)

        frame_cn = frame_cn.reindex(
            columns=['rename', 'movie_id', 'movie_name', 'director', 'movie_time', 'release_date', 'release_site',
                     'movie_code', 'unique_code'])

        self.save_to_db(frame_cn)
        self.add_timestamp()
        self.add_primary_key('movie_code')
        self.output().touch()

        self._logger.info('MovieCNTask done.')