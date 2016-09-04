# -*- coding=utf-8 -*-
from BaseTask import *
from movie import *
import time


class MovieMTimeTask(BaseTask):
    table = 'lqy_mtime_filminfo'

    # def output(self):
    #     return luigi.mock.MockTarget('temp_mtime')

    def requires(self):
        return None

    def run(self):
        self._logger.info('MovieMTimeTask starts.')

        sql = '''
            select t1.movieId, t1.name, t2.cname as director, t1.runTime, t1.showTime, t1.nation from
            (
              select t.* from (
                select * from t_mtime_movie_info
                where date(`date`)<=str_to_date('{specified_date}', '%%Y-%%m-%%d')
                order by `date` desc
                ) t
              group by movieId
            ) t1 left join t_mtime_director_info t2
            on t1.movieId = t2.movieId
        '''.format(specified_date=self.date)
        frame_mtime = self.data_base1.get_data_pd('cine1', sql)
        print 'length of frame is:', len(frame_mtime)

        frame_mtime.ix[:, 'rename'] = frame_mtime.ix[:, 'name'].apply(rewrite)
        frame_mtime = frame_mtime.reindex(
            columns=['rename', 'movieId', 'name', 'director', 'showTime', 'nation', 'runTime'])
        frame_mtime = frame_mtime.rename(
            columns={'name': 'movie_name', 'movieId': 'movie_id', 'showTime': 'release_date', 'nation': 'release_site',
                     'runTime': 'movie_time'})
        frame_mtime.ix[:, 'release_date'] = frame_mtime.ix[:, 'release_date'].apply(parse_release_date)
        frame_mtime.ix[:, 'movie_time'] = frame_mtime.ix[:, 'movie_time'].apply(deal_with_movie_time)
        frame_mtime.ix[:, 'release_site'] = frame_mtime.ix[:, 'release_site'].apply(deal_with_site)

        # frame_mtime = frame_mtime.sort_index(by='date', ascending=False).groupby(['movie_id']).first().reset_index()

        # 去重
        frame_mtime = frame_mtime.groupby('movie_id').first().reset_index()
        frame_mtime = frame_mtime.groupby(['rename', 'release_date']).first().reset_index()
        frame_mtime = frame_mtime[frame_mtime['movie_name'].notnull()]

        frame_mtime = frame_mtime.reindex(
            columns=['rename', 'movie_id', 'movie_name', 'director', 'movie_time', 'release_date', 'release_site'])
        # 去除非中文影片名
        frame_mtime = frame_mtime[frame_mtime['rename'].apply(check_rename)]

        self.save_to_db(frame_mtime)
        self.add_timestamp()
        self.add_primary_key('movie_id')
        self.output().touch()
        self._logger.info('MovieMTimeTask is done.')
