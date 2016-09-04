# -*- coding=utf-8 -*-
from BaseTask import *
from movie import *
import time

class MovieDoubanTask(BaseTask):
    table = 'lqy_douban_filminfo'

    # def output(self):
    #     return luigi.mock.MockTarget('temp_douban')

    def requires(self):
        return None

    def run(self):
        self._logger.info('MovieDoubanTask starts.')

        sql = '''
            select t1.movieId, t1.name, t2.name as director, t1.runTime, t1.showDate from
            (
              select t.* from (
                select * from t_douban_movie_info
                where date(`date`)<=str_to_date('{specified_date}', '%%Y-%%m-%%d')
                order by `date` desc
                ) t
              group by movieId
            ) t1 left join t_douban_director_info t2
            on t1.movieId = t2.movie_id
        '''.format(specified_date=self.date)
        frame_douban = self.data_base1.get_data_pd('cine1', sql)

        frame_douban.ix[:, 'rename'] = frame_douban.ix[:, 'name'].apply(rewrite)
        frame_douban = frame_douban.rename(
            columns={'movieId': 'movie_id', 'name': 'movie_name', 'showDate': 'release_date', 'runTime': 'movie_time'})
        frame_douban['movie_time'] = frame_douban['movie_time'].apply(deal_with_movie_time)
        frame_douban['release_site'] = frame_douban['release_date'].apply(get_release_site)
        frame_douban['release_date'] = frame_douban['release_date'].apply(parse_release_date)
        frame_douban = frame_douban.groupby(['rename', 'release_date']).first().reset_index()
        frame_douban = frame_douban.reindex(
                columns=['rename', 'movie_id', 'movie_name', 'director', 'movie_time', 'release_date', 'release_site'])
        frame_douban = frame_douban[frame_douban['movie_name'].notnull()]
        # 去除非中文影片名
        frame_douban = frame_douban[frame_douban['rename'].apply(check_rename)]
        #frame_douban = frame_douban[frame_douban['release_date'].notnull()]
        self.save_to_db(frame_douban)
        self.add_timestamp()
        self.add_primary_key('movie_id')
        self.output().touch()
        self._logger.info('MovieDoubanTask done.')