# -*- coding=utf-8 -*-
from BaseTask import *
from movie import *


class Movie1905Task(BaseTask):
    table = 'lqy_1905_filminfo'

    def requires(self):
        return None

    def run(self):
        self._logger.info('Movie1905Task starts.')
        sql = '''
            select film_id_1905 as movie_id, max(movie_name) as movie_name, max(director) as director,
                   max(releasedate) as release_date
            from (
                select * from ztl_test_spider_1905_filminfo
                where date(createed)<=str_to_date('{specified_date}', '%%Y-%%m-%%d')
                order by `createed` desc
            ) t
            group by film_id_1905

        '''.format(specified_date=self.date)
        frame_1905 = self.data_base1.get_data_pd('cine1', sql)

        # 数据处理
        frame_1905.ix[:, 'rename'] = frame_1905.ix[:, 'movie_name'].apply(rewrite)
        frame_1905.ix[:, 'release_site'] = frame_1905.ix[:, 'release_date'].apply(get_release_site)
        frame_1905.ix[:, 'release_date'] = frame_1905.ix[:, 'release_date'].apply(parse_release_date)
        frame_1905.ix[:, 'director'] = frame_1905.ix[:, 'director'].apply(get_director)

        # 去除有可能重复的数据
        frame_1905 = frame_1905.groupby(['movie_id']).first().reset_index()
        frame_1905 = frame_1905.groupby(['rename', 'release_date']).first().reset_index()
        frame_1905 = frame_1905.groupby(['director', 'release_date']).first().reset_index()

        frame_1905 = frame_1905.reindex(
                columns=['rename', 'movie_id', 'movie_name', 'director', 'movie_time', 'release_date', 'release_site'])

        # 除去movie_name为空的记录
        frame_1905 = frame_1905[frame_1905['movie_name'].notnull()]
        #frame_1905 = frame_1905[frame_1905['release_date'].notnull()]
        frame_1905 = frame_1905[frame_1905['rename'].apply(check_rename)]

        self.save_to_db(frame_1905)
        self.add_timestamp()
        self.add_primary_key('movie_id')
        self.output().touch()
        self._logger.info('Movie1905Task done.')

