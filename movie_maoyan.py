# -*- coding=utf-8 -*-
from BaseTask import *
from movie import *


class MovieMaoyanTask(BaseTask):
    table = 'lqy_maoyan_filminfo'

    # def output(self):
    #     return luigi.mock.MockTarget('temp_maoyan')

    def requires(self):
        return None

    def run(self):
        self._logger.info('MovieMaoyanTask starts.')

        sql = '''
            select t.* from (
                select film_id_maoyan, movie_name, director, movie_time, releasedate
                from ztl_test_spider_maoyan_filminfo
                where (instr(releasedate, '大陆') or instr(releasedate, '香港')  or instr(releasedate, '内地')
                        or instr(releasedate, '中国')  or instr(releasedate, '台湾'))
                        and date(createed)<=str_to_date('{specified_date}', '%%Y-%%m-%%d')
                order by `createed` desc
            ) t
            group by film_id_maoyan
        '''.format(specified_date=self.date)
        frame_maoyan = self.data_base1.get_data_pd('cine1', sql)

        frame_maoyan.ix[:, 'rename'] = frame_maoyan.ix[:, 'movie_name'].apply(rewrite)
        frame_maoyan.ix[:, 'release_site'] = frame_maoyan.ix[:, 'releasedate'].apply(get_release_site_my)
        frame_maoyan.ix[:, 'releasedate'] = frame_maoyan.ix[:, 'releasedate'].apply(parse_release_date_my)
        frame_maoyan.ix[:, 'movie_time'] = frame_maoyan.ix[:, 'movie_time'].apply(deal_with_movie_time)
        frame_maoyan.ix[:, 'director'] = frame_maoyan.ix[:, 'director'].apply(get_director_my)
        frame_maoyan = frame_maoyan.rename(columns={'film_id_maoyan': 'movie_id', 'releasedate': 'release_date'})

        frame_maoyan = frame_maoyan[frame_maoyan['movie_name'].notnull()]
        # frame_maoyan = frame_maoyan.sort_index(by='createed', ascending=False).groupby(
        #         ['movie_id']).first().reset_index()
        # 清洗
        frame_maoyan = frame_maoyan.groupby(['rename', 'release_date']).first().reset_index()
        frame_maoyan = frame_maoyan[frame_maoyan['movie_name'].notnull()]
        #frame_maoyan = frame_maoyan[frame_maoyan['release_date'].notnull()]
        # 去除非中文影片名
        frame_maoyan = frame_maoyan[frame_maoyan['rename'].apply(check_rename)]

        frame_maoyan = frame_maoyan.reindex(
                columns=['rename', 'movie_id', 'movie_name', 'director', 'movie_time', 'release_date', 'release_site'])

        self.save_to_db(frame_maoyan)
        self.add_timestamp()
        self.add_primary_key('movie_id')
        self.output().touch()
        self._logger.info('MovieMaoyanTask is done.')