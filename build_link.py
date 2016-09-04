# -*- coding=utf-8 -*-
from movie import *
from BaseTask import *
import merge as mg
import movie_cn as mc
import similar_name as sn
import time


class BuildLinkTask(BaseTask):
    table = 'link_table'

    def requires(self):

        #return [mg.MergeCNTask(), mg.MergeWebTask(), mc.MovieCNTask(), sn.SimilarNameTask()]
        return None

    @staticmethod
    def build_series(length, dict_):
        s = pd.Series(data=[None] * length)
        for k, ls in dict_.items():
            for v in ls:
                s[v] = k
        return s

    def run(self):
        self._logger.info('BuildLinkTask starts.')

        sql0 = '''
            select * from lqy_cn_filminfo
        '''
        frame_cn = self.get_data_sql(sql0)

        sql1 = '''
            select * from merge_link_cn
            where `status` = 1 or `status` = 3 or `status` = 30
        '''
        frame_link_cn = self.get_data_sql(sql1)

        sql2 = '''
            select * from merge_link_web
            where `status` = 1 or  `status` = 3 or `status` = 5 or `status` = 7
        '''
        frame_link_web = self.get_data_sql(sql2)

        # frame_cn['unique_code'] = frame_cn.apply(get_unique_code, axis=1)
        # frame_link_cn['unique_code'] = frame_link_cn.apply(get_unique_code, axis=1, md='movie_id2')

        dict_cn = {}
        for i in range(len(frame_cn)):
            dict_cn[frame_cn.iloc[i].movie_code] = i

        dict_1905 = {}
        dict_cbooo = {}
        dict_mtime = {}
        dict_douban = {}
        dict_maoyan = {}

        name2dict = {'1905': dict_1905, 'cbooo': dict_cbooo, 'mtime': dict_mtime,
                     'douban': dict_douban, 'maoyan': dict_maoyan}

        print 'a.....'
        try:
            for i in range(len(frame_link_cn)):
                source1, movie_id1, source2, movie_id2, movie_code = \
                    frame_link_cn.iloc[i][['source1', 'movie_id1', 'source2', 'movie_id2', 'movie_code']].values
                if source1 in name2dict.keys():
                    if movie_id1 not in name2dict[source1]:
                        name2dict[source1][movie_id1] = [dict_cn[movie_code]]
                    else:
                        name2dict[source1][movie_id1].append(dict_cn[movie_code])
                else:
                    pass
        except Exception as e:
            logger.exception(e)

        frame_mtime_cn = self.get_data_pd('mtime_cn_mapping')
        # frame_mtime_cn['unique_code'] = frame_mtime_cn.apply(get_unique_code, axis=1, md='movie_id2')
        try:
            for i in range(len(frame_mtime_cn)):
                movie_id1,  movie_code = \
                    frame_mtime_cn.iloc[i][['movie_id1', 'movie_code']].values
                if movie_id1 not in name2dict['mtime']:
                    name2dict['mtime'][movie_id1] = [dict_cn[movie_code]]
                else:
                    name2dict['mtime'][movie_id1].append(dict_cn[movie_code])
        except Exception as e:
            logger.exception(e)

        print 'b.....'
        for no in range(4):
            for i in range(len(frame_link_web)):
                source1, movie_id1, source2, movie_id2, movie_name1, movie_name2 = \
                    frame_link_web.iloc[i][['source1', 'movie_id1', 'source2', 'movie_id2', 'movie_name1', 'movie_name2']].values
                dict1 = name2dict[source1]
                dict2 = name2dict[source2]

                if movie_id1 not in dict1.keys() and movie_id2 in dict2.keys():
                    dict1[movie_id1] = dict2[movie_id2]
                elif movie_id2 not in dict2.keys() and movie_id1 in dict1.keys():
                    dict2[movie_id2] = dict1[movie_id1]
                elif movie_id1 in dict1.keys() and movie_id2 in dict2.keys():
                    dict1[movie_id1] = list(set(dict1[movie_id1]).union(set(dict2[movie_id2])))
                    dict2[movie_id2] = dict1[movie_id1]

            print 'linking step no.', no, '.....'

        print 'c.....'
        link_table = frame_cn[['movie_id', 'movie_name', 'release_date', 'movie_code']]
        link_table = link_table.rename(columns={'movie_id': 'movie_id_cn'})
        for col in ['1905', 'cbooo', 'mtime', 'douban', 'maoyan']:
            column_name_id = ''.join(['movie_id_', col])
            link_table[column_name_id] = self.build_series(len(link_table), name2dict[col])

        self.save_to_db(link_table)
        self.add_timestamp()
        self.output().touch()
        self._logger.info('BuildLinkTask is done.')


class ModifyLinkTask(BaseTask):
    def __init__(self):
        super(ModifyLinkTask, self).__init__(table='merge_link_cn(modify)')

    # def output(self):
    #     return self.mysql_target

    def requires(self):
        return [BuildLinkTask(), mg.MergeCNTask(), sn.SimilarNameTask()]
        #return None

    def run(self):
        self._logger.info('ModifyLinkTask starts.')
        start = time.clock()
        engine = self.output().engine
        conn = engine.connect()
        try:
            sql = '''
                update merge_link_cn as t1 set `status`=30
                where t1.status != 1 and t1.source1='1905' and(t1.movie_id1, t1.movie_id2) in
                        (select movie_id_1905, movie_id_cn from link_table where movie_id_1905 is not null)
            '''
            conn.execute(sql)


            sql = '''
                update merge_link_cn as t1 set `status`=30
                where t1.status != 1 and t1.source1='cbooo' and(t1.movie_id1, t1.movie_id2) in
                        (select movie_id_cbooo, movie_id_cn from link_table where movie_id_cbooo is not null)
            '''
            conn.execute(sql)

            sql = '''
                update merge_link_cn as t1 set `status`=30
                where t1.status != 1 and t1.source1='mtime' and(t1.movie_id1, t1.movie_id2) in
                        (select movie_id_mtime, movie_id_cn from link_table where movie_id_mtime is not null)
            '''
            conn.execute(sql)

            sql = '''
                update merge_link_cn as t1 set `status`=30
                where t1.status != 1 and t1.source1='douban' and(t1.movie_id1, t1.movie_id2) in
                        (select movie_id_douban, movie_id_cn from link_table where movie_id_douban is not null)
            '''
            conn.execute(sql)

            sql = '''
                update merge_link_cn as t1 set `status`=30
                where t1.status != 1 and t1.source1='maoyan' and(t1.movie_id1, t1.movie_id2) in
                        (select movie_id_maoyan, movie_id_cn from link_table where movie_id_maoyan is not null)
            '''
            conn.execute(sql)
        finally:
            conn.close()

        print 'ModifyLinkTask, time:', time.clock() - start
        self.output().touch()
        self._logger.info('ModifyLinkTask is done.')

