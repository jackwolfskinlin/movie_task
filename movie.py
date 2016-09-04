# -*- coding=utf-8 -*-
import re
import json
from datetime import datetime, date
from config import *
import math


class Movie(object):
    """
     ... note goes here, ...
    """
    def __init__(self, name, movie_id=None, director=None, movie_time=None, release_date=None, release_site=None):
        self.name = name
        self.director = director
        self.movie_time = movie_time
        self.release_date = release_date
        self.release_site = release_site
        self.movie_id = movie_id

    # def equal_to(self, other):
    #     if self.name == other.name:
    #         if all([self.release_date, self.release_site, other.release_date, other.release_date]) \
    #             and nearly_equal_site(self.release_site, other.release_site) \
    #                 and abs((self.release_date-other.release_date).days) <= 10:
    #             return True
    #         if all([self.director, other.director, self.movie_time, other.movie_time]) \
    #                 and self.director == other.director and self.movie_time == other.movie_time:
    #             return True
    #     else:
    #         if not all([self.release_date, self.release_site, other.release_date, other.release_date]):
    #             return False
    #         if not all([self.director, other.director, self.movie_time, other.movie_time]):
    #             return False
    #         if nearly_equal_site(self.release_site, other.release_site) and all([self.release_date, other.release_date]) \
    #             and abs((self.release_date - other.release_date).days) <= 10 and self.director == other.director \
    #                 and self.movie_time == other.movie_time:
    #             return True
    #     return False

    def __str__(self):
        kv = {'name': self.name, 'director': self.director, 'movie_time': self.movie_time, 'release_date': self.release_date,
              'release_site': self.release_site}
        return str(kv)

    def __getitem__(self, key):
        if key == 'director': return self.director
        elif key == 'name': return self.name
        elif key == 'movie_time': return self.movie_time
        elif key == 'release_date': return self.release_date
        elif key == 'release_site': return self.release_site
        elif key == 'movie_id': return self.movie_id
        else:
            raise KeyError('[%s]'%key)

    def __setitem__(self, key, value):
        if key == 'director':
            self.director = value
        elif key == 'name':
            self.name = value
        elif key == 'movie_time':
            self.movie_time = value
        elif key == 'release_date':
            self.release_date = value
        elif key == 'release_site':
            self.release_site = value
        elif key == 'movie_id':
            self.movie_id = value
        else:
            raise KeyError('[%s]' % key)


# # 数据中上映地点格式有, 1.中国 2. 中国/中国香港 3.中国 中国香港
# def nearly_equal_site(site1, site2):
#     sites_dict1 = []
#     sites_dict2 = []
#     if site1 is not None and not is_nan(site1):
#         sites_dict1 = re.split(u'[/ ]', site1)
#     if site2 is not None:
#         sites_dict2 = re.split(u'[/ ]', site2)
#     for val1 in sites_dict1:
#         for val2 in sites_dict2:
#             if val1 == val2:
#                 return True
#     return False


def rewrite(old_name):
    re_character = u'[!"#$%％&\'ˊ`°()（）《》！•￥、。，‘’“”：*+,-./:;<=>？℃÷×不?@[\\]^_`{|}~…·；\s——]+'
    chinese_num_dict = {u'一': u'1', u'二': u'2', u'三': u'3', u'四': u'4', u'五': u'5',
                        u'六': u'6', u'七': u'7', u'八': u'8', u'九': u'9', u'十': u'10',
                        u'之二': u'2', u'第一部': u'1', u'第一集': u'1', u'第二部': u'2', u'第二集': u'2',
                        u'第三部': u'3', u'第三集': u'3'}
    roman_numerals_dict = {u'Ⅰ': u'1', u'Ⅱ': u'2', u'Ⅲ': u'3', u'Ⅳ': u'4', u'Ⅴ': u'5',
                           u'Ⅵ': u'6', u'Ⅶ': u'7', u'Ⅷ': u'8', u'Ⅸ': u'9', u'Ⅹ': u'10'}
    valid_list = list(u'一二三四五六七八九十上下0123456789') + [u'终极版', u'动画版', u'儿童版']

    film_name = old_name

    #   匹配括号中的内容
    re_bracket = u'（.*?）|\(.*?\)'
    origin_bracket_content = re.findall(re_bracket, old_name)
    if len(origin_bracket_content) > 0:
        for i in range(len(origin_bracket_content)):
            # 去掉括号内容中的标点符号
            bracket_content = re.sub(re_character, '', origin_bracket_content[i])
            # 非数字和版本的，直接删除
            if bracket_content not in valid_list:
                film_name = film_name.replace(origin_bracket_content[i], '')
            # 替换中文数字
            elif bracket_content in chinese_num_dict.keys():
                number = chinese_num_dict[bracket_content]
                #        print origin_bracket_content, number
                film_name = film_name.replace(origin_bracket_content[i], number)
    # 去掉括号等标点符号
    film_name = re.sub(re_character, '', film_name)
    # 替换罗马数字
    re_roman_num = u'[ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩ]'
    numerals_list = re.findall(re_roman_num, film_name)
    if len(numerals_list) > 0:
        for num in numerals_list:
            film_name = film_name.replace(num, roman_numerals_dict[num])

    # 去掉电影名称中的"3D"
    film_name = re.sub('3D', '', film_name)
    # 去掉中文影片名后的字母
    temp = re.search(u'[\u4e00-\u9fa5]+', film_name)
    if temp is not None:
        film_name = re.sub(u'[\d]*[a-zA-Z]+[\d]*[a-zA-Z]+[\d]*$', '', film_name)
    # # 转为小写字母
    film_name = film_name.lower()
    return film_name


def get_director(director):
    if director is None or is_nan(director) or director == '':
        return None
    try:
        json_values = json.loads(director).values()
        if len(json_values) > 0:
            # d = re.sub(u'[a-zA-Z-\s]+', '', json_value[0])  #note: 导演名字中有中文、有拼音、有英文
            # if d == '':
            #     d = json_value[0]
            # return d
            json_values = map(lambda x: x.strip(), json_values)
            for i in range(len(json_values)):
                temp = re.search(u'[\u4e00-\u9fa5]+', json_values[i])
                if temp is not None:
                    json_values[i] = re.sub(u'[a-zA-Z\s\.\'-]+$', '', json_values[i])
            json_values = list(set(json_values))
            return '/'.join(json_values)
        else:
            return None
    except Exception as e:
        logger.warning('%s'%e)
        return None


def get_director_my(director):
    if director is None or is_nan(director) or director == '':
        return None
    try:
        values = director.split(',')
        if len(values) > 0:
            values = map(lambda x: x.strip(' \t\r\n()'), values)
            # for i in range(len(values)):
            #     temp = re.search(u'[\u4e00-\u9fa5]+', values[i])
            #     if temp is not None:
            #         values[i] = re.sub(u'[a-zA-Z\s\.\'-]+$', '', values[i])
            json_values = list(set(values))
            return '/'.join(json_values)
        else:
            return None
    except Exception as e:
        logger.warning('%s'%e)
        return None


def parse_release_date(date_str):
    release_date = None

    if date_str is None or is_nan(date_str):
        return release_date
    if isinstance(date_str, datetime):
        return date_str
    if isinstance(date_str, date):
        return datetime.strptime(str(date_str), '%Y-%m-%d')
    try:
        item = re.search(u'(\d{4})年(\d{1,2})月(\d{1,2})日', date_str)
        if item is not None:
            release_date = datetime.strptime('-'.join([item.group(1), item.group(2), item.group(3)]), '%Y-%m-%d')
            return release_date
        item = re.search(u'(\d{4})年(\d{1,2})月', date_str)
        if item is not None:
            release_date = datetime.strptime('-'.join([item.group(1), item.group(2), '01']), '%Y-%m-%d')
            return release_date
        item = re.search(u'(\d{4})年', date_str)
        if item is not None:
            release_date = datetime.strptime(item.group(1) + '-01-01', '%Y-%m-%d')
            return release_date
        item = re.search(u'(\d{4})-(\d{1,2})-(\d{1,2})', date_str)
        if item is not None:
            release_date = datetime.strptime(item.group(0), '%Y-%m-%d')
            return release_date
        item = re.search(u'(\d{4})-(\d{1,2})', date_str)
        if item is not None:
            release_date = datetime.strptime(item.group(0)+'-01', '%Y-%m-%d')
            return release_date
        item = re.search(u'(\d{4})', date_str)
        if item is not None:
            release_date = datetime.strptime(item.group(0)+'-01-01', '%Y-%m-%d')
            return release_date
    except Exception as e:
        logger.warning('msg:{},release_date:{}'.format(e, release_date))
        return None
    return release_date


def check_date(release_date):
    if release_date < datetime(1800, 1, 1):
        raise ValueError, 'Date format not correct:{}'.format(release_date)


def parse_release_date_my(date_str):
    release_date = None

    if date_str is None or is_nan(date_str):
        return release_date
    if isinstance(date_str, datetime):
        return date_str
    if isinstance(date_str, date):
        return datetime.strptime(str(date_str), '%Y-%m-%d')
    try:
        item = re.search(u'(\d{4})-(\d{1,2})-(\d{1,2})', date_str)
        if item is not None:
            release_date = datetime.strptime(item.group(0), '%Y-%m-%d')
            check_date(release_date)
            return release_date
        item = re.search(u'(\d{4})-(\d{1,2})', date_str)
        if item is not None:
            release_date = datetime.strptime(item.group(0)+'-01', '%Y-%m-%d')
            check_date(release_date)
            return release_date
        item = re.search(u'(\d{4})', date_str)
        if item is not None:
            release_date = datetime.strptime(item.group(0)+'-01-01', '%Y-%m-%d')
            check_date(release_date)
            return release_date
    except Exception as e:
        logger.warning('msg:%s, release_date:%s' % (e, date_str))
        return None
    return release_date


def get_release_site(site_str):
    release_site = None
    if site_str is None or is_nan(site_str):
        return release_site
    site = re.search(u'\((.*?)\)|（(.*?)）', site_str)
    if site is not None:
        if site.group(1) is not None:
            release_site = site.group(1)
        else:
            release_site = site.group(2)
    if release_site is not None:
        release_site = re.sub(u'大陆|首映|试播集|录像带发行', '', release_site)
        release_site = re.sub(u'電影節', u'电影节', release_site)
    if release_site == u'内地' or release_site == u'中国大陆':
        release_site = u'中国'
    if release_site == u'USA':
        release_site = u'美国'
    return release_site


def get_release_site_my(site_str):
    release_site = None
    if site_str is None or is_nan(site_str) or site_str == '':
        return release_site
    site = re.search(u'[-\d]+(.*?)上映', site_str)
    if site is not None:
        if site.group(1) is not None:
            release_site = site.group(1)

    if release_site is not None:
        release_site = re.sub(u'首映|试播集|录像带发行', '', release_site)
        release_site = re.sub(u'電影節', u'电影节', release_site)
    if release_site == u'内地' or release_site == u'中国大陆' or release_site == u'大陆':
        release_site = u'中国'
    if release_site == u'中国香港':
        release_site = u'香港'
    if release_site == u'USA':
        release_site = u'美国'
    return release_site


def deal_with_movie_time(t):
    if t is None or is_nan(t):
        return None
    # if re.match(u'^\d+$', t) is not None:
    #     t = '%smin' % t
    # t = re.sub(u'分钟', 'min', t)
    # t = re.search(u'(\d+min)', t)
    t = re.search(u'(\d+)', t)
    if t is None:
        return None
    else:
        return t.group(1)


def is_nan(num):
    if isinstance(num, float) and math.isnan(num):
        return True
    return False


def deal_with_site(site):
    rewrite_site = None
    if site is not None and not is_nan(site):
        sites = re.split(u'[/ ]', site)
        sites = map(lambda x: x.strip(), sites)
        sites = list(set(sites))
        rewrite_site = '/'.join(sites)
    return rewrite_site


def add_suffix(x, counts):
    if x['movie_id'] in counts.keys() and counts[x['movie_id']] > 1:
        return x['movie_name'] + u'(' + str(x['release_date'].year) + u')'
    else:
        return x['movie_name']


def get_counts(df, column):
    return dict(df.groupby([column]).size())


def get_unique_code(row, md='movie_id', rd='release_date'):
    try:

        return '|'.join([str(row[md]), str(row[rd].year)])
    except Exception as e:
        logger.exception(e)
        print row


def check_release_site(site):
    if any([u'中国' in site, u'香港' in site, u'台湾' in site, site == '', is_nan(site), site is None]):
        return True
    else:
        return False


def check_rename(film_name):
    """
    check if film_name cantains any Chinese character
    :param film_name: str
    :return: bool
    """
    temp = re.search(u'[\u4e00-\u9fa5]+', film_name)
    if temp is not None:
        return True
    else:
        return False
