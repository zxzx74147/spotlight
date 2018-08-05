import pickle
import os.path
import requests
import pymysql
import json
from xsb.config import *
import time

#数据库连接
try:
    db =pymysql.connect(host='10.1.4.109', port=3020, user='reader', passwd='MhxzKhl',db='xsb',connect_timeout=3)
except:
    print("db error 10.1.4.109")

sql_get_book_available='select goods_id from goods where  status=1 order by goods_id desc limit 1000;'

sql_get_book='select goods_id,json,create_time from goods where  goods_id=%s'


url_gid_str2int = 'https://xsb.myhug.cn/test/codec?strGoodsId=%s'
book_map_xsb = dict()
gid_map_str2int = dict()
gid_available = None

#载入图书缓存
if os.path.isfile(BOOK_MAP_PATH):
    with open(BOOK_MAP_PATH, "rb") as f:
        book_map_xsb = pickle.load(f)

#载入goods_id 缓存
if os.path.isfile(GID_STR2INT_MAP_PATH):
    with open(GID_STR2INT_MAP_PATH, "rb") as f:
        gid_map_str2int = pickle.load(f)

#载入goods_id_avaliable 缓存
if os.path.isfile(GID_AVALIABLE_PATH):
    with open(GID_AVALIABLE_PATH, "rb") as f:
        gid_available = pickle.load(f)

def _get_book(good_id):

    if isinstance(good_id,int):
        good_id = str(good_id)
    elif len(good_id) == 24:
        good_id = _gid_str2int(good_id)


    if good_id is None:
        return False

    if not good_id in book_map_xsb:
        cursor = db.cursor()
        sql = sql_get_book%good_id
        cursor.execute(sql)
        data = cursor.fetchone()
        if data:
            try:
                book = json.loads(data[1])
                good ={
                    'goods_id':data[0],
                    'create_time':data[2],
                    'isbn': book['isbn13'] if 'isbn13' in book else None,
                    'title':book['title'] if 'title' in book else None
                }
                book_map_xsb[good_id] = good
            except:
                return False


    if not good_id in book_map_xsb:
        book_map_xsb[good_id] = None
    ret = book_map_xsb[good_id]
    return ret

def _get_books_available():
    global gid_available
    try:
        cursor = db.cursor()
        cursor.execute(sql_get_book_available)
        data = cursor.fetchall()
        if data:
            gid_available = [i[0] for i in data]
            _save_gid_avaliable()
    except:
        pass


    return gid_available


def _gid_str2int(good_id_str):
    if not good_id_str in gid_map_str2int:
        req = url_gid_str2int % (good_id_str)
        print("do request %s" % (req))
        ret = requests.get(req)
        if ret.ok:
            temp = ret.text.split('<br/>')
            gid_int = temp[1]
            gid_map_str2int[good_id_str] = gid_int

            if len(gid_map_str2int) % 100 == 0:
                _save_gid_str2int()

    if good_id_str in gid_map_str2int:
        return gid_map_str2int[good_id_str]
    else:
        gid_map_str2int[good_id_str] = False

    return False

def _save_books():
    with open(BOOK_MAP_PATH, "wb") as f:
        pickle.dump(book_map_xsb, f, pickle.HIGHEST_PROTOCOL)

def _save_gid_str2int():
    with open(GID_STR2INT_MAP_PATH, "wb") as f:
        pickle.dump(gid_map_str2int, f, pickle.HIGHEST_PROTOCOL)

def _save_gid_avaliable():
    with open(GID_AVALIABLE_PATH, "wb") as f:
        pickle.dump(gid_available, f, pickle.HIGHEST_PROTOCOL)