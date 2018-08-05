import pickle
import os.path
import psycopg2

from xsb.config import BOOK_MAP_DOUBAN_PATH

book_map_douban = dict()

if os.path.isfile(BOOK_MAP_DOUBAN_PATH):
    with open(BOOK_MAP_DOUBAN_PATH, "rb") as f:
        book_map_douban = pickle.load(f)
        book_map_douban = dict((k,v) for k, v in book_map_douban.items() if v is not None)

try:
    conn = psycopg2.connect(database='douban', user='douban', password='about', host='10.1.1.17', port='5432',connect_timeout=1)
    # conn = psycopg2.connect(database='douban', user='douban', password='about', host='zxzx74147.oicp.net', port='5432',
    #                         connect_timeout=1)
except:
    print("db error 10.1.1.17")

def _get_book_douban(isbn):
    if not isbn in book_map_douban :
        if not 'conn' in vars():
            book_map_douban[isbn] = None
        else:
            with conn.cursor() as cur:
                sql = "SELECT tag_strs FROM book WHERE isbn='%s'" % (isbn)
                cur.execute(sql)
                rows = cur.fetchall()
                if rows:
                    row = rows[0]
                    ret = row[0]
                    book_map_douban[isbn] = ret
                else:
                    book_map_douban[isbn] = None
    ret = book_map_douban[isbn]
    return ret

def _get_douban_unrecorded():
    return list([k for k,v in book_map_douban.items() if v is None])

def _save_books_douban():
    with open(BOOK_MAP_DOUBAN_PATH, "wb") as f:
        pickle.dump(book_map_douban, f, pickle.HIGHEST_PROTOCOL)