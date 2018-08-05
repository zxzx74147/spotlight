from xsb.recommandor import Recommandor, MAX_CACHE
import threading
import collections
from xsb.error_code import *
import datetime
from xsb.recommandor_lf import RecommandorLF
from threading import Thread, Lock

class RecommandDemon:
    rank_list = collections.OrderedDict()
    refresh_thread = None
    recommandor_lock = Lock()
    recommandor_refresh_lock = Lock()

    def __init__(self):
        # self.recommandor = Recommandor()
        self.recommandor = RecommandorLF()
        self.recommandor.fit()

    def rank(self, uid, offset=0, limit=20):
        print('rank:This is a thread of %s' % threading.current_thread())
        result = dict()
        current_v = self.recommandor.version
        r = None
        if offset == 0:
            if uid in self.rank_list:
                ran = self.rank_list[uid]
                if ran['version'] == current_v:
                    r = ran
                    self.rank_list.move_to_end(uid)
                else:
                    self.rank_list.pop(uid)
        else:
            if uid in self.rank_list:
                r = self.rank_list[uid]
                self.rank_list.move_to_end(uid)

        if r is None:
            self.recommandor_lock.acquire()
            rank_ret = self.recommandor.rank(uid)
            self.recommandor_lock.release()
            if rank_ret['errno'] == ERROR_OK:
                self.rank_list[uid] = rank_ret
                r = self.rank_list[uid]
        if r:
            data = r['data'][offset:offset + limit]
            has_more = offset + limit < len(r['data'])
            result['has_more'] = has_more
            result['data'] = data
            return result
        else:
            return False

    def refresh(self):
        self.recommandor_refresh_lock.acquire()
        recommandor = RecommandorLF()
        recommandor.fit()
        self.recommandor_refresh_lock.release()

        self.recommandor_lock.acquire()
        self.recommandor = recommandor
        self.recommandor_lock.release()
        return True