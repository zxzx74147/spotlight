import numpy as np
import datetime
import time
from xsb.error_code import *


# from lightfm.evaluation import precision_at_k
# from lightfm.evaluation import auc_score
# from lightfm.evaluation import *
from lightfm import LightFM, cross_validation

from xsb.db._xsb import _get_book, _save_books,_save_gid_str2int,_get_books_available
from xsb.db._douban import _get_book_douban, _save_books_douban,_get_douban_unrecorded
from xsb.config import *
from lightfm.data import Dataset

LOSS = 'warp'
EMBEDDING_DIM = 16
N_ITER = 120
BATCH_SIZE = 128
L2=1e-9
LR = 1e-3
TAG_NUM = 128
MAX_EXAMPLE_PER_USER = 20


class RecommandorLF:
    def __init__(self):
        self.model = LightFM(no_components=EMBEDDING_DIM,loss=LOSS)

        self.uids = set()
        self.iids = set()
        self.u_i_r = list()
        self.u_hash_i_t = dict()
        self.item_tags = list()
        self.version = datetime.datetime.now()
        self.good_avaliable = _get_books_available()

    def rank(self,uid):
        result = dict()
        if uid in self.user_id_mapping:
            user = self.user_id_mapping[uid]
            scores = self.model.predict(user, self.good_avaliable, item_features=self.item_features)
            top_index = np.argsort(-scores)
            ret = list()
            for item in top_index:

                id = self.item_id_mapping_rev[self.good_avaliable[item]]
                good = _get_book(id)
                ret.append(good)
            result['data'] = ret
            result['errno'] = ERROR_OK
            result['version'] = self.version
        else:
            result['errno'] = ERROR_UID_NOT_FOUNT

        return result

    def fit(self):
        tags = dict()
        item_tags = dict()
        count = 0
        with open(BEHAVIOR_PATH, 'r') as f:

            while True:
                line = f.readline()
                if not line:
                    break
                temp = line.split(':')
                if len(temp) < 4:
                    continue
                user_id = temp[0]
                time = int(temp[1]) // 1000
                good_id = temp[3]
                book = _get_book(good_id)
                if not book:
                    continue
                example_age = time - int(book['create_time'])
                good_id = book['goods_id']
                isbn = book['isbn']
                book_tags = _get_book_douban(isbn)
                if book_tags:
                    for tag in book_tags:
                        if tag in tags:
                            tags[tag] = tags[tag] + 1
                        else:
                            tags[tag] = 1
                    item_tags[good_id] = [tag for tag in book_tags]
                self.uids.add(user_id)
                self.iids.add(good_id)
                if not user_id in self.u_hash_i_t:
                    self.u_hash_i_t[user_id] = list()
                self.u_hash_i_t[user_id].append((good_id, example_age))
                # self.u_i_r_t.append((user_id, good_id, example_age))
                count = count + 1

            _save_books()
            _save_books_douban()
            _save_gid_str2int()

        def takeSecond(elem):
            return elem[1]

        for k, v in self.u_hash_i_t.items():
            if len(v) > MAX_EXAMPLE_PER_USER:
                v.sort(key=takeSecond, reverse=False)
                v = v[:MAX_EXAMPLE_PER_USER]
            for rt in v:
                self.u_i_r.append((k, rt[0], rt[1]))

        tags_sorted = sorted(tags.items(), key=lambda kv: kv[1], reverse=True)
        # print(tags_sorted[:TAG_NUM])
        select_tags = [x[0] for x in tags_sorted[:TAG_NUM]]
        # print(select_tags)
        item_tags_filter = list(map(lambda kv: (kv[0], [x for x in kv[1] if x in select_tags]), item_tags.items()))

        self.dataset = Dataset()
        self.dataset.fit(users=self.uids, items=self.iids, item_features=select_tags)
        (interactions, weights) = self.dataset.build_interactions(self.u_i_r)
        self.item_features = self.dataset.build_item_features(item_tags_filter)
        (interactions_train, interactions_test) = cross_validation.random_train_test_split(interactions,
                                                                                           test_percentage=0.05,
                                                                                           random_state=np.random.RandomState(7))
        self.model.fit(interactions=interactions_train, item_features=self.item_features, epochs=N_ITER, num_threads=10,
                  verbose=False)
        (self.user_id_mapping, self.user_feature_mapping, self.item_id_mapping, self.item_feature_mapping) = self.dataset.mapping()
        self.item_id_mapping_rev = dict((v, k) for k, v in self.item_id_mapping.items())
        self.user_id_mapping_rev = dict((v, k) for k, v in self.user_id_mapping.items())

        self.good_avaliable = list(self.item_id_mapping[x] for x in self.good_avaliable if x in self.item_id_mapping)
