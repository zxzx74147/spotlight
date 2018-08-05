import numpy as np

import sklearn
from spotlight.interactions import Interactions
from spotlight.cross_validation import random_train_test_split
from xsb.bucketize import _bucketized

from xsb.error_code import *

from lightfm.data import Dataset
import datetime

from xsb.config import *
import torch
from xsb.db._xsb import _get_book, _save_books,_save_gid_str2int,_get_books_available
from xsb.db._douban import _get_book_douban, _save_books_douban,_get_douban_unrecorded
from spotlight.factorization.implicit import ImplicitFactorizationModel
LOSS = 'bpr'
EMBEDDING_DIM = 16
N_ITER = 1
BATCH_SIZE = 128
L2=1e-9
LR = 1e-3
TAG_NUM = 128
MAX_EXAMPLE_PER_USER = 20
import time


class Recommandor:
    def __init__(self):
        self.model_bpr = ImplicitFactorizationModel(loss=LOSS,
                                   embedding_dim=EMBEDDING_DIM,  # latent dimensionality
                                   n_iter=N_ITER,  # number of epochs of training
                                   batch_size=BATCH_SIZE,  # minibatch size
                                   l2=L2,  # strength of L2 regularization
                                   learning_rate=LR,
                                   use_cuda=torch.cuda.is_available())

        self.uids = set()
        self.iids = set()
        self.u_i_r = list()
        self.u_hash_i_t = dict()
        self.item_tags = list()
        self.version = datetime.datetime.now()
        self.good_avaliable = _get_books_available()
        self.good_age = None

    def rank(self,uid):
        result = dict()
        if uid in self.user_id_mapping:
            user = self.user_id_mapping[uid]
            now = int(time.time())
            scores = self.model_bpr.predict(user, self.good_avaliable, item_features=self.inter.item_features)
            top_index = np.argsort(-scores)
            ret = list()
            for item in top_index:
                id = self.item_id_mapping_rev[self.good_avaliable[item]]
                good = _get_book(id)
                temp = dict(good)
                temp['rank']= scores[item]
                ret.append(temp)
            result['data'] = ret
            result['errno'] = ERROR_OK
            result['version'] = self.version
        else:
            result['errno'] = ERROR_UID_NOT_FOUNT
        return result

    def fit(self):
        tags = dict()
        item_tags = dict()
        count=0
        with open(BEHAVIOR_PATH, 'r') as f:

            while True:
                line = f.readline()
                if not line:
                    break
                temp = line.split(':')
                if len(temp) != 4:
                    continue
                user_id =temp[0]
                time = int(temp[1])//1000
                good_id = temp[3][:-1]
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
        for k,v in self.u_hash_i_t.items():
            if len(v)>MAX_EXAMPLE_PER_USER:
                v.sort(key=takeSecond,reverse = False)
                v = v[:MAX_EXAMPLE_PER_USER]
            for rt in v:
                self.u_i_r.append((k, rt[0], rt[1]))
        print(len(self.uids))
        print(len(self.iids))
        print(len(self.u_i_r))
        print(len(item_tags))


        tags_sorted = sorted(tags.items(), key=lambda kv: kv[1], reverse=True)
        # print(tags_sorted[:TAG_NUM])
        select_tags = [x[0] for x in tags_sorted[:TAG_NUM]]
        # print(select_tags)
        item_tags_filter = list(map(lambda kv: (kv[0], [x for x in kv[1] if x in select_tags]), item_tags.items()))

        self.dataset = Dataset()
        self.dataset.fit(users=self.uids, items=self.iids, item_features=select_tags)
        (interactions, weights) = self.dataset.build_interactions(self.u_i_r)
        self.item_features = self.dataset.build_item_features(item_tags_filter)
        print(self.item_features.shape)

        u = interactions.row
        i = interactions.col
        r = np.ones_like(interactions.data)
        f = self.item_features.toarray()[:, len(self.iids):]
        f = sklearn.preprocessing.normalize(f, norm="l1", copy=False)
        c,table = _bucketized(interactions.data,bucket_num=EMBEDDING_DIM)
        # , context_features = c
        self.inter = Interactions(user_ids=u, item_ids=i, ratings=r, item_features=f)

        train, test = random_train_test_split(self.inter, random_state=np.random.RandomState(42), test_percentage=0.1)

        print('Split into \n {} and \n {}.'.format(train, test))
        print(test.item_features.shape)

        self.model_bpr.fit(train,verbose=True)

        (self.user_id_mapping, self.user_feature_mapping, self.item_id_mapping, self.item_feature_mapping) = self.dataset.mapping()
        self.item_id_mapping_rev = dict((v, k) for k, v in self.item_id_mapping.items())
        self.user_id_mapping_rev = dict((v, k) for k, v in self.user_id_mapping.items())

        self.good_avaliable = np.asarray(list(self.item_id_mapping[x] for x in self.good_avaliable if x in self.item_id_mapping))
        self.good_age = list(self._get_book(x)['create_time'] for x in self.good_avaliable if x in self.item_id_mapping)
