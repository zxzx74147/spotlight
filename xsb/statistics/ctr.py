import pandas as pd
import re
import os
import requests
import pickle
import math
from dateutil import parser

URL = 'https://xsb.myhug.cn/test/codec?strUId=%s'
UID_MAP_FILE = 'userid_str2int.cache'
uid_map_str2int = dict()
PATH_U_B = '/Users/zhengxin/Documents/workspace/recommender/spotlight/xsb/dataset/user_behavior'

if os.path.isfile(UID_MAP_FILE):
    with open(UID_MAP_FILE, "rb") as f:
        uid_map_str2int = pickle.load(f)


def _save_uid_str2int():
    with open(UID_MAP_FILE, "wb") as f:
        pickle.dump(uid_map_str2int, f, pickle.HIGHEST_PROTOCOL)


def getIntUid(strUid):
    if not strUid in uid_map_str2int or uid_map_str2int[strUid] is None:
        req = URL % (strUid)
        print("do request %s" % (req))
        ret = requests.get(req)
        if ret.ok:
            temp = ret.text.split('<br/>')
            uid_int = temp[0].split(":")[1]
            uid_map_str2int[strUid] = int(uid_int)
        if len(uid_map_str2int) % 100 == 0:
            _save_uid_str2int()

    return uid_map_str2int[strUid]


PATH = ['/Users/zhengxin/Documents/workspace/recommender/statistics/c01','/Users/zhengxin/Documents/workspace/recommender/statistics/c02',
'/Users/zhengxin/Documents/workspace/recommender/statistics/old/c01','/Users/zhengxin/Documents/workspace/recommender/statistics/old/c02'
        ]
# PATH = '/Users/zhengxin/Documents/workspace/recommender/statistics/old/c01'
# PATH2 = '/Users/zhengxin/Documents/workspace/recommender/statistics/old/c02'
testList = [
    '10650826',
    '10651162',
    '10650860',
    '10650816',
    '10651168',
    '10650804',
    '10650831',
    '10650986',
    '10650741',
    '10713935'
]

def decodeLine(line):
    out = re.findall('([A-Za-z0-9]+)\\[([A-Za-z0-9.\s/]+)\\]', line)
    # time= re.findall('d{2}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',line)

    ret = dict()
    ret['rec'] = False
    time = re.findall('\d{2}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', line)
    for t in time:
        t = '20' + t
        t = parser.parse(t)
        ret['ts'] = int(t.strftime('%s'))
        break
        # ret['ts'] = int(t.now())
    for i in out:
        if i[0] == 'uri':
            ret['uri'] = i[1]
        if i[0] == 'uId':
            ret['uId'] = getIntUid(i[1])
        if i[0] == 'ts':
            if not 'ts' in ret:
                ret['ts'] = int(i[1])
        if i[0] == 'rec':
            ret['rec'] = True
        if i[0] == 'goodsId':
            ret['goodsId'] = i[1]
    return ret

def main():
    files = list()
    for path in PATH:
        files+=list(map(lambda f: os.path.join(path, f), os.listdir(path)))
    print(files)
    uid_rec_hash = dict()
    uid_rec_fake_hash = dict()
    uid_count_hash = dict()
    uid_count_list_hash = dict()
    c_list = 0
    c_detail = 0
    g_enter = dict()

    # for file in files:
    #     print(file)
    #     if not '0731' in file :
    #         continue
    #     with open(file, 'rt', encoding='utf-8', errors='ignore') as f:
    #         for line in f:
    #             out = decodeLine(line)
    #             if not 'uId' in out:
    #                 continue
    #             if not 'uri' in out:
    #                 continue
    #             if not 'ts' in out:
    #                 continue
    #             if '/g2/detail'!=out['uri']:
    #                 continue
    #             uId = out['uId']
    #             ts = out['ts']
    #             if uId in g_enter:
    #                 g_enter[uId] = g_enter[uId] if g_enter[uId]<ts else ts
    #             else:
    #                 g_enter[uId] = ts
    temp = list()
    uid_gid_dict = dict()
    for file in files:
        if  not '0803' in file :#and not '0802' in file
            continue
        print(file)
        with open(file, 'rt', encoding='utf-8',errors='ignore') as f:
            for line in f:
                out = decodeLine(line)
                if not 'uri' in out:
                    continue
                uri = out['uri']
                if not uri == '/g2/list' and not uri == '/g2/detail':
                    continue
                if not 'uId' in out:
                    # print(out)
                    # print(line)
                    continue
                if not 'ts' in out:
                    print(out)
                    continue

                # if isinstance(out['ts'], str):
                #     print('======')
                #     print(out)

                rec = out['rec']
                uId = out['uId']
                if str(uId) in testList:
                    continue
                temp.append(out)
                if uri == '/g2/list':
                    c_list = c_list + 1
                    if rec:
                        if not uId in uid_rec_hash:
                            uid_rec_hash[uId] =1
                        else:
                            uid_rec_hash[uId] = uid_rec_hash[uId]+1

                    if not uId in uid_count_list_hash:
                        uid_count_list_hash[uId] = 1
                    else:
                        uid_count_list_hash[uId] = uid_count_list_hash[uId] + 1
                elif uri == '/g2/detail':
                    c_detail = c_detail + 1
                    if not 'goodsId' in out:
                        # print(out)
                        # print(line)
                        continue
                    key = str(uId)+out['goodsId']
                    if key in uid_gid_dict:
                        continue
                    uid_gid_dict[key] = True
                    if not uId in uid_count_hash:
                        uid_count_hash[uId] = 1
                    else:
                        uid_count_hash[uId] = uid_count_hash[uId] + 1

    with open(PATH_U_B, 'rt', encoding='utf-8', errors='ignore') as f:
        for line in f:
            h = line.split(':')
            if not h[2] == 'gEnter':
                continue
            out = dict()
            out['ts'] = int(h[1][:-3])
            out['uri'] = '/g2/detail'
            out['uId'] = int(h[0])
            out['log'] = False
            # if isinstance(out['ts'],str):
            #     print('======')
            #     print(out)

            temp.append(out)
    temp = sorted(temp,key=lambda x : x['ts'])

    for out in temp:
        uri = out['uri']
        uId = out['uId']
        ts = out['ts']
        if uri == '/g2/list':
            if 'log' in out:
                continue
            if uId in g_enter:
                if ts//600-g_enter[uId]//600>0:
                # if ts - g_enter[uId]> 0:
                    uid_rec_fake_hash[uId] = True
        elif uri == '/g2/detail':
            if uId in g_enter:
                g_enter[uId] = g_enter[uId] if g_enter[uId] < ts else ts
            else:
                g_enter[uId] = ts
    _save_uid_str2int()
    rec_count = 0
    for k,v in uid_rec_hash.items():
        rec_count+=v
    print('uid_rec_hash : sum = %d len= %d per = %2f'%(rec_count,len(uid_rec_hash),rec_count/len(uid_rec_hash)))
    print('uid_rec_fake_hash = %d'%(len(uid_rec_fake_hash)))
    print('%d %d'%(len(uid_count_list_hash),len(uid_count_hash)))
    print('%d %d' % (c_list, c_detail))

    rec_user_count = 0
    rec_click_count = 0
    nor_user_count = 0
    nor_click_count = 0
    bucket = dict()
    temp = sorted(uid_count_hash.items(), key=lambda x: x[1], reverse=True)
    print(temp[:40])

    for k, v in uid_count_hash.items():

        if not isinstance(k, int):
            continue
        # v = math.log(v,2)
        v = 40 if v>40 else v
        # if v>100:
        #     continue

        if k in uid_rec_hash:
            rec_user_count += 1
            rec_click_count += v
        elif k in uid_rec_fake_hash:
            nor_user_count += 1
            nor_click_count += v

        # if k in uid_rec_fake_hash or k in uid_rec_hash:
        #     index = k % 10
        #     if not index in bucket:
        #         bucket[index] = [0, 0]
        #     bucket[index][0] += 1
        #     bucket[index][1] += v

        index = k % 10
        if not index in bucket:
            bucket[index] = [0, 0]
        bucket[index][0] += 1
        bucket[index][1] += v

    print("rec %4d,%4d %.2f" % (rec_user_count, rec_click_count, rec_click_count / (rec_user_count if rec_user_count>0 else 1)))
    print("nor %4d,%4d %.2f" % (nor_user_count, nor_click_count, nor_click_count / nor_user_count))


    for i in range(0, 10):
        print("%d %4d,%4d %.2f" % (i, bucket[i][0], bucket[i][1], bucket[i][1] / bucket[i][0]))


if __name__ == '__main__':
    main()
    # temp = list()
    # with open(PATH_U_B, 'rt', encoding='utf-8', errors='ignore') as f:
    #     for line in f:
    #         h = line.split(':')
    #         out = dict()
    #         out['ts'] = int(h[1][:-3])
    #         out['uri'] = '/g2/detail' if h[2]=='gEnter' else 'abc'
    #         out['uId'] = int(h[0])
    #         out['log'] = False
    #         temp.append(out)
    #     temp = sorted(temp, key=lambda x: x['ts'])
