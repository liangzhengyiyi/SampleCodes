#!/root/anaconda3/python
#-*- coding:utf-8 -*-

import urllib.request as request
import json
import requests
import pandas as pd
import re
import numpy as np
import time
import sys
import math
from multiprocessing import Pool
import multiprocessing

import warnings
warnings.filterwarnings("ignore")


def get_tag(tag):
    tag_num = len(tag)
    if tag_num == 0:
        tag_1 = 'NA'
        tagcount_1 = 'NA'
        tag_2 = 'NA'
        tagcount_2 = 'NA'
        tag_3 = 'NA'
        tagcount_3 = 'NA'
    elif tag_num == 1:
        tag_1 = tag[0]['title']
        tagcount_1 = tag[0]['count']
        tag_2 = 'NA'
        tagcount_2 = 'NA'
        tag_3 = 'NA'
        tagcount_3 = 'NA'
    elif tag_num == 2:
        tag_1 = tag[0]['title']
        tagcount_1 = tag[0]['count']
        tag_2 = tag[1]['title']
        tagcount_2 = tag[1]['count']
        tag_3 = 'NA'
        tagcount_3 = 'NA'
    else: 
        tag_1 = tag[0]['title']
        tagcount_1 = tag[0]['count']
        tag_2 = tag[1]['title']
        tagcount_2 = tag[1]['count']
        tag_3 = tag[2]['title']
        tagcount_3 = tag[2]['count']
    return tag_1, tagcount_1, tag_2, tagcount_2, tag_3, tagcount_3


# multiprocess
def get_books(num, start, end):
    name = multiprocessing.current_process().name
    print(name, 'Starting', start, end)
    all_data = []
    book_list_1=[]
    Url_error_1 = []
    Url_error_2 = []
    t = 0
        
    for s in url_book[start:end]:
        try:
            response = requests.get(s, proxies=proxies, verify=False, headers=headers)
            book_1 = response.content.decode('UTF-8')
            book = json.loads(book_1)
            all_data.append(book)
            
            ID = int(re.search(r'\d{2,9}', s).group()) # modify according to the length of user_id
            t += 1
            if book['collections'] == []:
                total = 0
                book_list_1.append({'User_ID': ID, 'total': total, 'book_id': 'NA','title': 'NA',
                                    'author': 'NA', 'pubdate': 'NA', 'pages': 'NA', 'price': 'NA',
                                    'status': 'NA', 'updated_time': 'NA','rating': 'NA', 'tag_1': 'NA', 'tagcount_1': 'NA','tag_2': 'NA', 'tagcount_2': 'NA', 'tag_3': 'NA', 'url':'NA'})
                #book_list_1.append([ID, total,'NA', 'NA','NA',  'NA',  'NA',  'NA','NA',  'NA','NA', 'NA', 'NA','NA'])
            else:
                for i in range(0,20): # crawl the first page of book collections
                    if i <= (len(book['collections']) -1):
                        total = book['total']
                        book_id = book['collections'][i]['book_id']
                        title = book['collections'][i]['book']['title']
                        author_1 = book['collections'][i]['book']['author']
                        author = ''.join(author_1)
                        pubdate = book['collections'][i]['book']['pubdate']
                        pages = book['collections'][i]['book']['pages']
                        price = book['collections'][i]['book']['price']
                        status = book['collections'][i]['status']
                        updated_time = book['collections'][i]['updated']
                        rating = book['collections'][i]['book']['rating']['average']
                        tags = book['collections'][i]['book']['tags']
                        tag_1, tagcount_1, tag_2, tagcount_2, tag_3, tagcount_3 = get_tag(tags)                            #summary = book['collections'][i]['book']['summary']
                        url = book['collections'][i]['book']['url']
                        book_list_1.append({'User_ID': ID, 'total': total, 'book_id': book_id,'title': title, 
                                            'author': author, 'pubdate': pubdate, 'pages': pages, 'price': price,
                                            'status': status, 'updated_time': updated_time,'rating': rating, 'tag_1': tag_1, 'tagcount_1': tagcount_1,'tag_2': tag_2, 'tagcount_2': tagcount_2, 'tag_3': tag_3, 'tagcount_3': tagcount_3,'url': url})
                        #book_list_1.append([ID, total, book_id, title, author, pubdate, pages, price,status, updated_time,rating, tag, tag_count, url])

            if total > 20:
                n = math.ceil(total / 20) # how many times to turn pages, including the first time
                if total % 20 == 0:
                    r = 20
                else:
                    r = total % 20 
                for k in range(1, n-1):
                    s_new = 'https://api.douban.com/v2/book/user/%s/collections?apikey=0b2bdeda43b5688921839c8ecb20399b&start=%s&count=20&client=&udid=' % (ID, k*20) 
                    response = requests.get(s_new, proxies=proxies, verify=False, headers=headers)
                    book_1 = response.content.decode('UTF-8')
                    book = json.loads(book_1)
                    all_data.append(book)
                        #time.sleep(1)
                    for i in range(0,20):
                            #total = book['total']
                        book_id = book['collections'][i]['book_id']
                        title = book['collections'][i]['book']['title']
                        author_1 = book['collections'][i]['book']['author']
                        author = ''.join(author_1)
                        pubdate = book['collections'][i]['book']['pubdate']
                        pages = book['collections'][i]['book']['pages']
                        price = book['collections'][i]['book']['price']
                        status = book['collections'][i]['status']
                        updated_time = book['collections'][i]['updated']
                        rating = book['collections'][i]['book']['rating']['average']
                        tags = book['collections'][i]['book']['tags']
                            #summary = book['collections'][i]['book']['summary']
                        tag_1, tagcount_1, tag_2, tagcount_2, tag_3, tagcount_3 = get_tag(tags)    
                        url = book['collections'][i]['book']['url']
                        book_list_1.append({'User_ID': ID, 'total': total, 'book_id': book_id,'title': title, 
                                            'author': author, 'pubdate': pubdate, 'pages': pages, 'price': price,
                                            'status': status, 'updated_time': updated_time,'rating': rating, 'tag_1': tag_1, 'tagcount_1': tagcount_1,'tag_2': tag_2, 'tagcount_2': tagcount_2, 'tag_3': tag_3, 'tagcount_3': tagcount_3,'url': url})

                k = n-1
                s_new = 'https://api.douban.com/v2/book/user/%s/collections?apikey=0b2bdeda43b5688921839c8ecb20399b&start=%s&count=20&client=&udid=' % (ID, k*20)
                response = requests.get(s_new, proxies=proxies, verify=False)
                book_1 = response.content.decode('UTF-8')
                book = json.loads(book_1)

                all_data.append(book)
                for i in range(0, r):
                    #total = book['total']
                    book_id = book['collections'][i]['book_id']
                    title = book['collections'][i]['book']['title']
                    author_1 = book['collections'][i]['book']['author']
                    author = ''.join(author_1)
                    pubdate = book['collections'][i]['book']['pubdate']
                    pages = book['collections'][i]['book']['pages']
                    price = book['collections'][i]['book']['price']
                    status = book['collections'][i]['status']
                    updated_time = book['collections'][i]['updated']
                    rating = book['collections'][i]['book']['rating']['average']
                    tags = book['collections'][i]['book']['tags']
                        #summary = book['collections'][i]['book']['summary']
                    tag_1, tagcount_1, tag_2, tagcount_2, tag_3, tagcount_3 = get_tag(tags)    
                    url = book['collections'][i]['book']['url']
                    book_list_1.append({'User_ID': ID, 'total': total, 'book_id': book_id,'title': title, 
                                        'author': author, 'pubdate': pubdate, 'pages': pages, 'price': price,
                                        'status': status, 'updated_time': updated_time,'rating': rating, 'tag_1': tag_1, 'tagcount_1': tagcount_1,'tag_2': tag_2, 'tagcount_2': tagcount_2, 'tag_3': tag_3, 'tagcount_3': tagcount_3,'url': url})
        
        except OSError as e:
            print('OSError', s, e)
            Url_error_1.append(s)
            time.sleep(30)    
            
        except Exception as e:    
        #ID_error.append(ID)
            print('Exception', s, e)
            Url_error_2.append(s)
        
    #a = time.strftime('%d_%H_%M',time.localtime(time.time()))
    df = pd.DataFrame(book_list_1)
    #df = pd.DataFrame(book_list_1, columns = ['User_ID', 'total', 'book_id', 'title', 'author', 'pubdate', 'pages', 'price', 'status', 'updated_time' , 'rating', 'tag', 'tag_count', 'url'])
    df.to_csv('/root/Documents/Douban/multiprocess/raw_data/%d_book.csv' % end , index = None, encoding = 'utf-8')

    json_str = json.dumps(all_data, indent = 4)
    with open('/root/Documents/Douban/multiprocess/json/%d_alldata.json' % end, 'w') as json_file:
        json_file.write(json_str)
    
    with open ('/root/Documents/Douban/multiprocess/oserror/%d_oserror.txt' % end,'w') as f:
        for i in Url_error_1:
            f.write(str(i) + '\n') 
            
    with open ('/root/Documents/Douban/multiprocess/exception/%d_exception.txt' % end,'w') as f:
        for i in Url_error_2:
            f.write(str(i) + '\n')     

prefix_1 = list(range(100,1000))
np.random.seed(42)
suffix_1 = np.random.random_integers(0, 99999, size = 400)
suffix = [str(n).zfill(5) for n in suffix_1]

np.random.seed(5)
suffix_2 = np.random.random_integers(0, 99999, size = 200)
suffix_3 = [str(n).zfill(5) for n in suffix_2]

retA = [i for i in suffix if i in suffix_3]
suffix_new = [x for x in suffix if x not in retA]

prefix = [str(n) for n in prefix_1]
user_id = [n + s for n in prefix for s in suffix_new]


url_user = []
for i in user_id:
    url_new_1 = 'https://api.douban.com/v2/user/%s' % i
    url_new = url_new_1 + '?apikey=0b2bdeda43b5688921839c8ecb20399b'
    url_user.append(url_new)
    
url_book = []
for i in url_user:
    url_new = i.replace('user', 'book/user')
    url_new_2 = url_new.replace('?apikey', '/collections?apikey')
    url_book.append(url_new_2)

proxyHost = "http-cla.abuyun.com"
proxyPort = "9030"

    # 代理隧道验证信息
proxyUser = "HK833L818DDFV82C"
proxyPass = "451383F40A18DB37"

proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
    "host" : proxyHost,
    "port" : proxyPort,
    "user" : proxyUser,
    "pass" : proxyPass,
}

proxies = {
    "http"  : proxyMeta,
    "https" : proxyMeta,
}

headers = {'Proxy-Switch-Ip': 'yes'}



if __name__=='__main__':
    book_list = []
    process_num = 5
    batchsize = 72000
    end = batchsize
    for i in range(process_num): 
        p = multiprocessing.Process(target = get_books, args=(i, end * i, end * (i+1)))
        book_list.append(p)
        p.start()
    
    for p in book_list:
        p.join()
# 99,000 urls