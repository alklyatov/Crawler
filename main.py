import sys
import numpy as np
import requests
import hashlib
import pymorphy2
from scipy.spatial.distance import cosine
from datasketch import MinHash, MinHashLSH, lsh
from random import randint
from elasticsearch import Elasticsearch
from bs4 import BeautifulSoup

morph = pymorphy2.MorphAnalyzer(lang='ru')

def split(s):
    return [char for char in s]

#Connect to Elastic Serch
def connect_elastic():
    connection = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if connection.ping():
        print('\nConnection to Elastik successfully established!')
        return (connection)
    else:
        print('\nConnection error!')
        sys.exit()

def canonize(text):
    text1 = text
    text1 = text1.replace('.', '')
    text1 = text1.replace('?', '')
    text1 = text1.replace(',', '')
    text1 = text1.replace('!', '')
    text1 = text1.replace(';', '')
    text1 = text1.replace('—', '')
    return (text1)

def shingles(text):
    i = 0
    shingles_2=[]
    while i < len(text)-1:
        shingles_2.append((text[i]) + ' ' + (text[i + 1]))
        i = i + 1
    return shingles_2

def parsing(url):
    recording = {}
    hash_object = []
    links_filter = []
    txt = []
    title_article = []

    get_url = requests.get(url)
    if get_url.status_code == 200:
        #Decode the bytes into a string
        get_html = get_url.text
        soup = BeautifulSoup(get_html, 'html.parser')

        links = soup.findAll('a', {'class': ['article-link']})

        #Retrieving links
        for i in range(len(links)):
            k = (links[i].get('href'))
            links_filter.append(k)

        count_article = len(links)

        for i in range(len(links)):
            k = ('https://tproger.ru/news/' + links_filter[i])

            site = requests.get(k)
            get_html = site.text
            soup = BeautifulSoup(get_html, 'html.parser')
            #print(soup)

            # Title
            title = soup.find('meta', attrs={'property': 'og:title'})
            title_article.append(title.attrs.get('content'))
            recording['Title'] = title_article[i]

            # Time
            if (not soup.find('time', {'class': ['timeago']})):
                time = soup.find('time', {'class': ['localtime']})
            else:
                time = soup.find('time', {'class': ['timeago']})
            recording['Time'] = time.get_text(strip=True)

            # Link
            recording['Link'] = k

            #Author
            author = soup.find('meta', attrs={'name': 'author'})
            recording['Author'] = author.attrs.get('content')

            # Text
            article = soup.find('div', attrs={'class': 'entry-content'})
            all_p = article.find_all('p')
            text_from_p = (p.text.strip() for p in all_p)
            text = ' '.join(text_from_p)
            recording['Text'] = text

            #hashing
            hash_object.append(hashlib.md5(text.encode()).hexdigest())

            text1 = canonize(text)
            split_text = (text1.split())
            shingles_2 = shingles(split_text)
            txt.append(shingles_2)

            #To database
            try:
                if (not es.get_source(index='es_tproger', id=hash_object[i])):
                   es.index(index='es_tproger', id=hash_object[i], body=recording)
            except:
                es.index(index='es_tproger', id=hash_object[i], body=recording)

    else:
        if get_url.status_code == 404:
            print('\n Page not found!')
            sys.exit()
        print('\n Fatal error!')
        sys.exit()


    Similarty(txt, title_article)


# def serch_double(count_article,txt,title_article):
#     print('\nDo you want to search for incomplete doubles? (+/-)')
#     a = (str(input()))
#     while a != '+':
#         print('Its not plus :-( ')
#         if a == '-': break
#         a = (str(input()))
#     if a == '+':
#         for j in range (len(title_article)): print('Article '+str(j), title_article[j])
#         print('\nEnter 2 number of the article from which you want to search for incomplete duplicates ')
#         a = (int(input()))
#         b = (int(input()))

        # m1, m2 = MinHash(), MinHash()
        # for d in txt[a]:
        #      m1.update(d.encode('utf8'))
        # for d in txt[b]:
        #      m2.update(d.encode('utf8'))
        # print("Estimated Jaccard for data1 and data2 is", m1.jaccard(m2))


        # if type(a) == int and a >= 0 and a <= count_article:
        #     #Create LSH index
        #     lsh = MinHash(threshold=0.09)
        #     Article = []
        #     for k in range(count_article):
        #         #Create list wthith MinhashObject
        #         Article.append(MinHash())
        #
        #         for d in txt[k]:
        #             Article[k].update(d.encode('utf8'))
        #
        #         if k!= a: lsh.insert("Article"+ str(k), Article[k])
        #
        #     result = lsh.query(Article[a])
        #     print(f'\nArticles most similar to the Article {a} with the Jaccard coefficient 0.09 ',result)
        # else: print('Incorrect!')

def sklon(a):
    sklon = []
    p = morph.parse(a)[0]
    sklon.append(p.inflect({'nomn'}).word)
    sklon.append(p.inflect({'gent'}).word)
    sklon.append(p.inflect({'datv'}).word)
    sklon.append(p.inflect({'accs'}).word)
    sklon.append(p.inflect({'ablt'}).word)
    sklon.append(p.inflect({'loct'}).word)
    sklon.append(p.inflect({'voct'}).word)
    sklon.append(p.inflect({'gen2'}).word)
    sklon.append(p.inflect({'acc2'}).word)
    sklon.append(p.inflect({'loc2'}).word)
    return sklon


def MinHashi(s):
    с = 4294967300
    N = 128
    perms = []
    MH = []
    max_val = (2**32)-1
    for i in range (N):
        MH.append(float('inf'))
    for i in range(N):
        k1 = randint(0, max_val)
        k2 = randint(0, max_val)
        perms.append((k1,k2))
    for val in s:
        if not isinstance(val, int): val = hash(val)
        for perm_idx, perm_vals in enumerate(perms):
            a, b = perm_vals
            hashfunct = (a * val + b) % с
            if MH[perm_idx] > hashfunct: MH[perm_idx] = hashfunct
    return MH

def Similarty(txt,title_article):
    print('Do you want to find incomplete sets? ')
    a = (str(input()))
    while a != '+':
        a = (str(input()))
        print('Its not plus :-( ')
        if a == '-': break
        a = (str(input()))
    if a == '+':
        for j in range(len(title_article)): print('Article ' + str(j), title_article[j])
        print('\nEnter 2 number of the article from which you want to search for incomplete duplicates ')
        a = (int(input()))
        b = (int(input()))
        Minhash1 = MinHashi(txt[a])
        Minhash2 = MinHashi(txt[b])
        vec1 = np.array(Minhash1) / max(Minhash1)
        vec2 = np.array(Minhash2) / max(Minhash2)

        print("Estimated Jaccard for Article", a,"and Article", b,"is", cosine(vec1,vec2))


#0-1 0.00392156862745098 4367519206756211

def search():
    print('\nDo you want some search? (+/-)')
    a = (str(input()))
    while a != '+':
        print('Its not plus :-( ')
        if a == '-': sys.exit()
        a = (str(input()))
    print('\nEnter word for searching article:')
    a = (str(input()))
    lel = sklon(a)
    qeq = list(set(lel))
    print(qeq)
    for i in range (len(qeq)):
        search_object = {'query': {'match': {'Text': qeq[i]}}}
        res = es.search(index='es_tproger', body=search_object)
        if res['hits']=={'total': {'value': 0, 'relation': 'eq'}, 'max_score': None, 'hits': []} :
            if i == len(qeq):
                print('\nNo search object')
                sys.exit()
        else: print(res)



if __name__ == '__main__':
    url = ('https://tproger.ru/news/')
    MinHashi(url)
    es = connect_elastic()
    #es.indices.delete(index='es_tproger', ignore=[400, 404])
    parsing(url)
    search()



