# -*- coding: utf-8 -*-
#
# AIIR Final Project :: 載入預訓練好的 word2vec 作相關字詞搜尋
#

import sys
import pandas as pd
import sqlite3
import time

from gensim.models import word2vec
from nltk.metrics import edit_distance


#======================================================================================================
### 函數定義 :: 詞向量最相近的前 N 筆 :: 單筆
#======================================================================================================
def most_similar_1txt( w2v_model, queryText, topn ):
    try:
        similar_words = pd.DataFrame( w2v_model.wv.most_similar( queryText, topn = topn ), columns = [queryText, 'cos'] )
    except:
        similar_words = "N/A"
    
    return similar_words

#======================================================================================================
### 函數定義 :: Query Expansion
#======================================================================================================
def query_expansion( queryText ):
    
    # 定義 edit distance 值
    eDistance = 2
    queryExpansion = []

    # 透過預訓練的 word2vec model 取得搜尋關鍵字的相關字詞
    similar_words = most_similar_1txt( model, queryText, 10 )

    # 先將原搜尋關鍵字置入 queryExpansion 的 list 中
    queryExpansion.append( queryText )

    # 將相關字詞逐一作 edit distance 的檢查再置入 queryExpansion 中 :: 統一都轉成小寫 來作比對
    for word in similar_words[queryText]:
        if edit_distance( queryText.lower(), word.lower() ) <= eDistance:
            queryExpansion.append( word )

    return queryExpansion

#======================================================================================================
### 函數定義 :: 載入文件，產生搜尋基底內容
#======================================================================================================
def getQueryContent():
    
    # 讀取原始檔案
    df = pd.read_csv( "../data/metadata_oct19.csv", encoding='utf8', low_memory=False )

    # 只取 abstract 的部份
    content = df['abstract']

    # 去除 abstract 為 NaN 的部份
    content = content.dropna()

    # 取前 N 筆作為搜尋基底
    queryContent = content

    return queryContent

#======================================================================================================
### 函數定義 :: 關鍵字搜尋 :: sqlite3
#======================================================================================================
def keyWordsSearchSQLite3_OLD( dbConn, queryText ):
    """
    Not using this function for now
    """
    matchedPubmedId = []
    matchedAbstract = []
    for text in queryText:
        cur = dbConn.cursor()
        result = cur.execute("select pubmed_id, abstract from metadata_oct19 where abstract match ? limit 1500 order by rank", ['%'+text+'%'] ).fetchall()
        for res in result:
            #print( res[0] )
            #sys.exit()
            if res[0] != None and res[0] not in matchedPubmedId:
                matchedPubmedId.append( res[0] )
                matchedAbstract.append( res[1] )

    return matchedPubmedId, matchedAbstract


#======================================================================================================
### 函數定義 :: 檢查資料庫連線是否有建立成功
#======================================================================================================
def chk_conn( dbConn ):
     try:
        dbConn.cursor()
        return True
     except Exception as ex:
        return False

#======================================================================================================
### 預定義相關變數
#======================================================================================================
# 記錄執行時間_開始
start_time = time.time()

# 載入預訓練的 word2vec 模型
model = word2vec.Word2Vec.load("../model/w2v_model/word2vec_skipgram_dim_200_train_300.model")

# 定義搜尋關鍵字
queryText01 = 'covid19'
queryText02 = 'vaccine'

# 對搜尋字詞作 query expansin
queryExpansion01 = query_expansion( queryText01 )
queryExpansion02 = query_expansion( queryText02 )


#========================================================================================================
### By Dennis
#========================================================================================================

# Make uniform SQL query statement
def make_statement(q, limit=1000):

    statement = """SELECT *, rank 
                FROM cord19
                WHERE abstract MATCH "{q}" 
                ORDER BY rank
                LIMIT {lim}
                """.format(q=q, lim=limit)    
    return statement


def make_query(qlist, boolean_op = "OR"):
    q = " {} ".format(boolean_op).join(qlist)
    return q


#======================================================================================================
### 執行
#======================================================================================================
# 建立資料庫連線
#========================================================================================================
### By Dennis
#========================================================================================================

def make_intersect(res1, res2):
    id_set1 = {i[-2] for i in res1}
    id_set2 = {i[-2] for i in res2}
    inters = id_set1.intersection(id_set2)

    inters_score = []
    
    for pid in inters:
        for r1 in res1:
            for r2 in res2:
                if r1[-2] == pid and r2[-2] == pid:
                    score = r1[-1]+r2[-1]
                    inters_score.append((r1[-2], score))

    # sort intersection according to their score
    sorted_inters = sorted(inters_score, key=lambda x: x[-1], reverse=True)
    return sorted_inters


dbConn = sqlite3.connect('aiir.db')
search = sqlite3.connect("cord19_20211019_fts.sqlite")
cur = search.cursor()

# 執行初步搜尋
if chk_conn( search ) == True:
    q1 = make_query(queryExpansion01)
    q1 = make_statement(q1)

    q2 = make_query(queryExpansion02)
    q2 = make_statement(q2)
    res1 = cur.execute(q1).fetchall()
    res2 = cur.execute(q2).fetchall()

    # 對兩個搜尋結果取交集
    inters = make_intersect(res1, res2)

else:
    print( 'DB connect fail ~' )
    sys.exit()

# get pubmedID
matchedPubmedId = [i[0] for i in inters]

print("Found {} intersected articles".format(len(matchedPubmedId)))

#======================================================================================================
### 計算執行時間
#======================================================================================================

# 以交集的結果 再回 raw data 取得正式要回傳的資料
final_results = []
for mPid in matchedPubmedId:
    cur = dbConn.cursor()
    result = cur.execute("select pubmed_id, title, abstract, publish_time, authors, journal from metadata_oct19 where pubmed_id = ?", [mPid] ).fetchall()
    final_results.append(result)

end_time = time.time()
seconds = end_time - start_time

for res in final_results:
    print(res)


# 將執行時間轉換成「 時 : 分 : 秒 」的格式
m, s = divmod( seconds, 60 )
h, m = divmod( m, 60 )

print( '執行時間 :: ', "%d : %02d : %02d" % (h, m, s) )

print( "task finished !!" )
