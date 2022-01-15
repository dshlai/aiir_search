# -*- coding: utf-8 -*-
#
# AIIR Final Project :: 將原始語料載入 sqlite
#

import os
import sys
import pandas as pd
import pickle
import sqlite3
import time

#======================================================================================================
### 函數定義 :: 載入文件，產生搜尋基底內容
#======================================================================================================
def getQueryContent():
    
    # 讀取原始檔案
    df = pd.read_csv( "../data/metadata_oct19.csv", encoding='utf8', low_memory=False )
    print( df.columns )
    sys.exit()
    # 只取 abstract 的部份
    content = df['abstract']

    # 去除 abstract 為 NaN 的部份
    content = content.dropna()

    # 取前 N 筆作為搜尋基底
    queryContent = content.head( 300 )

    return queryContent

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
### 執行
#======================================================================================================
dbConn = sqlite3.connect('aiir.db')

if chk_conn(dbConn) == True:
    cur = dbConn.cursor()

    # 建立對應資料表
    #cur.execute('CREATE TABLE metadata_oct19( cord_uid, sha, source_x, title, doi, pmcid, pubmed_id, license, abstract, publish_time, authors, journal, mag_id, who_covidence_id, arxiv_id, pdf_json_files, pmc_json_files, url, s2_id )')
    #dbConn.commit()
    #sys.exit()

    # 讀取原始檔案
    #df = pd.read_csv( "../data/metadata_oct19.csv", encoding='utf8', low_memory=False )

    # 將原始檔案直接寫入 sqlite
    #df.to_sql('metadata_oct19', dbConn, if_exists='append', index=False)
    #print( 'Data import done ~' )

    # 作搜尋測試
    text = 'covid19'
    result = cur.execute("select pubmed_id, abstract from metadata_oct19 where abstract like ? limit 3", ['%'+text+'%'] ).fetchall()
    
    print( result[0][0] )
    sys.exit()
else:
    print( 'DB connection check failed !!' )
    sys.exit()
