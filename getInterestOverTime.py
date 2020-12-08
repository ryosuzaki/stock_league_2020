from datetime import datetime,timedelta
import csv
import time
import pytz
import os
import sys
import pandas as pd                        
from pytrends.request import TrendReq

#人気度の動向取得関数　
#単語インデックスとDFがペアになったディクショナリーを返す
def getInterestOverTime(wordlist,timeframe,startIdx,sleepTime=3,pauseTime=60):
    pytrends=TrendReq(hl='ja-JP', tz=-540)
    output=dict()
    errorIdx=[]
    for i in range(len(wordlist)):
        keyword=wordlist[i]
        try:
            kw_list = [keyword]
            pytrends.build_payload(kw_list, timeframe=timeframe, geo='JP')
            tmpDf = pytrends.interest_over_time()
            output[i]=tmpDf
            print(datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%T'),startIdx+i)
            time.sleep(sleepTime)
        except Exception as e:
            print(datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%T'),startIdx+i,e)
            errorIdx.append(i)
            if len(errorIdx)>30:
                print("TOO MANY ERRORS")
                sys.exit()
            time.sleep(pauseTime)
    return output

#DFのファイル出力関数
def exportInterestOverTime(output,wordlist,exportFile,startIdx):
    serialnum=datetime.now(pytz.timezone('Asia/Tokyo')).strftime("%y%m%d%H%M%S")
    for key,value in output.items():
        value["wordIdx"]=startIdx+key
        os.makedirs(os.path.join(exportFile,"interestOverTime"),exist_ok=True)
        value.to_csv(os.path.join(exportFile,"interestOverTime",serialnum+wordlist[key]+".csv"))
    return True

#実行関数 期間は2016/01/01から昨日まで
def run(wordlist,exportFile,startIdx):   
    yesterday=datetime.strftime(datetime.now(pytz.timezone('Asia/Tokyo'))-timedelta(days=1), '%Y-%m-%d')
    output=getInterestOverTime(wordlist=wordlist,timeframe='2016-01-01 '+yesterday,startIdx=startIdx)
    exportInterestOverTime(output,wordlist,exportFile,startIdx)
    print("finished!!(runInterestOverTime)")  

#wordlist読み込み　1列n行のcsvファイル
def read_wordlist(path):
    wordlist=[]
    with open(path,encoding='utf8') as fp:
        for row in csv.reader(fp, delimiter=";"):
            wordlist.append(row[0])
    return wordlist

#実行部
#numに区間番号(１から)
num=7
wordlist=read_wordlist(r"wordlist.csv")
if num*1600>len(wordlist):
    wordlist=wordlist[1600*(num-1):len(wordlist)]
else:
    wordlist=wordlist[1600*(num-1):1600*num]
run(wordlist=wordlist,exportFile=r"output",startIdx=1600*(num-1)+1)
