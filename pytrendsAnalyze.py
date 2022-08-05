# -*- coding: utf-8 -*-
"""
Created on Mon Oct 12 19:02:06 2020

@author: n1201023
"""
import datetime
import csv
import time
import os
import pandas as pd                        
from pytrends.request import TrendReq
#pytrendsでkeywordの関連トピックのdataframeを取得
pytrend = TrendReq(hl='ja-JP', tz=-540)
def pytrendsTemplate(func,words=[],slptime=10,pustime=600,pauselimit=5):#funcは()付けない
  output=[]
  cnt=0
  pause=0
  escape=False
  for keyword in words:
    try:
      kw_list = [keyword]
      pytrend.build_payload(kw_list, timeframe='now 7-d', geo='JP')
      tmpdf = func()
      output.append(tmpdf)
      cnt=cnt+1
      time.sleep(slptime)
      print(datetime.datetime.now().strftime('%T'),cnt)
    except Exception as e:
      if pause>pauselimit:
            escape=True
            break
      print(datetime.datetime.now().strftime('%T'),pause,e)
      time.sleep(pustime)
      pause=pause+1
  if escape:
    return output,cnt
  else:
    return output,-1

#会社名読み込み
def read_Conames(path=r"C:\Users\n1201023.STCN2\Documents\stockリーグ\Co_selectednames.csv"):
  Co_names=[]
  with open(path,encoding='utf8') as fp:
    for row in csv.reader(fp, delimiter=";"):
      Co_names.append(row[0])
  Co_names.pop(0)
  return Co_names

def reshapeRelatedTopics(output):
  now=datetime.datetime.now()
  timestamp=now.strftime('%F %T')
  serialnum=now.strftime("%y%m%d%H%M%S")
  risingdfs=[]
  topdfs=[]
  keylist=[]
  for dfdic in output:
    for key,value in dfdic.items():
      tmpdf=value["rising"]
      tmpdf["timestamp"]=timestamp
      tmpdf["keyword"]=key
      risingdfs.append(tmpdf)
      tmpdf=value["top"]
      tmpdf["timestamp"]=timestamp
      tmpdf["keyword"]=key
      topdfs.append(tmpdf)
      keylist.append(key)
  return risingdfs,topdfs,keylist,serialnum

def runRelatedTopics(wordlist=[],filepath=r"C:\Users\n1201023.STCN2\Documents\stockリーグ\pytrendsOutput"):
  cnt=0
  while True:
    output,cnt=pytrendsTemplate(func=pytrend.related_topics,words=wordlist[cnt:len(wordlist)])
    risingdfs,topdfs,keylist,serialnum=reshapeRelatedTopics(output=output)
    for i in range(len(keylist)):
      os.makedirs(os.path.join(filepath,"rising",keylist[i]),exist_ok=True)
      risingdfs[i].to_csv(os.path.join(filepath,"rising",keylist[i],serialnum+".csv"))
    for i in range(len(keylist)):
      os.makedirs(os.path.join(filepath,"top",keylist[i]),exist_ok=True)
      topdfs[i].to_csv(os.path.join(filepath,"top",keylist[i],serialnum+".csv"))
    if cnt==-1:
        break
    print("Now sleeping from "+datetime.datetime.now().strftime('%T'))
    time.sleep(1800)
  print("finished!!(runRelatedTopics)")

def runInterestOverTime(wordlist=[],filepath=r"C:\Users\n1201023.STCN2\Documents\stockリーグ\pytrendsOutput"):
  cnt=0
  serialnum=datetime.datetime.now().strftime("%y%m%d%H%M%S")
  while True:
    output,cnt=pytrendsTemplate(func=pytrend.interest_over_time,words=wordlist[cnt:len(wordlist)])
    for i in range(len(wordlist)):
      tmpdf=output[i].rename(columns={wordlist[i]:'value'})
      os.makedirs(os.path.join(filepath,"interestOverTime",wordlist[i]),exist_ok=True)
      tmpdf.to_csv(os.path.join(filepath,"interestOverTime",wordlist[i],serialnum+".csv"))
    if cnt==-1:
        break
    print("Now sleeping from "+datetime.datetime.now().strftime('%T'))
    time.sleep(1800)
  print("finished!!(runInterestOverTime)")


      
