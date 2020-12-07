from yahoo_finance_api2 import share
from datetime import datetime,timedelta
import os
import csv
import pandas as pd
import time
import pytz

#株価データ取得関数　
#証券コードと株価データがセットになったディクショナリーとエラーインデックスを出力
def getStockData(Co_info):
    stockData=dict()
    errorIdx=[]
    for i in range(len(Co_info)):
        #企業指定
        my_share=share.Share(Co_info[i][0]+'.T')
        tmpData=None
        tmpDf=None
        try:
            #取得期間、周期
            tmpData = my_share.get_historical(share.PERIOD_TYPE_MONTH,120,share.FREQUENCY_TYPE_WEEK,1)
            tmpData["timestamp"]=[datetime.fromtimestamp(i/1000) for i in tmpData["timestamp"]]
            tmpDf=pd.DataFrame(data=tmpData)
            tmpDf["code"]=Co_info[i][0]
            stockData[Co_info[i][0]]=tmpDf
            print(datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%T'),i,Co_info[i][0],Co_info[i][1])
            time.sleep(5)
        except Exception as e:
            print(datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%T'),i,e)
            errorIdx.append(i)
            time.sleep(10)
    return stockData,errorIdx

#株価データのファイル出力関数　後ろ4桁が証券コード
def exportStockData(stockData,exportFile):
    serialnum=datetime.now(pytz.timezone('Asia/Tokyo')).strftime("%y%m%d%H%M%S")
    for key,value in stockData.items():
        os.makedirs(os.path.join(exportFile,"stockData"),exist_ok=True)
        value.to_csv(os.path.join(exportFile,"stockData",serialnum+key+".csv"))
    return True

#実行関数
def run(Co_info,exportFile):
    stockData,errorIdx=getStockData(Co_info)
    exportStockData(stockData,exportFile)
    if len(errorIdx)>0:
        print("AN ERROR HAS OCCURRED AT",[Co_info[i][0] for i in errorIdx],[Co_info[i][1] for i in errorIdx]) 
    print('STOCK DATA IS UPDATED AT '+datetime.strftime(datetime.now(pytz.timezone('Asia/Tokyo')), '%Y %m/%d %H:%M:%S'))

#1列目に証券コード、2列目に企業名のcsvファイル
#2次元配列を出力
def readCo_info(Co_infoPath):
    Co_info=[]
    with open(Co_infoPath,encoding='utf8') as fp:
        for row in csv.reader(fp, delimiter=";"):
            Co_info.append(row[0].split(","))
    return Co_info

#実行部
Co_info=readCo_info(Co_infoPath=r"Co_info.csv")
run(Co_info,exportFile=r"output")
