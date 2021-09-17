import pandas as pd 
import glob
import os
import re
import datetime
import matplotlib.pyplot as plt
from tqdm import tqdm
import copy
import numpy as np


os.chdir("D:\\[山口]研究\\YJFX\\2018_USDJPY")

pickle_file = sorted(glob.glob( "*.pickle"))
month = "feb"
if month == "feb":
    file = sorted(glob.glob( "USD_JPY_201802**_data.pickle"))
    month = "february"
elif month == "sep":
    file = sorted(glob.glob( "USD_JPY_201809**_data.pickle"))
    month = "september"

for i in tqdm(file[1:2]):
    print(i)
#     i = file[2]#pickle_file[31:59]:#pickle_file[243:273]
    os.chdir("D:\\[山口]研究\\YJFX\\2018_USDJPY")
    name = re.search("(.*)(?=.pickle)",i).group()
    date = re.search("[0-9]+",i).group()
    #残すデータを入れる
    df = pd.read_pickle(i)[["Date","Time","Price","Amount","Event_Type","Distance","Gender_Side"]]
    index =  df.Date +" "+ df.Time
    index = index.apply(lambda x: datetime.datetime.strptime(x,"%Y/%m/%d %H:%M:%S.%f"))
    df.index = index
    #最終的に残すやつ
    tmp = df[["Event_Type","Gender_Side","Distance","Amount","Price"]]
    tmp.Price = tmp.Price.replace("",0)
    tmp.Price = tmp.Price.astype("float")
    tmp.Amount = tmp.Amount.astype("float")

    # データセット読み込み
    # df = pd.read_pickle("C:\\Users\\Owner\\Desktop\\OrderBook\\september\\target4\\data\\min\\20180903_target4_N100_TW1min_F1.pickle")
    features = pd.DataFrame()
    sell_features = pd.DataFrame()
    buy_features = pd.DataFrame()
    timelist = sorted(list(set(tmp.index)))
    # print(len(timelist))
    next_start_time= timelist[0]
    for df_indx in tqdm(timelist):
        _ = copy.deepcopy(tmp[next_start_time:df_indx])
        try:
            dp = _[_.Event_Type=="D"].Price[-1]
            next_start_time = _[_.Event_Type=="D"].Price.index[-1]


        except IndexError:
            dp = np.nan
            stay_pressure = pd.DataFrame([np.nan],columns=["stay_amount"],index=[df_indx])
            move_pressure = pd.DataFrame(np.array([np.nan]*9).reshape(1,9),columns=["Layer"+str(i) for i in range(1,10)],index=[df_indx])
            sell_values = pd.DataFrame(np.array([np.nan]*10).reshape(1,10),columns=["Sell_Side"+str(i) for i in range(1,11)],index=[df_indx])
            buy_values = pd.DataFrame(np.array([np.nan]*10).reshape(1,10),columns=["Buy_Side"+str(i) for i in range(1,11)],index=[df_indx])
            sell_features = pd.concat([sell_features, sell_values])
            buy_features = pd.concat([buy_features, buy_values])
            feature = pd.concat([stay_pressure, move_pressure],axis=1)
            features = pd.concat([features,feature])
            continue 

        indx = ((tmp[:df_indx].Event_Type=="Q") & (tmp[:df_indx].Gender_Side=="0"))
        indx = indx[indx==True].index[-1]
        if next_start_time > indx :
            next_start_time = indx

        tmp2 = copy.deepcopy(_.loc[indx:indx,:])
        tmp2 = tmp2[(tmp2.Event_Type=="Q") & (tmp2.Gender_Side=="0") ]
        tmp2 = tmp2[tmp2.Gender_Side=="0"]
        # print(tmp2.index[1])
        tmp2["Price"] = tmp2["Price"] - dp

        stay_amount = 0.0
        values = np.zeros(9)
        #約定価格に注文あるとき
        if len(tmp2[tmp2["Price"]==0.0])!=0: 
            #前約定価格の注文量を抽出
            stay_amount += tmp2[tmp2["Price"]==0.0].Amount.item()
            tmp2 = tmp2[tmp2["Price"]!=0.0]
        
        v = np.round((tmp2.Amount).values,decimals=1)[:9]
        values[:len(v)] = v
        buy_values = values
        buy_feature = pd.DataFrame(buy_values.reshape(1, 9), columns = ["Buy_Side" + str(i) for i in range(1, 10)], index = [df_indx])
        buy_features = pd.concat([buy_features, buy_feature])
    #     buy_price = (tmp2.Price+dp).values[:9]
        # buy_pressure = np.cumsum(values[:9])

        indx = ((tmp[:df_indx].Event_Type=="Q") & (tmp[:df_indx].Gender_Side=="1"))
        indx = indx[indx==True].index[-1]
        if next_start_time > indx :
            next_start_time = indx

        tmp2 = copy.deepcopy(_.loc[indx:indx,:])
        tmp2 = tmp2[tmp2.Event_Type=="Q"]
        tmp2 = tmp2[tmp2.Gender_Side=="1"]


        tmp2["Price"] = tmp2["Price"] - dp
        
        values = np.zeros(9)
        if len(tmp2[tmp2["Price"]==0.0])!=0:
            #前約定価格に注文なし
            stay_amount += tmp2[tmp2["Price"]==0.0].Amount.item()
            tmp2 = tmp2[tmp2["Price"]!=0.0]

        v = np.round((tmp2.Amount).values,decimals=1)[:9]
        values[:len(v)] = v
        sell_values = values
        sell_feature = pd.DataFrame(sell_values.reshape(1, 9), columns = ["Sell_Side" + str(i) for i in range(1, 10)], index = [df_indx])
        sell_features = pd.concat([sell_features, sell_feature])
        # sell_price = (tmp2.Price+dp).values[:9]
        # sell_pressure = np.cumsum(values[:9])

        stay_pressure = pd.DataFrame([stay_amount],columns=["stay_amount"],index=[df_indx])
        move_pressure = pd.DataFrame((buy_values - sell_values).reshape(1,9),columns=["Layer"+str(i) for i in range(1,10)],index=[df_indx])
        # sell_values = pd.DataFrame(sell_values.reshape(1,9),columns=["Sell_Side"+str(i) for i in range(1,10)],index=[df_indx])
        # buy_values = pd.DataFrame(buy_values.reshape(1,9),columns=["Buy_Side"+str(i) for i in range(1,10)],index=[df_indx])
    #     stay_pressure = pd.DataFrame([stay_amount],columns=["stay_amount"],index=[df.index[df_indx]])
    #     buy_price = pd.DataFrame(buy_price.reshape(1,9),columns=["buy_price"+str(i) for i in range(1,10)],index=[df.index[df_indx]])
    #     sell_price = pd.DataFrame(sell_price.reshape(1,9),columns=["sell_price"+str(i) for i in range(1,10)],index=[df.index[df_indx]])
    #     feature = pd.concat([stay_pressure,move_pressure],axis=1)

        feature = pd.concat([stay_pressure, move_pressure],axis=1)
        features = pd.concat([features,feature])
    
    os.chdir("D:\\[山口]研究\\YJFX\\new_pbf_d\\move_pressure")
    features.to_pickle(date+"_price_based_features.pickle")

    os.chdir("D:\\[山口]研究\\YJFX\\new_pbf_d\\sell_pressure")
    sell_features.to_pickle(date+"_Sell_Side_features_.pickle")

    os.chdir("D:\\[山口]研究\\YJFX\\new_pbf_d\\buy_pressure")
    buy_features.to_pickle(date+"_Buy_Side_features_.pickle")