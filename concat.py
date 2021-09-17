import pandas as pd
# from sklearn import tree
# from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import LeaveOneOut
from sklearn.model_selection import train_test_split
from sklearn.metrics import f1_score,recall_score,precision_score,accuracy_score
import matplotlib.pyplot as plt
import datetime as dt
import warnings
# from sklearn.model_selection import TimeSeriesSplit
from tqdm import tqdm
import numpy as np
import lightgbm as lgb
import seaborn as sns
from sklearn.metrics import confusion_matrix
from imblearn.under_sampling import RandomUnderSampler
import pickle
import optuna.integration.lightgbm as tuned_lgb


warnings.simplefilter('ignore')


# import yagmail
import time
import re
import os

time_window = "5seconds"
ita_len="N100"
target_name="target4"
condition="D"
data = pd.DataFrame()

# def send_gmail(text):
#     password = 'vxlm nuio gbzl jhkv'#'10241121'
#     id_ = '15t7001y@gmail.com'

#     message = text
#     yag=yagmail.SMTP(id_,password)
#     to=['19nm402a@vc.ibaraki.ac.jp']
#     yag.send(to,'Time_lap_program',message)
#     return
 


month = "feb"

if month == "feb":
    days = ["0201","0202"
        ,"0205","0206","0207","0208","0209"
        ,"0212","0213","0214","0215","0216"
        ,"0219","0220","0221","0222","0223"
        ,"0226","0227","0228"]
    dir_ = "D:\\[山口]研究\\YJFX\\data"
    out_name = "usdjpy_201802_d_new_pbf_" + time_window + ".pickle"
    print("\nFebruary\n")

elif month == "sep":
    days = ["0903","0904","0905","0906","0907"
        ,"0910","0911","0912","0913","0914"
        ,"0917","0918","0919","0920","0921"
        ,"0924","0925","0926","0927","0928"]
    dir_ = "/home/ts-zemi/TomoyaAkiyama/september/"
    out_name = "usdjpy_data_201809_pbf_"+time_window+".pickle"
    print("\nSeptember\n")
       
for i in tqdm(days):
    os.chdir("D:\\[山口]研究\\YJFX\\february\\" + time_window)
    df = pd.read_pickle("2018"+(i)+"_"+target_name+"_"+ita_len+"_TW"+ time_window +"_F1.pickle")

    os.chdir("D:\\[山口]研究\\YJFX\\new_pbf_d\\buy_pressure")
    features_buy = pd.read_pickle("2018"+ i + "_Buy_Side_features_.pickle")

    os.chdir("D:\\[山口]研究\\YJFX\\new_pbf_d\\sell_pressure")
    features_sell = pd.read_pickle("2018" + i + "_Sell_Side_features_.pickle")


    os.chdir("D:\\[山口]研究\\YJFX\\new_pbf_d\\move_pressure")
    features = pd.read_pickle("2018"+ i +"_price_based_features.pickle")

    features = pd.concat([features, features_buy, features_sell], axis = 1)

    # df["amount_move"] = 0.0
    # df["time"] = 0.0
    # for i in range(len(df)):
    #     dfindex = df.index[i]
    #     df.amount_move[i] = features[:dfindex]["amount_move"][-1]
    #     df.time[i] = features[:dfindex]["time"][-1]
    
    for i in features.columns:
        df[i] = 0.0
    
    for i in tqdm(range(len(df))):
        index = df.index[i]
        df.loc[index, features.columns] = features[:index].iloc[[-1],:].values[0]
    # df["amount_dfii"] = df["amount_move"].diff()
    # df = df.dropna()
    data = pd.concat([data,df])

diff_df = pd.DataFrame(data["t+1_d"] - data["t_d"])
data["return"] = diff_df
c_list = data.columns[:-1]
c_list = c_list.insert(1,"return")
data = data[c_list]

data["t+1_d"] = 100* data["t+1_d"].diff()/data["t+1_d"]
data.t_d   = 100*data.t_d.diff()/data.t_d
data.t_1_d = 100*data.t_1_d.diff()/data.t_1_d
data.t_2_d = 100*data.t_2_d.diff()/data.t_2_d
data.t_3_d = 100*data.t_3_d.diff()/data.t_3_d
data.t_4_d = 100*data.t_4_d.diff()/data.t_4_d
data.t_5_d = 100*data.t_5_d.diff()/data.t_5_d

#RV_window
RV_window = 5
r_sqrd = data.t_d.apply(lambda x: x*x)
data["RV"] = RV_window*r_sqrd.rolling(window=RV_window).mean()

os.chdir(dir_)
with open(out_name, 'wb') as f:
    pickle.dump(data, f)

# data.to_pickle(out_name)
# send_gmail("finish_df")
