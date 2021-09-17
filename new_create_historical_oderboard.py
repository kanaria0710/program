#########################################
#説明変数には板の変化(現在の価格＋呼値n個分),現在の価格　→　2N+2
#ヒストリカルな板&約定価格　5つ分
#目的変数　約定価格の変化
##########################################
from tqdm import tqdm
import glob
import matplotlib.pyplot as plt
import re
import os
import pandas as pd
import create_data as eda
import warnings
import numpy as np
import datetime as dt
import copy
warnings.simplefilter(action='ignore', category=FutureWarning)

#make_file_USDJPY_pickle
#import handling_data
#os.chdir('C:\\Users\\pc-owner\\Desktop\\研究室\\YJFX\\Order_Book')
#OrderBook2018 = os.listdir("F:/order_infomation_2018")
#for i in tqdm(range(len(OrderBook2018))):
#    data = handling_data.handling("F:\order_infomation_2018",OrderBook2018[i])
#
#    currency_types = data.currency_types()
#    data.file_devide("USD/JPY","2018_USDJPY")


##pickle file 読み込み
month = "february"
input_dir = "D:\\[山口]研究\\YJFX\\2018_USDJPY\\"
out_dir = "D:\\[山口]研究\\YJFX\\february\\"
# out_dir = out_dir + month

os.chdir(input_dir) #dirの変更
pickle_file = glob.glob( "*.pickle") #指定したファイル内容の取得
pickle_file.sort() #ソートする
## 対象とりあえず2月5日　～　2月9日　流動性が高い
## 対象とりあえず2月12日　～　2月16日　流動性が高い d 42~46
## 対象とりあえず2月19日　～　2月23日　流動性が高い d 49~53
#2/1,2 -> 31,32
#2/26,27,28 -> 56,57,58

#Feburary 31,59


###
#9/3~7    245~249
#9/10~14  252~256
#9/17~21  259~263
#9/24~28  266~270
###
tar = "target4"
N = 100

for d in tqdm([31, 32, 35, 36, 37, 38, 39, 42, 43, 44, 45, 46, 49, 50, 51, 52, 53, 56, 57, 58]):
    
    #d = 35
    s_p = dt.datetime.now()
    s_a = dt.datetime.now()


    print("reading_data"+str(d),"\n")
    print("target:"+tar,"\n")
    print("N:"+str(N),"\n")
    os.chdir(input_dir)
    usdjpy_20180102 = pd.read_pickle(pickle_file[d])

    date = re.search(r"(?<=USD_JPY_)(.*)(?=_data)", pickle_file[d]).group(0)
    #edaパッケージによってfloat type化(main機能)
    f = eda.data_parse(usdjpy_20180102)
    df= f.df_float_type()

    #同じタイミングでD：約定が発生していないかチェック
    df_index = df[df.Event_Type == "D"]
    check_index = (len(df_index) == len(set(df_index)))

    #同時間に約定が複数あるとき　リストに加える
    duplicated_d = df_index.groupby(df_index.index).agg({"Price":lambda x : np.array(x),"Amount":lambda x : np.array(x)})

    for i in tqdm(range(len(duplicated_d))):
        #約定が複数同時刻にある場合Amountとの加重平均を約定価格に利用
        agg_price = round(np.sum(duplicated_d.Price.iloc[i] * duplicated_d.Amount.iloc[i])/np.sum(duplicated_d.Amount.iloc[i]),3)
        duplicated_d.Price.iloc[i] = agg_price

    #最初の約定時のq0 q1 を保持
    start = df.between_time(*pd.to_datetime([str(df.index[0]),str(duplicated_d.index[0])]).time)
    #分割　q0　q1　d0　d
    ###########################start の初期化############################################
    extraction = ["Distance","Price","Amount","Total Amount"]
    try:
        start_q0 = start.between_time(*pd.to_datetime( [str(start[(start.Gender_Side == "0") & (start.Event_Type == "Q") ].index[-1]) , str(start[(start.Gender_Side == "0") & (start.Event_Type == "Q") ].index[-1]) ] ).time)
        start_q0 = start_q0[start_q0.Event_Type == "Q"][extraction]
    except IndexError:
        start_q0 = pd.DataFrame([[1,0,0,0]],columns=["Distance","Price","Amount","Total Amount"],index=[duplicated_d.index[0]])
        pass

    try:
        start_q1 = start.between_time(*pd.to_datetime( [str(start[(start.Gender_Side == "1") & (start.Event_Type == "Q") ].index[-1]) , str(start[(start.Gender_Side == "1") & (start.Event_Type == "Q") ].index[-1]) ] ).time)
        start_q1 = start_q1[start_q1.Event_Type == "Q"][extraction]
    except IndexError:
        start_q1 = pd.DataFrame([[1,0,0,0]],columns=["Distance","Price","Amount","Total Amount"],index=[duplicated_d.index[0]])
        pass

    #time_horizen 設定
    for time_delta in [1]:
        
        time_horizen = dt.timedelta(seconds=time_delta)
        if time_delta == 1:
            time = "1seconds"
            print(time,"\n")
        
        elif time_delta == 5:
            time = "5minutes"
            print(time,"\n")
        
        elif time_delta == 10:
            time = "10minutes"
            print(time, "\n")
        
        # elif time_delta == 30:
        #     time = "30seconds"
        #     print(time, "\n")
        

        #切り上げの割り算
        length = -(-(duplicated_d.index[-1] - duplicated_d.index[0]) // time_horizen)

        start_index = duplicated_d.index[0]
        index = start_index

        info_orderboard = pd.DataFrame()
        #呼値範囲　説明変数　2n　+　2　個
        n = N

        #目的変数　time_horizen　の　何step先か
        forward = 1
        target = tar


        q0_t = copy.copy(start_q0)
        q1_t = copy.copy(start_q1)
        q0 = copy.copy(start_q0)
        q1 = copy.copy(start_q1)
        ex_df = pd.DataFrame(index = [index+time_horizen] , columns=[i for i in range(-1*n, n + 1, 1)])
        ex_df = ex_df.fillna(0)
        ex_df.columns = list(map(lambda x : "t_"+str(x),ex_df.columns))
        ex_df_t_1 = copy.copy(ex_df)
        ex_df_t_1.columns = ["t_1_"+str(i) for i in range(-1*n, n + 1, 1)]
        ex_df_t_2 = copy.copy(ex_df_t_1)
        ex_df_t_2.columns = ["t_2_"+str(i) for i in range(-1*n, n + 1, 1)]
        ex_df_t_3 = copy.copy(ex_df_t_2)
        ex_df_t_3.columns = ["t_3_"+str(i) for i in range(-1*n, n + 1, 1)]
        ex_df_t_4 = copy.copy(ex_df_t_3)
        ex_df_t_4.columns = ["t_4_"+str(i) for i in range(-1*n, n + 1, 1)]
        ex_df_t_5 = copy.copy(ex_df_t_4)
        ex_df_t_5.columns = ["t_5_"+str(i) for i in range(-1*n, n + 1, 1)]

        d_price_t = duplicated_d[(start_index <= duplicated_d.index) & (duplicated_d.index <= index+time_horizen) ].Price[-1]
        d_price_t_1 = copy.copy(d_price_t)
        d_price_t_2 = copy.copy(d_price_t_1)
        d_price_t_3 = copy.copy(d_price_t_2)
        d_price_t_4 = copy.copy(d_price_t_3)
        d_price_t_5 = copy.copy(d_price_t_4)

        t_1_columns = ["t_1_"+str(i) for i in range(-1*n, n + 1, 1)]
        t_2_columns = ["t_2_"+str(i) for i in range(-1*n, n + 1, 1)]
        t_3_columns = ["t_3_"+str(i) for i in range(-1*n, n + 1, 1)]
        t_4_columns = ["t_4_"+str(i) for i in range(-1*n, n + 1, 1)]
        t_5_columns = ["t_5_"+str(i) for i in range(-1*n, n + 1, 1)]

        ex_df_t = pd.DataFrame()

        for i in tqdm(range(length)):
            q0_t_1 = q0_t
            q0_t_1.index = [index] * len(q0_t_1)
            q1_t_1 = q1_t
            q1_t_1.index = [index] * len(q1_t_1)

            seg_df = df.between_time(*pd.to_datetime([str(index),str(index + time_horizen)]).time)
            #start_index更新
            index = index + time_horizen
            #ここで用いる約定価格を決定している(最新約定を利用)
            d_price = duplicated_d[(start_index <= duplicated_d.index) & (duplicated_d.index <= index) ].Price[-1]

            #呼び値基準で上下n個
            price_list = [round(d_price + 0.001*i,3) for i in range( n , -1*n - 1, -1)]

            cnt = 0
            while cnt < len(seg_df):
                #同時間で区切る
                part_of_seg = seg_df.between_time(*pd.to_datetime([str(seg_df.index[cnt]),str(seg_df.index[cnt])]).time)

                #最後の要素かチェック
                judge_last = cnt + len(part_of_seg) == len(seg_df)

                #分割　q0　q1　d0　d1
                extraction = ["Distance","Price","Amount","Total Amount"]

                #d板を用いて加重平均約定も利用可？
                d_t = part_of_seg[(part_of_seg["Event_Type"] == "D")][extraction]
                q0_t = part_of_seg[(part_of_seg["Event_Type"] == "Q") & (part_of_seg["Gender_Side"] == "0")][extraction]
                q1_t = part_of_seg[(part_of_seg["Event_Type"] == "Q") & (part_of_seg["Gender_Side"] == "1")][extraction]
                d0_t = part_of_seg[(part_of_seg["Event_Type"] == "D") & (part_of_seg["Gender_Side"] == "0")][extraction]
                d1_t = part_of_seg[(part_of_seg["Event_Type"] == "D") & (part_of_seg["Gender_Side"] == "1")][extraction]

                #変更がない場合前の板を参照
                if len(q0_t) == 0:
                    q0_t = q0

                elif len(q0_t) != 0:
                    q0 = q0_t

                if len(q1_t) == 0:
                    q1_t = q1

                elif len(q1_t) != 0:
                    q1 = q1_t

                cnt = cnt + len(part_of_seg)


            ######### q0_t - q0_t_1 の状態，q1_t - q1_t_1 の状態がほしい
            #########　※要素が間にないとき　→　変化　0


            ###############価格↓　distance↓###########################
            ex_q0_t = copy.copy(q0_t.Amount)
            ex_q0_t_1 = copy.copy(q0_t_1.Amount)
            ex_q0_t.index = q0_t.Price
            ex_q0_t_1.index = q0_t_1.Price

            ex_q0= pd.concat([ex_q0_t,ex_q0_t_1],axis=1)
            ex_q0 = ex_q0.fillna(0)
            ex_q0.columns = ["t","t-1"]
            ex_q0 = ex_q0["t"] - ex_q0["t-1"]
            ex_q0 = ex_q0.sort_index(ascending=False)
            ex_q0 = pd.DataFrame([ex_q0.index,ex_q0.values]).T
            ex_q0.index = [index] * len(ex_q0)
            ex_q0.columns = ["Price","Amount"]
            #########################################################

            ################価格↑ distance↑###########################
            ex_q1_t = copy.copy(q1_t.Amount)
            ex_q1_t_1 = copy.copy(q1_t_1.Amount)
            ex_q1_t.index = q1_t.Price
            ex_q1_t_1.index = q1_t_1.Price

            ex_q1= pd.concat([ex_q1_t,ex_q1_t_1],axis=1)
            ex_q1 = ex_q1.fillna(0)
            ex_q1.columns = ["t","t-1"]
            ex_q1 = -1*(ex_q1["t"] - ex_q1["t-1"])
            ex_q1 = ex_q1.sort_index(ascending=False)
            ex_q1 = pd.DataFrame([ex_q1.index,ex_q1.values]).T
            ex_q1.index = [index] * len(ex_q1)
            ex_q1.columns = ["Price","Amount"]
            ##########################################################



            #板の状態　df_q
            #q1_t_sorted = q1_t.sort_values(by = "Distance",ascending=False)
            #q_t = pd.concat([q1_t_sorted , q0_t])

            q_t = pd.concat([ex_q1,ex_q0])

            #本質的にはいらない
            q_t.index = [index]*len(q_t)

            ex_df = pd.DataFrame(index = [index] , columns=[i for i in range(-1*n, n + 1, 1)])
            ex_df = ex_df.fillna(0)
            ex_df.columns = list(map(lambda x : "t_"+str(x),ex_df.columns))

            for j in range(len(q_t)):
                try:
                    c = price_list.index(q_t.Price[j])
                    ex_df.iloc[0,c] = q_t.Amount[j]

                except ValueError:
                    pass

            
            ###target3###
            #time + time_horizen ~ time+2time_horizen 後の一番近い約定を目的変数に
    #            condition1 = duplicated_d[(start_index <= duplicated_d.index) &(duplicated_d.index > index )& (duplicated_d.index <= index + forward * time_horizen) ].Price
            condition2 = duplicated_d[(start_index <= duplicated_d.index) &(duplicated_d.index >= index + forward * time_horizen ) & (duplicated_d.index < index + 2 * forward * time_horizen) ].Price
            if len(condition2) > 0:
                pre = condition2[0]
            elif len(condition2) == 0:
                pre = d_price
            #############


            if (pre - d_price) > 0 :
                direction = 1

            elif (pre - d_price) == 0:
                direction = 0

            elif (pre - d_price) < 0:
                direction = -1


            test_df = pd.DataFrame(index = [index],columns = ["direction","t+1_d","t_d","t_1_d","t_2_d","t_3_d","t_4_d","t_5_d"])
            test_df["direction"] = direction
            test_df["t+1_d"] = pre
            test_df["t_d"] = d_price
            test_df["t_1_d"] = d_price_t_1
            test_df["t_2_d"] = d_price_t_2
            test_df["t_3_d"] = d_price_t_3
            test_df["t_4_d"] = d_price_t_4
            test_df["t_5_d"] = d_price_t_5

            ex_df_t = pd.concat([test_df,ex_df,ex_df_t_1,ex_df_t_2,ex_df_t_3,ex_df_t_4,ex_df_t_5], axis=1)
            info_orderboard = pd.concat([info_orderboard,ex_df_t])
            ex_df_t_5 = copy.copy(ex_df_t_4)
            ex_df_t_5.index = [index+time_horizen]
            ex_df_t_5.columns = t_5_columns
            ex_df_t_4 = copy.copy(ex_df_t_3)
            ex_df_t_4.index = [index+time_horizen]
            ex_df_t_4.columns = t_4_columns
            ex_df_t_3 = copy.copy(ex_df_t_2)
            ex_df_t_3.index = [index+time_horizen]
            ex_df_t_3.columns = t_3_columns
            ex_df_t_2 = copy.copy(ex_df_t_1)
            ex_df_t_2.index = [index+time_horizen]
            ex_df_t_2.columns = t_2_columns
            ex_df_t_1 = copy.copy(ex_df)
            ex_df_t_1.index = [index+time_horizen]
            ex_df_t_1.columns = t_1_columns

            d_price_t_5 = copy.copy(d_price_t_4)
            d_price_t_4 = copy.copy(d_price_t_3)
            d_price_t_3 = copy.copy(d_price_t_2)
            d_price_t_2 = copy.copy(d_price_t_1)
            d_price_t_1 = copy.copy(d_price)

        try:
            os.mkdir("D:\\[山口]研究\\YJFX\\february\\"+time+"\\")
        except FileExistsError:
            pass

        os.chdir("D:\\[山口]研究\\YJFX\\february\\"+time+"\\")
        info_orderboard.to_pickle(date +"_" + target + "_N" + str(n) + "_TW" + str(time) + "_F" + str(forward) + ".pickle")
