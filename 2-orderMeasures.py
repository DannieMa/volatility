
from jq_data_capi import *
import pandas as pd
import numpy as np
import itertools
from multiprocessing import Pool
import time
import statsmodels.api as sm
import warnings

warnings.filterwarnings('ignore')

class ebq_data(object):
    instance = jq_data()

    def __init__(self, ip_addr="10.84.137.198", port=7000, login_info=["13100000002", "gdzq12345"]):
        result = self.instance.connect(ip_addr, port)
        # print (time.asctime(), ": connecting.......")
        if result < 0:
            print('connection failed.')
        else:
            pass
        self.instance.is_connected()
        l_result = self.instance.login(login_info[0], login_info[1])
        if l_result != 0:
            print('login failed.')
        else:
            pass
    def get_trade_df(self, symbol, start_time, end_time):
        order_data = self.instance.get_trades(symbol, start_time, end_time)
        df = []
        for x in order_data:
            df.append([x.time, x.index, x.bsc.decode(), x.price, x.volume, x.ask, x.bid])
        df = pd.DataFrame(df, columns=['time', 'index', 'bsc', 'price', 'volume', 'ask', 'bid'])
        df['date'] = df.time.apply(lambda x: int(str(x)[:8]))
        df['code'] = symbol
        return df

def get_proxy(day):
    """
    基于逐笔交易数据计算订单执行相关指标
    :param df_trade:
    :return:
    """
    df_trades = trade_df.loc[trade_df.date == day,]
    ask_orders = df_trades[['ask', 'bsc', 'volume','time']]
    ask_orders = ask_orders.loc[ask_orders.ask > 0,]
    ask_orders = ask_orders.rename(columns={'ask': 'order_no'})

    bid_orders = df_trades[['bid', 'bsc', 'volume','time']]
    bid_orders = bid_orders.loc[bid_orders.bid > 0,]
    bid_orders = bid_orders.rename(columns={'bid': 'order_no'})

    df_orders = ask_orders.append(bid_orders)  # 订单数据，已订单提交序号为索引

    df_volume = df_orders[['order_no', 'bsc', 'volume']].groupby(['order_no', 'bsc']).sum()
    df_volume = df_volume.reset_index().pivot(index='order_no', columns='bsc', values='volume')
    df_volume = df_volume.fillna(0)
    df_volume['volume'] = df_volume.B + df_volume.S + df_volume.C
    df_volume['c_pct'] = df_volume.C / df_volume.volume
    #   成交订单规模
    size_mean = df_trades.loc[df_trades.bsc != 'C', 'volume'].mean()
    size_std = df_trades.loc[df_trades.bsc != 'C', 'volume'].std()
    #   订单取消率
    cancel_pct = len(df_volume.loc[df_volume.c_pct == 1,]) / len(df_volume)
    #   订单完成度
    exe_orders =1-df_volume.c_pct
    exe_mean = exe_orders.mean()
    exe_std = exe_orders.std()
    #   订单执行粒度
    df_num = df_orders.loc[df_orders.bsc!='C',('order_no','volume')].groupby('order_no').count()
    num_mean=df_num.volume.mean()
    num_std=df_num.volume.std()
    #   订单执行时间
    df_time = df_orders.loc[df_orders.bsc!='C',('order_no','time')].groupby('order_no').agg(['max','min'])
    df_time=df_time.reset_index()
    df_time.columns=['order_no','end_t','start_t']
    #df_time['time']=[(pd.to_datetime(str(x)[:-3])-pd.to_datetime(str(y)[:-3])).seconds for x,y in zip(df_time.end_t,df_time.start_t) ]
    df_time['time']=(df_time.end_t-df_time.start_t)%1000
    time_mean=df_time.time.mean()
    time_std=df_time.time.std()

    return [day,size_mean,size_std,cancel_pct,exe_mean,exe_std,num_mean,num_std,time_mean,time_std]



start_d = '2016-01-01'
end_d = '2016-01-31'

start_t = ' 09:30:00'
end_t = ' 14:57:00'
ebq = ebq_data()
stock_ls = [x.decode() for x in ebq.instance.get_all_securities(Stock)]

for code in stock_ls:
    print('read at: ', time.asctime())
    trade_df = ebq.get_trade_df(code, start_d + start_t, end_d + end_t)
    trade_df.bsc=trade_df.bsc.fillna('C')
    trade_df.loc[trade_df.bsc == '\x00', 'bsc'] = 'C'
    break
    trade_df.to_csv('processData/%s_sample.csv'%code)
    # PROCESS = 6
    # day_ls = list(set(trade_df.date))
    #
    # print('start at: ', time.asctime())
    # para_ls=[]
    # # r=get_proxy(day_ls[0])
    # pool = Pool(PROCESS)
    # #para_ls=pool.map(get_proxy,day_ls[:20])
    # for day in day_ls:
    #     r=pool.apply(get_proxy, day).get()
    # pool.close()
    # pool.join()
    # print('end at: ', time.asctime())
    #
    # para_df=pd.DataFrame(para_ls,columns=['day','size_mean','size_std','cancel_pct','exe_mean','exe_std','num_mean','num_std','time_mean','time_std'])
    # para_df['code']=code
    # para_df.to_csv('order_measures_by_code/%s.csv'%code)
    # break