"""
订单行为描述
    订单完成度
    订单持续事件
    订单取消率
"""
import pandas as pd
import numpy as np
import time

def get_proxy(df_trade):
    """
    基于逐笔交易数据计算订单执行相关指标
    :param df_trade:
    :return:
    """

    ask_orders = df_trades[['ask', 'bsc', 'volume','time','date']]
    ask_orders = ask_orders.loc[ask_orders.ask > 0,]
    ask_orders = ask_orders.rename(columns={'ask': 'order_no'})

    bid_orders = df_trades[['bid', 'bsc', 'volume','time','date']]
    bid_orders = bid_orders.loc[bid_orders.bid > 0,]
    bid_orders = bid_orders.rename(columns={'bid': 'order_no'})

    df_orders = ask_orders.append(bid_orders)  # 订单数据，已订单提交序号为索引

    df_volume = df_orders[['date','order_no', 'bsc', 'volume']].groupby(['date','order_no', 'bsc']).sum()
    df_volume = df_volume.reset_index()
    df_volume=pd.pivot_table(df_volume,index=['date','order_no'], columns='bsc', values='volume')
    df_volume = df_volume.fillna(0)
    df_volume['volume'] = df_volume.B + df_volume.S + df_volume.C
    df_volume['c_pct'] = df_volume.C / df_volume.volume
    df_volume=df_volume.reset_index()
    #   成交订单规模
    size_mean = df_trades.loc[df_trades.bsc != 'C',('date','volume')].groupby('date').mean()
    size_std = df_trades.loc[df_trades.bsc != 'C', ('date','volume')].groupby('date').std()
    #   订单取消率
    cancel_pct =df_volume.loc[df_volume.c_pct == 1,('date','c_pct')].groupby('date').count()/(df_volume[['date','c_pct']].groupby('date').count())
    #   订单完成度
    df_volume['exe_ratio']=1-df_volume.c_pct
    exe_mean = df_volume[['date','exe_ratio']].groupby('date').mean()
    exe_std = df_volume[['date','exe_ratio']].groupby('date').std()
    #   订单执行粒度
    df_num = df_orders.loc[df_orders.bsc!='C',('date','order_no','volume')].groupby(['date','order_no']).count()
    df_num=df_num.reset_index()
    num_mean=df_num[['date','volume']].groupby('date').mean()
    num_std=df_num[['date','volume']].groupby('date').std()

    #   订单执行时间
    # df_time = df_orders.loc[df_orders.bsc!='C',('date','order_no','time')].groupby(['date','order_no']).agg(['max','min'])
    # df_time.columns=['end_t','start_t']
    # df_time = df_time.reset_index()
    # #df_time['time']=[(pd.to_datetime(str(x)[:-3])-pd.to_datetime(str(y)[:-3])).seconds for x,y in zip(df_time.end_t, df_time.start_t)]
    # df_time['time']=df_time.end_t//1000-df_time.start_t//1000
    # time_mean=df_time.time.mean()
    # time_std=df_time.time.std()

    para=pd.DataFrame({'size_mean':size_mean.volume,'size_std':size_std.volume,'cancel_pct':cancel_pct.c_pct,'exe_mean':exe_mean.exe_ratio,
                  'exe_std':exe_std.exe_ratio,'num_mean':num_mean.volume,'num_std':num_std.volume})
    para=para.reset_index()
    return para

if __name__ == '__main__':

    df_trades = pd.read_csv('processData/000100.XSHE_sample.csv', index_col=0)  # 逐笔成交数据
    df_trades.bsc = df_trades.bsc.fillna('C')

    print('start at: ', time.asctime())

    order_proxy=get_proxy(df_trades)

    print('end at: ', time.asctime())

    order_proxy.to_csv('processData/orders_sample_all.csv')
