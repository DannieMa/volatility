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

    ask_orders = df_trades[['ask', 'bsc', 'volume', 'time']]
    ask_orders = ask_orders.loc[ask_orders.ask > 0,]
    ask_orders = ask_orders.rename(columns={'ask': 'order_no'})

    bid_orders = df_trades[['bid', 'bsc', 'volume', 'time']]
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
    exe_orders = 1 - df_volume.c_pct
    exe_mean = exe_orders.mean()
    exe_std = exe_orders.std()
    #   订单执行粒度
    df_num = df_orders.loc[df_orders.bsc != 'C', ('order_no', 'volume')].groupby('order_no').count()
    num_mean = df_num.volume.mean()
    num_std = df_num.volume.std()
    # #   订单执行时间
    # df_time = df_orders.loc[df_orders.bsc != 'C', ('order_no', 'time')].groupby('order_no').agg(['max', 'min'])
    # df_time = df_time.reset_index()
    # df_time.columns = ['order_no', 'end_t', 'start_t']
    # # df_time['time']=[(pd.to_datetime(str(x)[:-3])-pd.to_datetime(str(y)[:-3])).seconds for x,y in zip(df_time.end_t,df_time.start_t) ]
    # df_time['time'] = (df_time.end_t - df_time.start_t) % 1000
    # time_mean = df_time.time.mean()
    # time_std = df_time.time.std()

    return [size_mean, size_std, cancel_pct, exe_mean, exe_std, num_mean, num_std]


if __name__ == '__main__':
    df_trades = pd.read_csv('processData/000100.XSHE_sample.csv', index_col=0)  # 逐笔成交数据
    df_trades.bsc = df_trades.bsc.fillna('C')
    df_trades=df_trades.loc[df_trades.date==20160105,]

    print('start at: ', time.time())
    order_proxy = [['000100.XSHE', 20160105] + get_proxy(df_trades)]
    print('end at: ', time.time())
    order_proxy=pd.DataFrame(order_proxy,columns=['code','date','size_mean',
                                                  'size_std', 'cancel_pct', 'exe_mean', 'exe_std', 'num_mean', 'num_std'])
    order_proxy.to_csv(('processData/orders_sample.csv'))