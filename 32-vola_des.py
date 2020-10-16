"""
指数在不同频率下收益序列的波动率
"""

import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import seaborn as sns

plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

def getVola_intra(df_price,n):
    '''

    :param df_price:
    :return:
    '''

    df_price=df_price.set_index(['date','time'])
    df_price=df_price.sort_index()
    df_price[df_price > 1000000] /= 10000
    df_price=df_price.replace(0,np.nan)

    df_ret=df_price/df_price.shift()-1
    df_ret=df_ret.dropna(how='all')
    df_ret.columns=[x.replace('XSHG','SH').replace('XSHE','SZ') for x in df_ret.columns]
    vola=df_ret.std()*np.sqrt(n)
    return vola

def getVola_daily(df_price,freq,n):
    """

    :param df_price:
    :param freq:
    :param n:
    :return:
    """
    df_price = df_price.resample(freq).last()
    df_price = df_price.dropna(how='all')

    ret = df_price / df_price.shift() - 1
    ret['date'] = [int(x.strftime('%Y%m%d')) for x in ret.index]
    ret=ret.loc[(ret.date>=20110701)&(ret.date<=20200701),]
    ret = ret.set_index('date')
    ret=ret.dropna(how='all')
    vola=ret.std()*np.sqrt(n)

    return vola

if __name__=='__main__':

    code_ls_map = {'000016.SH': '上证50', '000905.SH': '中证500', '000906.SH': '中证800',
                      '000852.SH': '中证1000', '399006.SZ': '创业板'}

    para_df=[]
    #   1min
    df_1min = pd.read_csv('data_min_index/close_min1.csv', index_col=0)
    df_1min=df_1min.loc[(df_1min.date>=20110701)&(df_1min.date<=20200701),]
    vola_1min_df = pd.DataFrame(getVola_intra(df_1min, 240 * 240),columns=['1分钟'])

    #   5min
    df_5min = pd.read_csv('data_min_index/close_min5.csv', index_col=0)
    df_5min=df_5min.loc[(df_5min.date>=20110701)&(df_5min.date<=20200701),]
    vola_5min_df = pd.DataFrame(getVola_intra(df_5min, 240 * 48),columns=['5分钟'])

    #   10min
    df_10min = pd.read_csv('data_min_index/close_min10.csv', index_col=0)
    df_10min=df_10min.loc[(df_10min.date>=20110701)&(df_10min.date<=20200701),]
    vola_10min_df = pd.DataFrame(getVola_intra(df_10min, 240 * 24),columns=['10分钟'])

    #   30min
    df_30min=pd.read_csv('data_min_index/close_min30.csv',index_col=0)
    df_30min=df_30min.loc[(df_30min.date>=20110701)&(df_30min.date<=20200701),]
    vola_30min_df = pd.DataFrame(getVola_intra(df_30min, 240 * 8),columns=['30分钟'])

    #   60min
    df_1h=pd.read_csv('data_min_index/close_min60.csv',index_col=0)
    df_1h=df_1h.loc[(df_1h.date>=20100701)&(df_1h.date<=20200701),]
    vola_1h_df = pd.DataFrame(getVola_intra(df_1h, 240 * 4),columns=['1小时'])

    #   daily
    df_1d = pd.read_csv('data_min_index/指数日行情.csv', index_col=0)
    df_1d = df_1d[['S_INFO_WINDCODE', 'TRADE_DT', 'S_DQ_CLOSE']].reset_index(drop=True)
    df_1d.columns = ['code', 'date', 'price']
    df_1d = df_1d.pivot(index='date', columns='code', values='price')
    df_1d = df_1d.sort_index()
    df_1d=df_1d.loc[(df_1d.index>=20100701)&(df_1d.index<=20200701),]
    df_1d.index = [pd.to_datetime(str(x)) for x in df_1d.index]
    vola_1d_df=pd.DataFrame(getVola_daily(df_1d,'D',240),columns=['日频'])

    #   1 week
    vola_1w_df = pd.DataFrame(getVola_daily(df_1d, 'W', 48),columns=['周频'])
    #   2 week
    vola_2w_df = pd.DataFrame(getVola_daily(df_1d, '2W', 24),columns=['双周'])
    #   1 month
    vola_1m_df = pd.DataFrame(getVola_daily(df_1d, '1M', 12),columns=['月频'])
    #   季度
    vola_1m_df = pd.DataFrame(getVola_daily(df_1d, 'Q', 4),columns=['季频'])
    #   1 year
    vola_1y_df = pd.DataFrame(getVola_daily(df_1d, '1Y', 1),columns=['年频'])

    vola_df=pd.concat([vola_1min_df,vola_5min_df,vola_10min_df,vola_30min_df,vola_1h_df,vola_1d_df,vola_1w_df,vola_2w_df,vola_1m_df,vola_1y_df],axis=1)

    vola_df.index=[code_ls_map[x]for x in vola_df.index]
    vola_df.to_excel('vola0831/vola_df.xlsx')


