"""
保存指数不同频率下的收益序列
"""

import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import seaborn as sns

plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号


def get_ret_intra(df_price):
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

    return df_ret

def get_ret_daily(df_price,freq):
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
    ret = ret.set_index('date')
    ret=ret.dropna(how='all')

    return ret

if __name__=='__main__':


    code_ls_map = {'000016.SH': '上证50', '000905.SH': '中证500', '000906.SH': '中证800',
                      '000852.SH': '中证1000', '399006.SZ': '创业板'}

    ret_dict={}
    #   1min
    df_1min = pd.read_csv('data_min_index/close_min1.csv', index_col=0)
    df_1min=df_1min.loc[(df_1min.date>=20100701)&(df_1min.date<=20200701),]
    ret_dict['1分钟']=get_ret_intra(df_1min)

    #   5min
    df_5min = pd.read_csv('data_min_index/close_min5.csv', index_col=0)
    df_5min=df_5min.loc[(df_5min.date>=20100701)&(df_5min.date<=20200701),]
    ret_dict['5分钟'] =get_ret_intra(df_5min)

    #   10min
    df_10min = pd.read_csv('data_min_index/close_min10.csv', index_col=0)
    df_10min=df_10min.loc[(df_10min.date>=20100701)&(df_10min.date<=20200701),]
    ret_dict['10分钟'] =get_ret_intra(df_10min)

    #   30min
    df_30min=pd.read_csv('data_min_index/close_min30.csv',index_col=0)
    df_30min=df_30min.loc[(df_30min.date>=20100701)&(df_30min.date<=20200701),]
    ret_dict['30分钟'] =get_ret_intra(df_30min)

    #   60min
    df_1h=pd.read_csv('data_min_index/close_min60.csv',index_col=0)
    df_1h=df_1h.loc[(df_1h.date>=20100701)&(df_1h.date<=20200701),]
    ret_dict['1小时'] =get_ret_intra(df_1h)

    #   daily
    df_1d = pd.read_csv('data_min_index/指数日行情.csv', index_col=0)
    df_1d = df_1d[['S_INFO_WINDCODE', 'TRADE_DT', 'S_DQ_CLOSE']].reset_index(drop=True)
    df_1d.columns = ['code', 'date', 'price']
    df_1d = df_1d.pivot(index='date', columns='code', values='price')
    df_1d = df_1d.sort_index()
    df_1d=df_1d.loc[(df_1d.index>=20100701)&(df_1d.index<=20200701),]
    df_1d.index = [pd.to_datetime(str(x)) for x in df_1d.index]
    ret_dict['日频'] =get_ret_daily(df_1d,'D')
    #   1 week
    ret_dict['周频'] =get_ret_daily(df_1d,'W')
    #   2 week
    ret_dict['双周'] =get_ret_daily(df_1d,'2W')
    #   1 month
    ret_dict['月频'] =get_ret_daily(df_1d,'M')
    #   季度
    ret_dict['季频'] =get_ret_daily(df_1d,'Q')
    #   1 year
    ret_dict['年频'] =get_ret_daily(df_1d,'Y')

    for code in code_ls_map.keys():
        writer = pd.ExcelWriter('vola0831/ret_df_{}.xlsx'.format(code_ls_map[code]))
        for freq in ret_dict.keys():
            df=ret_dict[freq][code].reset_index()
            df=df.dropna()
            df.to_excel(writer,freq)
        writer.save()



