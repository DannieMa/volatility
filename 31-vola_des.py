
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import seaborn as sns

plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

def reshape_time_bar(df_price,time_bar):
    '''
    统一时间标签
    :param df_price: 价格序列
    :param time_bar: 时间标签，eg. 小时频率，time_bar=[1100,1200,1400,1500]
    :return: 统一化之后的df_price
    '''

    df_price=df_price.set_index(['date','time'])
    df_price=df_price.replace(0,np.nan)
    df_price[df_price>1000000]/=10000
    df_price=df_price.stack().reset_index()
    df_price.columns=['date','time','code','price']
    df_new=pd.pivot_table(df_price,index=['date','code'],columns='time',values='price')
    df_new=pd.DataFrame(df_new.apply(lambda x:pd.Series(x.dropna().to_list()),axis=1))
    df_new=df_new[df_new.columns[:len(time_bar)]]
    df_new.columns=time_bar
    df_new=df_new.stack().reset_index()
    df_new.columns=['date','code','time','price']
    df_new=pd.pivot_table(df_new,index=['date','time'],columns='code',values='price')
    df_new=df_new.reset_index()

    return df_new


def getVola_daily(df_price,period,freq,n):
    '''
    计算年化收益波动
    :param df_ret: 价格序列
    :param period: rolling(period)
    :param freq: resample(freq)
    :param n: 年化频率。eg. 1小时频率，n=240*4
    :return:
    '''

    df_price=df_price.sort_values(['date','time'])
    df_price.date=(df_price.date.apply(str)+df_price.time.apply(str)).apply(lambda x:pd.to_datetime(x))
    df_price=df_price.drop(['time'],axis=1)
    df_price=df_price.set_index('date')
    df_ret=df_price/df_price.shift()-1

    vola=df_ret.rolling(period).std().resample(freq).last()*np.sqrt(n)
    vola=vola.dropna(how='all')
    vola=vola.reset_index()
    vola.date=vola.date.apply(lambda x:int(x.strftime('%Y%m%d')))
    vola=vola.set_index('date')
    vola.columns=[x.replace('XSHG','SH').replace('XSHE','SZ') for x in vola.columns]

    vola=vola.reset_index()
    vola['month']=vola.date.apply(lambda x:str(x)[:6])
    vola=vola.groupby('month').last().set_index('date')

    month_ls=[int(x.strftime('%Y%m%d')) for x in pd.date_range(start='31/1/2004', end='31/12/2020', freq='M') ]
    month_dict={str(x)[:6]:x for x in month_ls}
    vola['date']=[month_dict[str(x)[:6]] for x in vola.index]
    vola=vola.set_index('date')

    return vola

def getVola_month(df_ret,period,n):
    '''
    计算月频波动
    :param df_price:
    :param period:
    :param freq:
    :param n:
    :return:
    '''
    vola=df_ret.rolling(period).std()*np.sqrt(n)
    vola=vola.reset_index()
    vola['month']=vola.date.apply(lambda x:str(x)[:6])
    vola=vola.groupby('month').last().set_index('date')
    vola=vola.dropna(how='all')

    month_ls=[int(x.strftime('%Y%m%d')) for x in pd.date_range(start='31/1/2004', end='31/12/2020', freq='M') ]
    month_dict={str(x)[:6]:x for x in month_ls}
    vola['date']=[month_dict[str(x)[:6]] for x in vola.index]
    vola=vola.set_index('date')

    return vola

def getVola_intra(df_price,n):
    '''

    :param df_price:
    :return:
    '''

    df_price=df_price.set_index(['date','time'])
    df_price[df_price > 1000000] /= 10000
    df_price=df_price.stack().reset_index()
    df_price.columns=['date','time','code','price']
    df_price=pd.pivot_table(df_price,index=['date','code'],columns='time',values='price')
    df_price[900]=0
    df_price=df_price.stack().reset_index()
    df_price.columns=['date','code','time','price']
    df_price=pd.pivot_table(df_price,index=['date','time'],columns='code',values='price')
    df_price=df_price.sort_index()
    df_price=df_price.replace(0,np.nan)

    df_ret=df_price/df_price.shift()-1
    df_ret=df_ret.dropna(how='all')
    df_ret=df_ret.reset_index()
    df_ret=df_ret.drop(['time'],axis=1)

    vola=df_ret.groupby('date').std()*np.sqrt(n)
    vola=vola.dropna(how='all')
    vola.columns=[x.replace('XSHG','SH').replace('XSHE','SZ') for x in vola.columns]

    vola=vola.reset_index()
    vola['month']=vola.date.apply(lambda x:str(x)[:6])
    vola=vola.groupby('month').last().set_index('date')

    month_ls=[int(x.strftime('%Y%m%d')) for x in pd.date_range(start='31/1/2004', end='31/12/2020', freq='M') ]
    month_dict={str(x)[:6]:x for x in month_ls}
    vola['date']=[month_dict[str(x)[:6]] for x in vola.index]
    vola=vola.set_index('date')

    return vola

def save_vola_daily():
    """
    根据日内分钟收益，计算日度波动率
    分钟收益：1min, 5min, 10min, 30min, 1h
    :return:
    """
    para_df=[]

    #   1min
    df_1min = pd.read_csv('data_min_index/close_min1.csv', index_col=0)
    vola_1min_df = getVola_intra(df_1min, 240 * 240)
    arrays = [['vola_1min']*len(vola_1min_df.columns), vola_1min_df.columns.to_list()]
    vola_1min_df.columns= pd.MultiIndex.from_arrays(arrays, names=('vola_proxy', 'code'))
    para_df.append(vola_1min_df)

    #   5min
    df_5min = pd.read_csv('data_min_index/close_min5.csv', index_col=0)
    vola_5min_df = getVola_intra(df_5min, 240 * 48)
    arrays = [['vola_5min']*len(vola_5min_df.columns), vola_5min_df.columns.to_list()]
    vola_5min_df.columns= pd.MultiIndex.from_arrays(arrays, names=('vola_proxy', 'code'))
    para_df.append(vola_5min_df)

    #   10min
    df_10min = pd.read_csv('data_min_index/close_min10.csv', index_col=0)
    vola_10min_df = getVola_intra(df_10min, 240 * 24)
    arrays = [['vola_10min']*len(vola_10min_df.columns), vola_10min_df.columns.to_list()]
    vola_10min_df.columns= pd.MultiIndex.from_arrays(arrays, names=('vola_proxy', 'code'))
    para_df.append(vola_10min_df)

    #   30min
    df_30min=pd.read_csv('data_min_index/close_min30.csv',index_col=0)
    df_30min=reshape_time_bar(df_30min,[1000,1030,1100,1130,1330,1400,1430,1500])
    vola_30min_df=getVola_daily(df_30min,20,'d',240*8)
    arrays = [['vola_30min']*len(vola_30min_df.columns), vola_30min_df.columns.to_list()]
    vola_30min_df.columns= pd.MultiIndex.from_arrays(arrays, names=('vola_proxy', 'code'))
    para_df.append(vola_30min_df)

    #   1h
    df_1h=pd.read_csv('data_min_index/close_min60.csv',index_col=0)
    df_1h=reshape_time_bar(df_1h,[1100,1200,1400,1500])
    vola_1h_df=getVola_daily(df_1h,20,'d',240*4)
    arrays = [['vola_1h']*len(vola_1h_df.columns), vola_1h_df.columns.to_list()]
    vola_1h_df.columns= pd.MultiIndex.from_arrays(arrays, names=('vola_proxy', 'code'))
    para_df.append(vola_1h_df)

    return pd.concat(para_df,axis=1)

def save_vola_monthly():
    """
    根据日度收益，计算月度波动率
    日度收益：daily, 1 week, 2 week, 1 month
    :return:
    """
    para_df=[]

    #   daily
    df_1d = pd.read_csv('data_min_index/指数日行情.csv', index_col=0)
    df_1d = df_1d[['S_INFO_WINDCODE', 'TRADE_DT', 'S_DQ_CLOSE']].reset_index(drop=True)
    df_1d.columns = ['code', 'date', 'price']
    df_1d = df_1d.pivot(index='date', columns='code', values='price')
    df_1d = df_1d.sort_index()
    ret_1d = df_1d / df_1d.shift() - 1
    vola_1d_df = getVola_month(ret_1d, 20, 240)
    arrays = [['vola_1d']*len(vola_1d_df.columns), vola_1d_df.columns.to_list()]
    vola_1d_df.columns= pd.MultiIndex.from_arrays(arrays, names=('vola_proxy', 'code'))
    para_df.append(vola_1d_df)

    #   week
    df_1d.index = [pd.to_datetime(str(x)) for x in df_1d.index]
    df_1w = df_1d.resample('W').last()
    df_1w = df_1w.dropna(how='all')

    ret_1w = df_1w / df_1w.shift() - 1
    ret_1w['date'] = [int(x.strftime('%Y%m%d')) for x in ret_1w.index]
    ret_1w = ret_1w.set_index('date')
    vola_1w_df = getVola_month(ret_1w, 20, 48)
    arrays = [['vola_1w']*len(vola_1w_df.columns), vola_1w_df.columns.to_list()]
    vola_1w_df.columns= pd.MultiIndex.from_arrays(arrays, names=('vola_proxy', 'code'))
    para_df.append(vola_1w_df)

    #   2 week
    df_2w = df_1d.resample('2W').last()
    df_2w = df_2w.dropna(how='all')

    ret_2w = df_2w / df_2w.shift() - 1
    ret_2w['date'] = [int(x.strftime('%Y%m%d')) for x in ret_2w.index]
    ret_2w = ret_2w.set_index('date')
    vola_2w_df = getVola_month(ret_2w, 20, 24)
    arrays = [['vola_2w']*len(vola_2w_df.columns), vola_2w_df.columns.to_list()]
    vola_2w_df.columns= pd.MultiIndex.from_arrays(arrays, names=('vola_proxy', 'code'))
    para_df.append(vola_2w_df)

    #   1 month
    df_1m = df_1d.resample('m').last()
    df_1m = df_1m.dropna(how='all')

    ret_1m = df_1m / df_1m.shift() - 1
    ret_1m['date'] = [int(x.strftime('%Y%m%d')) for x in ret_1m.index]
    ret_1m = ret_1m.set_index('date')
    vola_1m_df = getVola_month(ret_1m, 20, 12)
    arrays = [['vola_1m']*len(vola_1m_df.columns), vola_1m_df.columns.to_list()]
    vola_1m_df.columns= pd.MultiIndex.from_arrays(arrays, names=('vola_proxy', 'code'))
    para_df.append(vola_1m_df)

    return pd.concat(para_df,axis=1)

def save_vola_yearly():
    """
    计算年频波动
    收益： quarterly, 1 year, 2 year, 3 year .... 8 year
    :return:
    """

    para_df=[]
    #    quarterly
    df_1d = pd.read_csv('data_min_index/指数日行情.csv', index_col=0)
    df_1d = df_1d[['S_INFO_WINDCODE', 'TRADE_DT', 'S_DQ_CLOSE']].reset_index(drop=True)
    df_1d.columns = ['code', 'date', 'price']
    df_1d = df_1d.pivot(index='date', columns='code', values='price')
    df_1d = df_1d.sort_index()

    df_1d.index=[pd.to_datetime(str(x))for x in df_1d.index]
    df_q=df_1d.resample('Q').last()
    df_q.index=[int(x.strftime('%Y%m%d'))for x in df_q.index]
    ret_q=df_q/df_q.shift()-1   #季度收益
    vola_q_df=ret_q.rolling(20).std()*np.sqrt(4)
    arrays = [['vola_q'] * len(vola_q_df.columns), vola_q_df.columns.to_list()]
    vola_q_df.columns = pd.MultiIndex.from_arrays(arrays, names=('vola_proxy', 'code'))
    para_df.append(vola_q_df)

    df_1y=df_1d.resample('Y').last()
    df_1y.index=[int(x.strftime('%Y%m%d'))for x in df_1y.index]
    ret_1y=df_1y/df_1y.shift()-1   #年度收益
    ret_1y=ret_1y.loc[ret_1y.index>=20100101,]
    vola_1y_df=pd.DataFrame(ret_1y.std()).T
    vola_1y_df.index=[vola_q_df.index[-1]]
    arrays = [['vola_1y'] * len(vola_1y_df.columns), vola_1y_df.columns.to_list()]
    vola_1y_df.columns = pd.MultiIndex.from_arrays(arrays, names=('vola_proxy', 'code'))
    para_df.append(vola_1y_df)

    return pd.concat(para_df,axis=1)


if __name__=='__main__':

    code_ls_map = {'000016.SH': '上证50', '000905.SH': '中证500', '000906.SH': '中证800',
                      '000852.SH': '中证1000', '399006.SZ': '创业板'}
    proxy_ls_map={'vola_10min':'10分钟', 'vola_1h':'1小时', 'vola_1min':'1分钟', 'vola_30min':'30分钟', 'vola_5min':'5分钟',
                  'vola_1d':'日频', 'vola_1m':'月频', 'vola_1w':'周频', 'vola_2w':'双周',
                  'vola_1y':'年频', 'vola_q':'季频'}
    proxy_ls=['1分钟','5分钟','10分钟','30分钟','1小时','日频','周频','双周','月频','季频','年频']

    data_path='vola/vola_mean.xlsx'
    para=[]
    vola_daily = save_vola_daily()
    vola_daily = vola_daily.loc[vola_daily.index >= 20100101,]
    para.append(vola_daily.mean().reset_index().pivot(index='code',columns='vola_proxy',values=0))

    vola_monthly = save_vola_monthly()
    vola_monthly = vola_monthly.loc[vola_monthly.index >= 20100101,]
    para.append(vola_monthly.mean().reset_index().pivot(index='code',columns='vola_proxy',values=0))

    vola_yearly = save_vola_yearly()
    vola_yearly = vola_yearly.loc[vola_yearly.index >= 20100101,]
    para.append(vola_yearly.mean().reset_index().pivot(index='code',columns='vola_proxy',values=0))

    para_df=pd.concat(para,axis=1)
    para_df.columns = [proxy_ls_map[x] for x in para_df.columns]
    para_df = para_df[proxy_ls]
    para_df.index=[code_ls_map[x]for x in para_df.index]
    para_df.to_excel(data_path)
    for code in para_df.index:
        para_df.loc[code].plot.bar(title='%s指数不同收益频率下的年化波动率'% code)
        plt.savefig('vola/vola_plot_%s.jpg'%code)
        plt.close('all')

