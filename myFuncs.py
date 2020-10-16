
import pandas as pd
import numpy as np

import warnings
warnings.filterwarnings('ignore')


def min_dict(freq):

    """
    将分钟索引标准化
    :param freq: 分钟频率
    :return:   标准化的分钟序列字典

    """
    day = '2016-02-01'
    df_1min=pd.DataFrame({'time':list(pd.date_range(day+' 09:30:00',day+' 11:30:00',freq='1min'))+list(pd.date_range(day+' 13:00:00',day+' 15:00:00',freq='1min'))})
    df_1min.time=df_1min.time.apply(lambda x:int(x.strftime('%H%M')))

    df=pd.DataFrame({'time_ls':list(pd.date_range(day+' 09:30:00',day+' 11:30:00',freq=freq))+list(pd.date_range(day+' 13:00:00',day+' 15:00:00',freq=freq))})
    df.time_ls=df.time_ls.apply(lambda x:int(x.strftime('%H%M')))

    df=df.merge(df_1min,left_on='time_ls',right_on='time',how='outer')
    df=df.sort_values('time')
    df.time_ls=df.time_ls.fillna(method='bfill')
    df=df.astype(int)
    df['i']=1
    df={x:v[1] for x,v in df.pivot(index='i',columns='time',values='time_ls').items()}

    return df


if __name__=='__main__':

    freq='10min'
    dd=min_dict(freq)

