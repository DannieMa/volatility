

from jq_data_capi import *
import pandas as pd
import math
import numpy as np
from myFuncs import min_dict

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
    def trade_days(self,start_date,end_date):
        day_ls=self.instance.get_trade_days(start_date, end_date)
        day_ls=[d.decode() for d in day_ls]
        return day_ls

    def get_min_df(self,symbol,start_time,end_time,fq,freq):

        min_data=self.instance.get_minute_price(symbol, start_time, end_time,fq,freq)
        df=[[x.time,x.close] for x in min_data]
        df=pd.DataFrame(df,columns=['time',symbol])
        df['date']=df.time//1000000000
        df.time = df.time % 1000000000 // 100000
        min_norm=min_dict('%dmin'%freq)
        df.time=df.time.apply(lambda x: min_norm[x])

        return df[['date','time',symbol]]


if __name__=='__main__':

    start_d = '2010-01-01'
    end_d = '2020-07-01'

    start_t=' 09:00:00'
    end_t=' 15:00:00'

    ebq_sample=ebq_data()
    day_ls = ebq_sample.trade_days(start_d, end_d)
    index_ls=['000016.XSHG',  '000905.XSHG', '000906.XSHG', '000852.XSHG','399006.XSHE']
    freq_ls=[1,5,10,30,60]

    for freq in freq_ls:
        para_df=pd.DataFrame()
        for symbol in index_ls:
            min_df = ebq_sample.get_min_df(symbol, start_d + start_t, end_d + end_t, 0, freq)
            if len(para_df)==0:
                para_df=min_df
            else:
                para_df=para_df.merge(min_df,on=['date','time'],how='outer')
            print(symbol)

        para_df=para_df.sort_values(['date','time'])
        para_df.to_csv('data_min_index/close_min%d.csv'%freq)
        print(freq)

