'''

数据读取测试

'''

from jq_data_capi import *
import pandas as pd
import numpy as np

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

    def get_lv2_data(self,symbol, start_time, end_time):
        tick_data = self.instance.get_stock_ticks(symbol, start_time, end_time)
        tick_data = [x for x in tick_data if x.time % 1000000000 > 93000000 and x.current > 0]
        lv2_data = [[x.time, x.current,x.volume,x.bought.a10v, x.bought.a9v, x.bought.a8v, x.bought.a7v, x.bought.a6v, x.bought.a5v,
                     x.bought.a4v, x.bought.a3v, x.bought.a2v, x.bought.a1v, x.bought.b1v, x.bought.b2v, x.bought.b3v,
                     x.bought.b4v, x.bought.b5v, x.bought.b6v, x.bought.b7v, x.bought.b8v, x.bought.b9v, x.bought.b10v,
                     x.bought.a10p, x.bought.a9p, x.bought.a8p, x.bought.a7p, x.bought.a6p, x.bought.a5p,
                     x.bought.a4p, x.bought.a3p, x.bought.a2p, x.bought.a1p, x.bought.b1p, x.bought.b2p, x.bought.b3p,
                     x.bought.b4p, x.bought.b5p, x.bought.b6p, x.bought.b7p, x.bought.b8p, x.bought.b9p, x.bought.b10p]
                    for x in tick_data]
        lv2_columns = ['time','price','volume'] + ['av{}'.format(x) for x in range(10, 0, -1)] + ['bv{}'.format(x) for x in range(1, 11)]+ ['ap{}'.format(x) for x in range(10, 0, -1)] + ['bp{}'.format(x) for x in range(1, 11)]
        lv2_frame = pd.DataFrame(lv2_data, columns=lv2_columns)
        lv2_frame['date']=lv2_frame.time.apply(lambda x:int(str(x)[:8]))
        lv2_frame['code']=symbol
        return lv2_frame

    def trade_days(self,start_date,end_date):
        day_ls=self.instance.get_trade_days(start_date, end_date)
        day_ls=[d.decode() for d in day_ls]
        return day_ls

    def get_order_df(self,symbol,start_time,end_time):
        order_data=self.instance.get_orders(symbol,start_time,end_time)
        df=[]
        for x in order_data:
            df.append([x.time,x.side,x.price,x.volume,x.count,x.orders])
        df=pd.DataFrame(df,columns=['time','side','price','volume','num','orders'])
        df['date']=df.time.apply(lambda x:int(str(x)[:8]))
        df['code']=symbol
        return df

    def get_trade_df(self,symbol,start_time,end_time):
        order_data=self.instance.get_trades(symbol,start_time,end_time)
        df=[]
        for x in order_data:
            df.append([x.time,x.index,x.bsc.decode(),x.price,x.volume,x.ask,x.bid])
        df=pd.DataFrame(df,columns=['time','index','bsc','price','volume','ask','bid'])
        df['date']=df.time.apply(lambda x:int(str(x)[:8]))
        df['code']=symbol
        return df

    def get_min_df(self,symbol,start_time,end_time,fq,freq):

        min_data=self.instance.get_minute_price(symbol, start_time, end_time,fq,freq)
        df=[[x.time,x.open,x.close,x.volume,x.money] for x in min_data]
        df=pd.DataFrame(df,columns=['time','open','close','volume','money'])
        df['date']=df.time.apply(lambda x:int(str(x)[:8]))
        df['code']=symbol
        return df


if __name__=='__main__':

    start_d = '2016-01-01'
    end_d = '2020-07-15'

    start_t=' 09:30:00'
    end_t=' 15:00:00'

    symbol='600000.XSHG'

    ebq_sample=ebq_data()
    #data=ebq_sample.instance.get_orders_detail(symbol, start_d + start_t, end_d + end_t)


    day_ls = ebq_sample.trade_days(start_d, end_d)
    # order_df = ebq_sample.get_order_df(symbol, start_d + start_t, end_d + end_t)
    # order_df.to_csv('processData/order_data_sample.csv')
    # index_ls=['000016.XSHG', '000300.XSHG',  '000905.XSHG', '000906.XSHG', '000852.XSHG']
    # symbol='399006.XSHE'
    # for day in day_ls:
        # tick=[len(ebq_sample.instance.get_index_ticks(symbol,day+start_t, day+end_t)) for symbol in index_ls]
        # flagDF.append([day]+tick)

        # min1=len(ebq_sample.get_min_df(symbol,day+start_t, day+end_t,1,1))
        # tick = len(ebq_sample.instance.get_index_ticks(symbol, day + start_t, day + end_t))
        #flagDF.append([day,tick,min1])

        # tick_df=ebq_sample.get_lv2_data(symbol, day+start_t, day+end_t)
        # trade_df=ebq_sample.get_trade_df(symbol, day+start_t, day+end_t)
        # order_df=ebq_sample.get_order_df(symbol, day+start_t, day+end_t)
        #min10_df=ebq_sample.get_min_df(symbol, day+start_t, day+end_t,2,10)

        #flagDF.append([day,len(tick_df),len(trade_df),len(order_df)])
        # tick_df.to_csv('processData/tick_'+day+'.csv')
        # trade_df.to_csv('processData/trade_'+day+'.csv')
        # order_df.to_csv('processData/order_'+day+'.csv')
        # min10_df.to_csv('processData/min10_'+day+'.csv')

        # print(day)

    # flagDF=pd.DataFrame(flagDF,columns=['day']+index_ls)
    # flagDF.to_csv('processData/flag_tick_index.csv')
    #
    # flagDF=pd.DataFrame(flagDF,columns=['day','tick','min1'])
    # flagDF.to_csv('processData/flag_399006.csv')

    #temp=ebq_sample.instance.get_stock_ticks(symbol, start_time, end_time)
