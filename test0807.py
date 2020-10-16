
from jq_include import *
from jq_data_capi import *
from jq_callback_capi import *
from jq_playback_capi import *
from jq_subscriber_capi import *
import time
import pandas as pd


if __name__ == '__main__':

    start_date='2016-02-01'
    end_date='2019-12-31'

    start_time=' 9:30:00'
    end_time=' 15:00:00'

    stock_code='000001.XSHE'

    new_ebq = jq_data()
    ip_str = "10.84.137.198"
    # ip = bytes(ip_str, encoding = "utf-8")
    result = new_ebq.connect(ip_str, 7000)
    l_result = new_ebq.login("13100000002", "gdzq12345")

    day_ls=new_ebq.get_trade_days(start_date,end_date)

    # test_day='2016-05-30'
    # tick=len(new_ebq.get_stock_ticks(stock_code,test_day+start_time,test_day+end_time))
    # print(tick)


    # para=[]
    # for d in day_ls:
    #     day=d.decode()
    #     tick=len(new_ebq.get_stock_ticks(stock_code,day+start_time,day+end_time))
    #     #trade=len(new_ebq.get_trades(stock_code,day+start_time,day+end_time))
    #     #order=len(new_ebq.get_orders(stock_code,day+start_time,day+end_time))
    #     #para.append([day,tick,trade,order])
    #     para.append([day, tick])
    #     print(day)
    #     print(tick)
    # paraDF=pd.DataFrame(para,columns=['date','tick'])
    # paraDF.to_csv('flag_tick.csv')


    # para = []
    # for d in day_ls:
    #     day = d.decode()
    #     #tick = len(new_ebq.get_stock_ticks(stock_code, day + start_time, day + end_time))
    #     trade=len(new_ebq.get_trades(stock_code,day+start_time,day+end_time))
    #     # order=len(new_ebq.get_orders(stock_code,day+start_time,day+end_time))
    #     # para.append([day,tick,trade,order])
    #     para.append([day, trade])
    #     print(day)
    #     print(trade)
    # paraDF = pd.DataFrame(para, columns=['date', 'trade'])
    # paraDF.to_csv('flag_trade.csv')
    #


    para = []
    for d in day_ls:
        day = d.decode()
        #tick = len(new_ebq.get_stock_ticks(stock_code, day + start_time, day + end_time))
        #trade=len(new_ebq.get_trades(stock_code,day+start_time,day+end_time))
        try:
            order=len(new_ebq.get_orders(stock_code,day+start_time,day+end_time))
        except:
            order=0

        # para.append([day,tick,trade,order])
        para.append([day, order])
        print(day)
        print(order)
    paraDF = pd.DataFrame(para, columns=['date', 'order'])
    paraDF.to_csv('flag_order.csv')
