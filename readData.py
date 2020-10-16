# -*- coding: utf-8 -*-
"""
Created on Mon Aug  3 21:16:05 2020

@author: dell
"""

from jq_include import *
from jq_data_capi import *
from jq_callback_capi import *
from jq_playback_capi import *
from jq_subscriber_capi import *
import time
import pandas as pd

class ebq_data(object):
    instance = jq_data()
    def __init__(self, ip_addr="10.84.137.198", port=7000, login_info=["13100000002", "gdzq12345"]):
        result = self.instance.connect(ip_addr, port)
        print (time.asctime(), ": connecting.......")
        if result < 0:
            print (time.asctime(), ": connecting failed")
        else:
            print (time.asctime(), ": connecting successed")
        self.instance.is_connected()
        l_result = self.instance.login(login_info[0], login_info[1])
        if l_result != 0:
            print (time.asctime(), ": login failed")
        else:
            print (time.asctime(), ": login success")

    def __del__(self):
        self.__disconnect__()
        print ("Disconnected? "+str(self.instance.is_connected()))

    def __disconnect__(self):
        self.instance.dis_connect()
        
    def GetTradeDate(self, startdate, enddate):
        return self.instance.get_trade_days(startdate, enddate)
    
    def GetKLine(self, symbol, startdate, enddate):
        '''
        日行情
        '''
        symbol_trade = self.instance.get_day_price(symbol, startdate, enddate)
        return len(symbol_trade)
    
    def GetMinute(self, symbol, startdate, enddate,fq,freq):
        '''
        日行情
        '''
        symbol_trade = self.instance.get_minute_price(symbol, startdate, enddate,fq,freq)
        return len(symbol_trade)  
    
    def GetLevel2(self, symbol, startdate, enddate):
        '''
        level 2
        '''
        start_time=startdate+' 09:00:00'
        end_time=enddate+' 15:00:01'
        symbol_trade = self.instance.get_stock_ticks(symbol, start_time, end_time)
        return len(symbol_trade)
    
    def GetTransaction(self, symbol, startdate, enddate):
        '''
        逐笔成交
        '''
        start_time=startdate+' 09:00:00'
        end_time=enddate+' 15:00:01'
        symbol_trade = self.instance.get_trades(symbol, start_time, end_time)
        return len(symbol_trade)
    
    def GetOrders(self, symbol, startdate, enddate):
        '''
        报单
        '''
        start_time=startdate+' 09:00:00'
        end_time=enddate+' 15:00:01'
        symbol_trade = self.instance.get_orders(symbol, start_time, end_time)
        return len(symbol_trade)

if __name__ == '__main__':

    start_date='2016-01-01'
    end_date='2019-12-31'

    stock_code='000001.XSHE'

    new_ebq = jq_data()
    ip_str = "10.84.137.198"
    # ip = bytes(ip_str, encoding = "utf-8")
    result = new_ebq.connect(ip_str, 7000)
    l_result = new_ebq.login("13100000002", "gdzq12345")

    day_ls=new_ebq.get_trade_days(start_date, end_date)
    
    flag=[]
    for d in day_ls:
        day=d.decode()
        a=new_ebq.get_stock_ticks(stock_code, day+' 09:00:00', day+' 15:00:00')
        b=new_ebq.get_trades(stock_code,day+' 09:00:00', day+' 15:00:00')
        c=new_ebq.get_orders(stock_code,day+' 09:00:00', day+' 15:00:00')
        flag.append([day,len(a),len(b),len(c)])
        print(day)

    flag_df=pd.DataFrame(flag,columns=['date','level2','trades','orders'])
    flag_df.to_csv('flag_df3.csv')

# =============================================================================
#     flag=[]
#     for day in day_ls:
#         
#         day=day.decode()
#         day_price=len(new_jd.get_day_price(stock_code,day,day))
#         minute_price=len(new_jd.get_minute_price(stock_code, day+' 09:30:00',day+' 15:00:00',fq=1,freq=10))
#         ticks=len(new_jd.get_stock_ticks(stock_code, day+' 09:30:00',day+' 15:00:00'))   
#         trades=len(new_jd.get_trades(stock_code, day+' 09:30:00',day+' 15:00:00'))  
#         orders=len(new_jd.get_orders(stock_code, day+' 09:30:00',day+' 15:00:00'))
#         flag.append([day,day_price,minute_price,ticks,trades,orders])
#         print(day)
# =============================================================================
