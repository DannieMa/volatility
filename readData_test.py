# -*- coding: utf-8 -*-
"""
Created on Tue Aug  4 22:16:58 2020

@author: dell
"""

from jq_include import *
from jq_data_capi import *
from jq_callback_capi import *
from jq_playback_capi import *
from jq_subscriber_capi import *
import time
import pandas as pd

if __name__ == '__main__':
    
    
    start_date="2016-01-04"
    end_date="2019-12-31"

    stock_code="000001.XSHE"
    
    new_jd=jq_data()
    connect_result = new_jd.connect("10.84.137.108", 7000)
    result = new_jd.login("13100000002", "gdzq12345")
    
    # 日行情
    dayPrice=new_jd.get_day_price(stock_code,start_date,end_date)
    
    # 分钟行情
    minutePrice=new_jd.get_minute_price(stock_code, start_date+" 09:30:00",start_date+" 15:00:00",fq=1,freq=10)
    
    # tick
    stockTicks=new_jd.get_stock_ticks(stock_code, start_date+" 09:30:00",start_date+" 15:00:00")
    
    # trades
    trades=new_jd.get_trades(stock_code, start_date+" 09:30:00",start_date+" 15:00:00")
    
    # null 
    orders=new_jd.get_orders(stock_code, start_date+" 09:30:00",start_date+" 15:00:00")
    
    