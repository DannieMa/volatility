from jq_data_capi import *
from WindPy import w
import time
import os
import sys
import pandas as pd
import numpy as np
import multiprocessing

from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score

import warnings
warnings.filterwarnings('ignore')

import pyodbc
userid = 'fepusr'
pw = 'fepusr,2012'
cnxn_string = 'DRIVER={SQL SERVER};SERVER=10.88.2.201;DATABASE=Wind;DATABASE=LINK_ZG.FUNDRISKCONTROL;UID=' \
              + userid + ';PWD=' + pw

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

    def __del__(self):
        self.__disconnect__()
        # print ("Disconnected? "+str(self.instance.is_connected()))

    def __disconnect__(self):
        self.instance.dis_connect()

    def TickTrade(self, symbol, starttime, endtime):
        '''
        :param symbol: stock symbol formatted like '000001.XSHE'
        :param starttime: start time formatted like '2017-11-30 09:38:21' or start date formatted like '2017-11-30'
        :param endtime: end time formatted like '2017-11-30 14:38:21' or end date formatted like '2017-11-30'
        :return: a structured pd.dataframe with time, price, volume and side
        '''
        symbol_trade = self.instance.get_trades(symbol, starttime, endtime)
        trade_list = []
        for trade_info in symbol_trade:
            trade_list.append(
                [int(trade_info.time / 1000), trade_info.price / 10000., int(trade_info.volume / 100),
                 trade_info.bsc])
        trade_df = pd.DataFrame(trade_list, columns=['time', 'price', 'volume', 'bsc'])
        trade_df = trade_df[trade_df.price > 0.01][trade_df.volume > 0]
        trade_df['value'] = trade_df.price * trade_df.volume * 100
        return trade_df


def get_lv2_data(symbol, start_time, end_time):
    instance = jq_data()
    instance.connect("10.84.137.198", 7000)
    instance.login("13100000002", "gdzq12345")
    tick_data = instance.get_stock_ticks(symbol, start_time, end_time)
    tick_data = [x for x in tick_data if x.time % 1000000000 > 93000000 and x.current > 0]
    lv2_data = [[x.time, x.bought.a10v, x.bought.a9v, x.bought.a8v, x.bought.a7v, x.bought.a6v, x.bought.a5v,
                 x.bought.a4v, x.bought.a3v, x.bought.a2v, x.bought.a1v, x.bought.b1v, x.bought.b2v, x.bought.b3v,
                 x.bought.b4v, x.bought.b5v, x.bought.b6v, x.bought.b7v, x.bought.b8v, x.bought.b9v, x.bought.b10v]
                for x in tick_data]
    lv2_columns = ['time'] + ['a{}'.format(x) for x in range(10, 0, -1)] + ['b{}'.format(x) for x in range(1, 11)]
    lv2_frame = pd.DataFrame(lv2_data, columns=lv2_columns)
    return lv2_frame

def get_ticktrade(stock_code, start_time, end_time):
    # print(time.asctime(), ': Start ticktrade data with', stock_code[:6])
    ebq_sample = ebq_data()
    ticktrade = ebq_sample.TickTrade(stock_code, start_time, end_time)
    # df.to_csv('ticktrade_' + stock_code[:6] + '.csv')
    return ticktrade

# 复权处理
def stock_reinstate(stock_code, start_date, end_date):
    stock_ticker = stock_code.split('.')[0]
    if stock_ticker.startswith('6'):
        stock_ticker += '.SH'
    else:
        stock_ticker += '.SZ'
    '''
    w.start()
    reinstate_factor = w.wsd(stock_ticker, 'adjfactor', start_date, end_date)
    w.close()
    '''
    cnxn = pyodbc.connect(cnxn_string, unicode_results='True')
    cursor = cnxn.cursor()
    sql = u"""
    declare @EXECSTR VARCHAR(MAX)
    SET @EXECSTR=''+'
    select
        convert(varchar(100), CONVERT(datetime, TRADE_DT), 23) as tdate,
        S_DQ_ADJFACTOR as adjfactor
    from OPENQUERY(LINK_WIND_ORA27,
    ''
    select a.TRADE_DT, a.S_DQ_ADJFACTOR
    '+'
    from WD_USER.ASHAREEODPRICES a
    where a.S_INFO_WINDCODE = ''''%s''''
    and a.TRADE_DT between ''''%s'''' and ''''%s''''
    order by a.TRADE_DT
    '')'
    exec(@execstr)
    """ % (stock_ticker, ''.join(start_date.split('-')), ''.join(end_date.split('-')))
    reinstate_factor = pd.read_sql(sql, cnxn, index_col='tdate').sort_index()
    cursor.close()
    cnxn.close()

    reinstate_index = [int(str(x).replace('-', '')) for x in reinstate_factor.index]
    reinstate_factor.index = reinstate_index
    return reinstate_factor.adjfactor

def normal_timebar(ticktrade, time_period, reinstate_map):
    # ticktrade = ticktrade[ticktrade.time % 1000000 >= 93000]
    trade_df = ticktrade.copy()
    # trade_df = trade_df[trade_df.time // 100 % 10000 > 925][trade_df.time // 100 % 10000 < 1500]
    trade_df = trade_df[trade_df.time // 100 % 10000 <= 1500]
    if type(time_period) is int:
        if time_period == 60:
            trade_df['time_flag'] = trade_df.time // 1000000
            trade_df['minute_flag'] = (trade_df.time % 1000000 - 84500) // 18500
        elif time_period == 120:
            trade_df['time_flag'] = trade_df.time // 1000000
            trade_df['minute_flag'] = trade_df.time % 1000000 // 120000
        else:
            trade_df['time_flag'] = trade_df.time // 10000
            trade_df['minute_flag'] = trade_df.time % 10000 // (time_period * 100)
        grouped = trade_df.groupby(['time_flag', 'minute_flag'])
    else:
        if time_period == 'M':
            trade_df['time_flag'] = trade_df.time // 100000000
        else:
            trade_df['time_flag'] = trade_df.time // 1000000
        grouped = trade_df.groupby('time_flag')
    time_bar = pd.concat([grouped.time.first(), grouped.time.last(), grouped.price.first(), grouped.price.max(),
                          grouped.price.min(), grouped.price.last(), grouped.volume.sum(), grouped.value.sum()], axis=1)
    time_bar.columns = ['open_time', 'close_time', 'open', 'high', 'low', 'close', 'volume', 'value']
    time_bar['avg'] = time_bar.value / time_bar.volume / 100
    time_bar['date'] = time_bar.close_time // 1000000
    time_bar['adjfactor'] = time_bar.date.map(reinstate_map)
    time_bar[['open', 'high', 'low', 'close', 'avg']] = \
        time_bar[['open', 'high', 'low', 'close', 'avg']].apply(lambda x: x * time_bar.adjfactor)
    time_bar = time_bar.drop(['date', 'adjfactor'], axis=1)
    if time_period == 60:
        def trade_hour_formatter(int_time):
            check_point = int_time % 1000000
            hold_point = int_time // 1000000 * 1000000
            if check_point < 103000:
                return hold_point + 103000
            elif check_point < 113000:
                return hold_point + 113000
            elif check_point < 140000:
                return hold_point + 140000
            else:
                return hold_point + 150000

        time_bar.close_time = time_bar.close_time.apply(trade_hour_formatter)
    elif time_period == 120:
        time_bar.close_time = time_bar.close_time.apply(
            lambda x: x // 1000000 * 1000000 + 113000 if x % 1000000 < 113000 else x // 1000000 * 1000000 + 150000)
    else:
        time_bar.close_time = time_bar.close_time.apply(
            lambda x: int(np.ceil(x / time_period / 100) * time_period * 100))
        time_bar.close_time = time_bar.close_time.apply(lambda x: x + 4000 if x % 10000 == 6000 else x)
        time_bar.close_time = time_bar.close_time.apply(lambda x: x - 17000 if x % 1000000 == 130000 else x)
    time_bar.close_time = time_bar.close_time.apply(lambda x: str(x))
    time_bar.close_time = pd.to_datetime(time_bar.close_time)
    time_bar = time_bar.drop('open_time', axis=1).drop_duplicates(subset='close_time', keep='first').set_index(
        'close_time')
    return time_bar

# 获取正常时间戳下的分钟级别数据
def normal_minute_data(stock_code, start_time, end_time, period):
    ticktrade_data = get_ticktrade(stock_code, start_time, end_time)
    if ticktrade_data.shape[0] == 0:
        return None
    reinstate_map = stock_reinstate(stock_code, start_time.split(' ')[0], end_time.split(' ')[0])
    normal_data = normal_timebar(ticktrade_data, period, reinstate_map)
    return normal_data

if __name__ == '__main__':

    start_time = '2010-01-01 09:00:00'
    end_time = '2010-04-30 16:00:00'
    symbol='000001.XSHE'

    tick_df=get_lv2_data(symbol, start_time, end_time)

