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

# w.start()


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

    def TickBarMaker(self, symbol, starttime, endtime, tick_per_bar):
        '''
        :param symbol: stock symbol formatted like '000001.XSHE'
        :param starttime: start time formatted like '2017-11-30 09:38:21' or start date formatted like '2017-11-30'
        :param endtime: end time formatted like '2017-11-30 14:38:21' or end date formatted like '2017-11-30'
        :param tick_per_bar: desired tick number in each bar formatted like 10000
        :return: a structured pd.dataframe with open&close time, OHLC, volume
        '''
        trade_df = self.TickTrade(symbol, starttime, endtime)
        tick_index = pd.Series(range(len(trade_df)), index=trade_df.index)
        tick_index.name = 'tick_index'
        trade_df = pd.concat([trade_df, tick_index], axis=1)
        trade_df['cusum_tick'] = (np.floor(trade_df.tick_index / tick_per_bar))
        trade_df.cusum_tick = trade_df.cusum_tick.astype(int)
        grouped = trade_df.groupby('cusum_tick')
        tick_bar = pd.concat([grouped.time.first(), grouped.time.last(), grouped.price.first(), grouped.price.max(),
                                grouped.price.min(), grouped.price.last(), grouped.volume.sum()], axis=1)
        tick_bar.columns = ['open_time', 'close_time', 'open', 'high', 'low', 'close', 'volume']
        tick_bar.index.name = 'Bar'
        return tick_bar

    def TickImbalanceBar(self, symbol, starttime, endtime, initial_bar_size, ewma_alpha):
        '''
        :param symbol: stock symbol formatted like '000001.XSHE'
        :param starttime: start time formatted like '2017-11-30 09:38:21' or start date formatted like '2017-11-30'
        :param endtime: endtime: end time formatted like '2017-11-30 14:38:21' or end date formatted like '2017-11-30'
        :param initial_bar_size: initial bar size valued as an int like 1000
        :param ewma_alpha: exponential weighted moving average alpha, valued in [0, 1]
        :return: a structured pd.dataframe with open&close time, OHLC, volume, value
        '''
        trade_df = self.TickTrade(symbol, starttime, endtime)
        total_trade = len(trade_df)
        tick_index = pd.Series(range(total_trade), index=trade_df.index)
        tick_index.name = 'tick_index'
        trade_df = pd.concat([trade_df, tick_index], axis=1)
        trade_df.set_index('tick_index', inplace=True)
        trade_df['side'] = trade_df.bsc.map({b'B': 1, b'S': -1})
        trade_df['value'] = trade_df.price * trade_df.volume
        trade_side = trade_df.side.values.tolist()
        bar_list = []
        bar_list.extend(np.zeros(initial_bar_size).astype(int))
        bar_document = pd.DataFrame([[trade_df.side[:initial_bar_size].mean(), initial_bar_size]],
                                    columns=['Prob', 'Size'])
        tick_count = initial_bar_size
        current_bar = 1
        current_threshold = abs(bar_document.Prob[0] * bar_document.Size[0])
        while tick_count < total_trade:
            current_theta = 0
            bar_size_count = 0
            while abs(current_theta) < current_threshold:
                current_theta += trade_side[tick_count]
                bar_list.append(current_bar)
                tick_count += 1
                bar_size_count += 1
                if tick_count >= total_trade:
                    break
            bar_size = bar_size_count
            bar_prob = float(current_theta) / bar_size_count
            bar_document = bar_document.append(
                pd.DataFrame([[bar_prob, bar_size]], index=[current_bar],  columns=['Prob', 'Size']))
            expected_bar_prob, expected_bar_size = bar_document.ewm(alpha=ewma_alpha).mean().ix[current_bar].values
            current_threshold = abs(expected_bar_prob * expected_bar_size)
            current_bar += 1
        trade_df['bar'] = bar_list
        grouped = trade_df.groupby('bar')
        tick_imbalance_bar = pd.concat([grouped.time.first(), grouped.time.last(), grouped.price.first(),
                                        grouped.price.max(), grouped.price.min(), grouped.price.last(),
                                        grouped.volume.sum(), grouped.value.sum()], axis=1)
        tick_imbalance_bar.columns = ['open_time', 'close_time', 'open', 'high', 'low', 'close', 'volume', 'value']
        tick_imbalance_bar.index.name = 'Bar'
        return tick_imbalance_bar

    def TickRunsBar(self, symbol, starttime, endtime, initial_bar_size, ewma_alpha):
        '''
        :param symbol: stock symbol formatted like '000001.XSHE'
        :param starttime: start time formatted like '2017-11-30 09:38:21' or start date formatted like '2017-11-30'
        :param endtime: endtime: end time formatted like '2017-11-30 14:38:21' or end date formatted like '2017-11-30'
        :param initial_bar_size: initial bar size valued as an int like 1000
        :param ewma_alpha: exponential weighted moving average alpha, valued in [0, 1]
        :return: a structured pd.dataframe with open&close time, OHLC, volume, value
        '''
        trade_df = self.TickTrade(symbol, starttime, endtime)
        total_trade = len(trade_df)
        tick_index = pd.Series(range(total_trade), index=trade_df.index)
        tick_index.name = 'tick_index'
        trade_df = pd.concat([trade_df, tick_index], axis=1)
        trade_df.set_index('tick_index', inplace=True)
        trade_df['side'] = trade_df.bsc.map({'B': 1, 'S': -1})
        trade_df['value'] = trade_df.price * trade_df.volume
        trade_side = trade_df.side.values.tolist()
        bar_list = []
        bar_list.extend(np.zeros(initial_bar_size).astype(int))
        initial_prob = (trade_df.side[:initial_bar_size].mean() + 1) / 2
        bar_document = pd.DataFrame([[initial_prob, initial_bar_size]], columns=['Prob', 'Size'])
        tick_count = initial_bar_size
        current_bar = 1
        current_threshold = max(bar_document.Prob[0], 1 - bar_document.Prob[0]) * bar_document.Size[0]
        while tick_count < total_trade:
            current_theta = 0
            bar_size_count = 0
            buy_count = 0
            sell_count = 0
            while current_theta < current_threshold:
                if trade_side[tick_count] > 0:
                    buy_count += trade_side[tick_count]
                else:
                    sell_count -= trade_side[tick_count]
                current_theta = max(buy_count, sell_count)
                bar_list.append(current_bar)
                tick_count += 1
                bar_size_count += 1
                if tick_count >= total_trade:
                    break
            bar_size = bar_size_count
            bar_prob = float(buy_count) / bar_size_count
            bar_document = bar_document.append(
                pd.DataFrame([[bar_prob, bar_size]], index=[current_bar],  columns=['Prob', 'Size']))
            expected_bar_prob, expected_bar_size = bar_document.ewm(alpha=ewma_alpha).mean().ix[current_bar].values
            current_threshold = max(expected_bar_prob, 1 - expected_bar_prob) * expected_bar_size
            current_bar += 1
        trade_df['bar'] = bar_list
        grouped = trade_df.groupby('bar')
        tick_runs_bar = pd.concat([grouped.time.first(), grouped.time.last(), grouped.price.first(),
                                   grouped.price.max(), grouped.price.min(), grouped.price.last(),
                                   grouped.volume.sum(), grouped.value.sum()], axis=1)
        tick_runs_bar.columns = ['open_time', 'close_time', 'open', 'high', 'low', 'close', 'volume', 'value']
        tick_runs_bar.index.name = 'Bar'
        return tick_runs_bar

    def VolumeImbalanceBar(self, symbol, starttime, endtime, initial_bar_size, ewma_alpha):
        '''
        :param symbol: stock symbol formatted like '000001.XSHE'
        :param starttime: start time formatted like '2017-11-30 09:38:21' or start date formatted like '2017-11-30'
        :param endtime: endtime: end time formatted like '2017-11-30 14:38:21' or end date formatted like '2017-11-30'
        :param initial_bar_size: initial bar size valued as an int like 1000
        :param ewma_alpha: exponential weighted moving average alpha, valued in [0, 1]
        :return: a structured pd.dataframe with open&close time, OHLC, volume
        '''
        trade_df = self.TickTrade(symbol, starttime, endtime)
        total_trade = len(trade_df)
        tick_index = pd.Series(range(total_trade), index=trade_df.index)
        tick_index.name = 'tick_index'
        trade_df = pd.concat([trade_df, tick_index], axis=1)
        trade_df.set_index('tick_index', inplace=True)
        trade_df['side'] = trade_df.bsc.map({b'B': 1, b'S': -1})
        trade_df['value'] = trade_df.price * trade_df.volume
        trade_df['signed_volume'] = trade_df.volume * trade_df.side
        trade_side = trade_df.signed_volume.values.tolist()
        bar_list = []
        bar_list.extend(np.zeros(initial_bar_size).astype(int))
        bar_document = pd.DataFrame([[trade_df.signed_volume[:initial_bar_size].mean(), initial_bar_size]],
                                    columns=['Prob', 'Size'])
        tick_count = initial_bar_size
        current_bar = 1
        current_threshold = abs(bar_document.Prob[0] * bar_document.Size[0])
        while tick_count < total_trade:
            current_theta = 0
            bar_size_count = 0
            while abs(current_theta) < current_threshold:
                current_theta += trade_side[tick_count]
                bar_list.append(current_bar)
                tick_count += 1
                bar_size_count += 1
                if tick_count >= total_trade:
                    break
            print('current threshold is: ', current_threshold)
            bar_size = bar_size_count
            bar_prob = float(current_theta) / bar_size_count
            bar_document = bar_document.append(
                pd.DataFrame([[bar_prob, bar_size]], index=[current_bar],  columns=['Prob', 'Size']))
            expected_bar_prob, expected_bar_size = bar_document.ewm(alpha=ewma_alpha).mean().ix[current_bar].values
            current_threshold = abs(expected_bar_prob * expected_bar_size)
            current_bar += 1
        trade_df['bar'] = bar_list
        grouped = trade_df.groupby('bar')
        volume_imbalance_bar = pd.concat([grouped.time.first(), grouped.time.last(), grouped.price.first(),
                                          grouped.price.max(), grouped.price.min(), grouped.price.last(),
                                          grouped.volume.sum(), grouped.value.sum()], axis=1)
        volume_imbalance_bar.columns = ['open_time', 'close_time', 'open', 'high', 'low', 'close', 'volume', 'value']
        volume_imbalance_bar.index.name = 'Bar'
        return volume_imbalance_bar

    def VolumeRunsBar(self, symbol, starttime, endtime, initial_bar_size, ewma_alpha):
        '''
        :param symbol: stock symbol formatted like '000001.XSHE'
        :param starttime: start time formatted like '2017-11-30 09:38:21' or start date formatted like '2017-11-30'
        :param endtime: endtime: end time formatted like '2017-11-30 14:38:21' or end date formatted like '2017-11-30'
        :param initial_bar_size: initial bar size valued as an int like 1000
        :param ewma_alpha: exponential weighted moving average alpha, valued in [0, 1]
        :return: a structured pd.dataframe with open&close time, OHLC, volume, value
        '''
        trade_df = self.TickTrade(symbol, starttime, endtime)
        total_trade = len(trade_df)
        tick_index = pd.Series(range(total_trade), index=trade_df.index)
        tick_index.name = 'tick_index'
        trade_df = pd.concat([trade_df, tick_index], axis=1)
        trade_df.set_index('tick_index', inplace=True)
        trade_df['side'] = trade_df.bsc.map({'B': 1, 'S': -1})
        trade_df['value'] = trade_df.price * trade_df.volume
        trade_df['signed_volume'] = trade_df.volume * trade_df.side
        trade_side = trade_df.signed_volume.values.tolist()
        bar_list = []
        bar_list.extend(np.zeros(initial_bar_size).astype(int))
        bar_document = pd.DataFrame([[(abs(trade_df.signed_volume[:initial_bar_size].mean())+1) / 2, initial_bar_size]],
                                    columns=['Prob', 'Size'])
        tick_count = initial_bar_size
        current_bar = 1
        current_threshold = bar_document.Prob[0] * bar_document.Size[0]
        while tick_count < total_trade:
            current_theta = 0
            bar_size_count = 0
            buy_count = 0
            sell_count = 0
            while current_theta < current_threshold:
                if trade_side[tick_count] > 0:
                    buy_count += trade_side[tick_count]
                else:
                    sell_count -= trade_side[tick_count]
                current_theta = max(buy_count, sell_count)
                bar_list.append(current_bar)
                tick_count += 1
                bar_size_count += 1
                if tick_count >= total_trade:
                    break
            bar_size = bar_size_count
            bar_prob = float(current_theta) / bar_size_count
            bar_document = bar_document.append(
                pd.DataFrame([[bar_prob, bar_size]], index=[current_bar],  columns=['Prob', 'Size']))
            expected_bar_prob, expected_bar_size = bar_document.ewm(alpha=ewma_alpha).mean().ix[current_bar].values
            current_threshold = np.abs(expected_bar_prob * expected_bar_size)
            current_bar += 1
        trade_df['bar'] = bar_list
        grouped = trade_df.groupby('bar')
        tick_runs_bar = pd.concat([grouped.time.first(), grouped.time.last(), grouped.price.first(),
                                        grouped.price.max(), grouped.price.min(), grouped.price.last(),
                                        grouped.volume.sum(), grouped.value.sum()], axis=1)
        tick_runs_bar.columns = ['open_time', 'close_time', 'open', 'high', 'low', 'close', 'volume', 'value']
        tick_runs_bar.index.name = 'Bar'
        return tick_runs_bar

    def VolumeBarMaker(self, symbol, starttime, endtime, volume_per_bar):
        '''
        :param symbol: stock symbol formatted like '000001.XSHE'
        :param starttime: start time formatted like '2017-11-30 09:38:21' or start date formatted like '2017-11-30'
        :param endtime: end time formatted like '2017-11-30 14:38:21' or end date formatted like '2017-11-30'
        :param volume_per_bar: desired volume num in each bar formatted like 10000
        :return: a structured pd.dataframe with open&close time, OHLC, volume
        '''
        trade_df = self.TickTrade(symbol, starttime, endtime)
        trade_df['cusum_volume'] = (np.floor(trade_df.volume.cumsum() / volume_per_bar))
        trade_df.cusum_volume = trade_df.cusum_volume.astype(int)
        grouped = trade_df.groupby('cusum_volume')
        volume_bar = pd.concat([grouped.time.first(), grouped.time.last(), grouped.price.first(), grouped.price.max(),
                                grouped.price.min(), grouped.price.last(), grouped.volume.sum()], axis=1)
        volume_bar.columns = ['open_time', 'close_time', 'open', 'high', 'low', 'close', 'volume']
        volume_bar.index.name = 'Bar'
        return volume_bar

    def ValueBarMaker(self, symbol, starttime, endtime, value_per_bar):
        '''
        :param symbol: stock symbol formatted like '000001.XSHE'
        :param starttime: start time formatted like '2017-11-30 09:38:21' or start date formatted like '2017-11-30'
        :param endtime: end time formatted like '2017-11-30 14:38:21' or end date formatted like '2017-11-30'
        :param value_per_bar: desired exchanged money in each bar formatted like 10000
        :return: a structured pd.dataframe with open&close time, OHLC, volume, value
        '''
        trade_df = self.TickTrade(symbol, starttime, endtime)
        trade_df['value'] = trade_df.price * trade_df.volume
        trade_df['cusum_value'] = (np.floor(trade_df.value.cumsum() / value_per_bar))
        trade_df.cusum_value = trade_df.cusum_value.astype(int)
        grouped = trade_df.groupby('cusum_value')
        value_bar = pd.concat([grouped.time.first(), grouped.time.last(), grouped.price.first(), grouped.price.max(),
                               grouped.price.min(), grouped.price.last(), grouped.volume.sum(), grouped.value.sum()],
                              axis=1)
        value_bar.columns = ['open_time', 'close_time', 'open', 'high', 'low', 'close', 'volume', 'value']
        value_bar.index.name = 'Bar'
        return value_bar

    def ValueImbalanceBar(self, symbol, starttime, endtime, initial_bar_size, ewma_alpha):
        '''
        :param symbol: stock symbol formatted like '000001.XSHE'
        :param starttime: start time formatted like '2017-11-30 09:38:21' or start date formatted like '2017-11-30'
        :param endtime: endtime: end time formatted like '2017-11-30 14:38:21' or end date formatted like '2017-11-30'
        :param initial_bar_size: initial bar size valued as an int like 1000
        :param ewma_alpha: exponential weighted moving average alpha, valued in [0, 1]
        :return: a structured pd.dataframe with open&close time, OHLC, volume
        '''
        trade_df = self.TickTrade(symbol, starttime, endtime)
        total_trade = len(trade_df)
        tick_index = pd.Series(range(total_trade), index=trade_df.index)
        tick_index.name = 'tick_index'
        trade_df = pd.concat([trade_df, tick_index], axis=1)
        trade_df.set_index('tick_index', inplace=True)
        trade_df['side'] = trade_df.bsc.map({'B': 1, 'S': -1})
        trade_df['value'] = trade_df.price * trade_df.volume
        trade_df['signed_value'] = trade_df.value * trade_df.side
        trade_side = trade_df.signed_value.values.tolist()
        bar_list = []
        bar_list.extend(np.zeros(initial_bar_size).astype(int))
        bar_document = pd.DataFrame([[trade_df.signed_value[:initial_bar_size].mean(), initial_bar_size]],
                                    columns=['Prob', 'Size'])
        tick_count = initial_bar_size
        current_bar = 1
        current_threshold = abs(bar_document.Prob[0] * bar_document.Size[0])
        while tick_count < total_trade:
            current_theta = 0
            bar_size_count = 0
            while abs(current_theta) < current_threshold:
                current_theta += trade_side[tick_count]
                bar_list.append(current_bar)
                tick_count += 1
                bar_size_count += 1
                if tick_count >= total_trade:
                    break
            bar_size = bar_size_count
            bar_prob = float(current_theta) / bar_size_count
            bar_document = bar_document.append(
                pd.DataFrame([[bar_prob, bar_size]], index=[current_bar],  columns=['Prob', 'Size']))
            expected_bar_prob, expected_bar_size = bar_document.ewm(alpha=ewma_alpha).mean().ix[current_bar].values
            current_threshold = abs(expected_bar_prob * expected_bar_size)
            current_bar += 1
        trade_df['bar'] = bar_list
        grouped = trade_df.groupby('bar')
        value_imbalance_bar = pd.concat([grouped.time.first(), grouped.time.last(), grouped.price.first(),
                                         grouped.price.max(), grouped.price.min(), grouped.price.last(),
                                         grouped.volume.sum(), grouped.value.sum()], axis=1)
        value_imbalance_bar.columns = ['open_time', 'close_time', 'open', 'high', 'low', 'close', 'volume', 'value']
        value_imbalance_bar.index.name = 'Bar'
        return value_imbalance_bar

    def MinuteData(self, symbol, starttime, endtime, rehab=1, freq=1):
        '''
        :param symbol: stock symbol formatted like '000001.XSHE'
        :param starttime: start time formatted like '2017-11-30 09:38:21'
        :param endtime: end time formatted like '2017-11-30 14:38:21'
        :param rehab: rehabilitation method, could be 0, 1, or 2
        :param freq: minute frequency, could be 1, 5, 15, 30, or 60
        :return: a structured pd.dataframe with open&close time, OHLC, volume, value
        '''
        symbol_data = self.instance.get_minute_price(symbol, starttime, endtime, rehab, freq)
        data_list = []
        for minute_info in symbol_data:
            data_list.append(
                [int(minute_info.time / 1000), minute_info.open / 10000., minute_info.high / 10000.,
                 minute_info.low / 10000., minute_info.close / 10000., minute_info.volume, minute_info.money,
                 minute_info.avg / 10000])
        minute_df = pd.DataFrame(data_list, columns=['time', 'open', 'high', 'low', 'close', 'volume', 'value', 'avg'])
        return minute_df

    def TickData(self, symbol, starttime, endtime, instrument='index'):
        if instrument == 'stock':
            tick_data = self.instance.get_stock_ticks(symbol, starttime, endtime)
        elif instrument == 'fund':
            tick_data = self.instance.get_fund_ticks(symbol, starttime, endtime)
        else:
            tick_data = self.instance.get_index_ticks(symbol, starttime, endtime)
        tick_list = []
        for tick_info in tick_data:
            tick_list.append([int(tick_info.time / 1000), tick_info.current / 10000., tick_info.high / 10000.,
                              tick_info.low / 10000., int(tick_info.volume / 100),
                              tick_info.money])
        tick_df = pd.DataFrame(tick_list, columns=['time', 'current_price', 'high', 'low', 'volume', 'value'])
        return tick_df

    def GetTradeDate(self, startdate, enddate):
        return self.instance.get_trade_days(startdate, enddate)


Saved_List = [
['600000.XSHG', 5, 320, 8200, 111000],
['600016.XSHG', 5, 470, 20100, 169000],
['600019.XSHG', 5, 590, 19900, 152000],
['600028.XSHG', 5, 460, 24600, 146000],
['600029.XSHG', 5, 520, 14800, 128000],
['600030.XSHG', 5, 730, 17800, 312000],
['600036.XSHG', 5, 550, 10200, 249000],
['600048.XSHG', 5, 500, 15900, 171000],
['600050.XSHG', 5, 2010, 61500, 463000],
['600104.XSHG', 5, 500, 6000, 175000],
['600196.XSHG', 5, 300, 3000, 100000],
['600276.XSHG', 5, 320, 1700, 100000],
['600309.XSHG', 5, 630, 7500, 245000],
['600340.XSHG', 5, 660, 6700, 230000],
['600519.XSHG', 5, 330, 700, 382000],
['600547.XSHG', 5, 470, 3900, 130000],
['600585.XSHG', 5, 470, 6700, 165000],
['600606.XSHG', 5, 380, 11900, 93000],
['600690.XSHG', 5, 620, 9600, 142000],
['600703.XSHG', 5, 440, 6200, 129000],
['600887.XSHG', 5, 860, 11900, 277000],
['601006.XSHG', 5, 340, 9100, 76000],
['601088.XSHG', 5, 430, 5600, 116000],
['601166.XSHG', 5, 490, 15300, 260000],
['601169.XSHG', 5, 250, 7900, 65000],
['601186.XSHG', 5, 540, 10500, 136000],
['601288.XSHG', 5, 430, 46100, 163000],
['601318.XSHG', 5, 1090, 14700, 781000],
['601328.XSHG', 5, 320, 18000, 112000],
['601390.XSHG', 5, 540, 11500, 103000],
['601398.XSHG', 5, 470, 32600, 180000],
['601601.XSHG', 5, 350, 4100, 143000],
['601628.XSHG', 5, 350, 3700, 108000],
['601668.XSHG', 5, 1480, 48500, 463000],
['601688.XSHG', 5, 500, 8900, 175000],
['601766.XSHG', 5, 790, 18400, 196000],
['601818.XSHG', 5, 280, 21300, 87000],
['601857.XSHG', 5, 260, 8200, 66000],
['601888.XSHG', 5, 200, 1500, 62000],
['601939.XSHG', 5, 330, 17300, 112000],
['601988.XSHG', 5, 400, 35400, 135000],
['601989.XSHG', 5, 1040, 28800, 208000]
]
# List = [item for item in Saved_List]
# Time = ['2017-01-01 09:00:00', '2017-12-31 15:30:00']


def func(stock_code, start_time, end_time, volume):
    print(time.asctime(), ': Start with', stock_code[:6])
    ebq_sample = ebq_data()
    df = ebq_sample.VolumeBarMaker(stock_code, start_time, end_time, volume)
    df.to_csv('vb_'+stock_code[:6]+'.csv')
    return 0


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


def get_all_stock():
    instance = jq_data()
    instance.connect("10.84.137.108", 7000)
    instance.login("13100000002", "gdzq12345")
    all_stock = [x.decode() for x in instance.get_all_securities(Stock)]
    return all_stock


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


def adjusted_ticktrade(stock_code, start_time, end_time, continuous_auction_only=False):
    raw_ticktrade = get_ticktrade(stock_code, start_time, end_time)
    if continuous_auction_only:
        raw_ticktrade = raw_ticktrade[raw_ticktrade.time % 1000000 >= 93000]
    reinstate_map = stock_reinstate(stock_code, start_time.split(' ')[0], end_time.split(' ')[0])
    raw_ticktrade['date'] = raw_ticktrade.time // 1000000
    raw_ticktrade['adjfactor'] = raw_ticktrade.date.map(reinstate_map)
    raw_ticktrade.price = raw_ticktrade.price * raw_ticktrade.adjfactor
    return raw_ticktrade.drop(labels=['date', 'adjfactor'], axis=1)


def get_minutedata(stock_code, start_time, end_time, period, adjust_time=True):
    # print(time.asctime(), ': Start minute data with', stock_code[:6])
    ebq_sample = ebq_data()
    minutedata = ebq_sample.MinuteData(stock_code, start_time, end_time, 0, period)
    if minutedata.shape[0] == 0:
        return minutedata
    reinstate_map = stock_reinstate(stock_code, start_time.split(' ')[0], end_time.split(' ')[0])
    reinstate_map = reinstate_map.reset_index().drop_duplicates().set_index('index').adjfactor
    minutedata['date'] = minutedata.time // 1000000
    minutedata['adjfactor'] = minutedata.date.map(reinstate_map)
    minutedata[['open', 'high', 'low', 'close', 'avg']] = \
        minutedata[['open', 'high', 'low', 'close', 'avg']].apply(lambda x: x * minutedata.adjfactor)

    def time_adjust(minutedata, digit_time=20171224000000):
        minutedata.time[minutedata.time > digit_time] -= 100
        minutedata.time[minutedata.time % 10000 == 9900] -= 4000
        minutedata.time[minutedata.time % 1000000 == 125900] -= 13000

    if adjust_time:
        time_adjust(minutedata)
    return minutedata.drop(labels=['date', 'adjfactor'], axis=1)


def save_minutedata(save_path, stock_code, start_time, end_time, period, adjust_time=True):
    # print(time.asctime(), ': Start with', stock_code[:6])
    ebq_sample = ebq_data()
    minutedata = ebq_sample.MinuteData(stock_code, start_time, end_time, 1, period)
    if minutedata.shape[0] == 0:
        print('No data: ', stock_code)
        return None
    reinstate_map = stock_reinstate(stock_code, start_time.split(' ')[0], end_time.split(' ')[0])
    reinstate_map = reinstate_map.reset_index().drop_duplicates().set_index('index').adjfactor
    minutedata['date'] = minutedata.time // 1000000
    minutedata['adjfactor'] = minutedata.date.map(reinstate_map)
    minutedata[['open', 'high', 'low', 'close', 'avg']] = \
        minutedata[['open', 'high', 'low', 'close', 'avg']].apply(lambda x: x * minutedata.adjfactor)

    def time_adjust(minutedata, digit_time=20171224000000):
        minutedata.time[minutedata.time > digit_time] -= 100
        minutedata.time[minutedata.time % 10000 == 9900] -= 4000
        minutedata.time[minutedata.time % 1000000 == 125900] -= 13000

    if adjust_time:
        time_adjust(minutedata)
    if isinstance(stock_code, str):
        csv_name = save_path + stock_code[:6] + '.csv'
    else:
        csv_name = save_path+bytes.decode(stock_code[:6])+'.csv'
    minutedata.drop(labels=['date', 'adjfactor'], axis=1).to_csv(csv_name)


def standard_tickbar(stock_code, tick_per_bar):
    print(time.asctime(), ': Start with', stock_code[:6])
    ticktrade = pd.read_csv('ticktrade_'+stock_code[:6]+'.csv', index_col=0)
    trade_df = ticktrade.copy()
    tick_index = pd.Series(range(len(trade_df)), index=trade_df.index)
    tick_index.name = 'tick_index'
    trade_df = pd.concat([trade_df, tick_index], axis=1)
    trade_df['cusum_tick'] = (np.floor(trade_df.tick_index / tick_per_bar))
    trade_df.cusum_tick = trade_df.cusum_tick.astype(int)
    grouped = trade_df.groupby('cusum_tick')
    tick_bar = pd.concat([grouped.time.first(), grouped.time.last(), grouped.price.first(), grouped.price.max(),
                        grouped.price.min(), grouped.price.last(), grouped.volume.sum()], axis=1)
    tick_bar.columns = ['open_time', 'close_time', 'open', 'high', 'low', 'close', 'volume']
    tick_bar.index.name = 'Bar'
    tick_bar.to_csv('tick_' + stock_code[:6] + '.csv')
    return 0


def standard_volumebar(stock_code, volume_per_bar):
    print(time.asctime(), ': Start with', stock_code[:6])
    ticktrade = pd.read_csv('ticktrade_'+stock_code[:6]+'.csv', index_col=0)
    trade_df = ticktrade.copy()
    trade_df['cusum_volume'] = (np.floor(trade_df.volume.cumsum() / volume_per_bar))
    trade_df.cusum_volume = trade_df.cusum_volume.astype(int)
    grouped = trade_df.groupby('cusum_volume')
    volume_bar = pd.concat([grouped.time.first(), grouped.time.last(), grouped.price.first(), grouped.price.max(),
                            grouped.price.min(), grouped.price.last(), grouped.volume.sum()], axis=1)
    volume_bar.columns = ['open_time', 'close_time', 'open', 'high', 'low', 'close', 'volume']
    volume_bar.index.name = 'Bar'
    volume_bar.to_csv('volume_' + stock_code[:6] + '.csv')
    return 0


def standard_valuebar(stock_code, value_per_bar):
    print(time.asctime(), ': Start with', stock_code[:6])
    ticktrade = pd.read_csv('ticktrade_'+stock_code[:6]+'.csv', index_col=0)
    trade_df = ticktrade.copy()
    trade_df['value'] = trade_df.price * trade_df.volume
    trade_df['cusum_value'] = (np.floor(trade_df.value.cumsum() / value_per_bar))
    trade_df.cusum_value = trade_df.cusum_value.astype(int)
    grouped = trade_df.groupby('cusum_value')
    value_bar = pd.concat([grouped.time.first(), grouped.time.last(), grouped.price.first(), grouped.price.max(),
                           grouped.price.min(), grouped.price.last(), grouped.volume.sum(), grouped.value.sum()],
                          axis=1)
    value_bar.columns = ['open_time', 'close_time', 'open', 'high', 'low', 'close', 'volume', 'value']
    value_bar.index.name = 'Bar'
    value_bar.to_csv('value_' + stock_code[:6] + '.csv')
    return 0


def get_standard_bar(stock_profile, starttime, endtime, periods):
    stock_code = stock_profile[0].split('.')[0]
    sample_ticktrade = adjusted_ticktrade(stock_profile[0], starttime, endtime)
    if type(periods) is list:
        for period in periods:
            vi_time = get_minutedata(stock_profile[0], starttime, endtime, period)
            bar_num_benchmark = vi_time.shape[0]
            tick_total = sample_ticktrade.shape[0]
            volume_total = sample_ticktrade.volume.sum()
            value_total = sample_ticktrade.value.sum()
            tick_per_bar = int(tick_total // bar_num_benchmark)
            volume_per_bar = int(volume_total // bar_num_benchmark)
            value_per_bar = int(value_total // bar_num_benchmark)
            vi_tick = VI_tickbar(sample_ticktrade, tick_per_bar)
            vi_volume = VI_volumebar(sample_ticktrade, volume_per_bar)
            vi_value = VI_valuebar(sample_ticktrade, value_per_bar)
            vi_time.to_csv('time_M' + str(period) + '_' + stock_code + '.csv')
            vi_tick.to_csv('tick_M' + str(period) + '_' + stock_code + '.csv')
            vi_volume.to_csv('volume_M' + str(period) + '_' + stock_code + '.csv')
            vi_value.to_csv('value_M' + str(period) + '_' + stock_code + '.csv')
    else:
        vi_time = get_minutedata(stock_profile[0], starttime, endtime, periods)
        bar_num_benchmark = vi_time.shape[0]
        tick_total = sample_ticktrade.shape[0]
        volume_total = sample_ticktrade.volume.sum()
        value_total = sample_ticktrade.value.sum()
        tick_per_bar = int(tick_total // bar_num_benchmark)
        volume_per_bar = int(volume_total // bar_num_benchmark)
        value_per_bar = int(value_total // bar_num_benchmark)
        vi_tick = VI_tickbar(sample_ticktrade, tick_per_bar)
        vi_volume = VI_volumebar(sample_ticktrade, volume_per_bar)
        vi_value = VI_valuebar(sample_ticktrade, value_per_bar)
        vi_time.to_csv('time_M' + str(periods) + '_' + stock_code + '.csv')
        vi_tick.to_csv('tick_M' + str(periods) + '_' + stock_code + '.csv')
        vi_volume.to_csv('volume_M' + str(periods) + '_' + stock_code + '.csv')
        vi_value.to_csv('value_M' + str(periods) + '_' + stock_code + '.csv')
    print(stock_profile[0]+' Completed!')


# 构造有向成交量 时间等分K线数据
def VI_timebar(ticktrade, time_period='D'):
    trade_df = ticktrade.copy()
    if type(time_period) is int:
        trade_df['time_flag'] = trade_df.time // 10000
        trade_df['minute_flag'] = trade_df.time % 10000 // (time_period * 100)
        signed_volume = trade_df.groupby(by=['time_flag', 'minute_flag', 'bsc']).volume.sum().unstack()
        signed_volume = signed_volume.fillna(0)
        grouped = trade_df.groupby(['time_flag', 'minute_flag'])
    else:
        if time_period == 'M':
            trade_df['time_flag'] = trade_df.time // 100000000
        elif time_period == 'H':
            trade_df['time_flag'] = trade_df.time // 10000
        else:
            trade_df['time_flag'] = trade_df.time // 1000000
        signed_volume = trade_df.groupby(by=['time_flag', 'bsc']).volume.sum().unstack()
        signed_volume = signed_volume.fillna(0)
        grouped = trade_df.groupby('time_flag')
    time_bar = pd.concat([grouped.time.first(), grouped.time.last(), grouped.price.first(), grouped.price.max(),
                          grouped.price.min(), grouped.price.last(), grouped.volume.sum()], axis=1)
    time_bar.columns = ['open_time', 'close_time', 'open', 'high', 'low', 'close', 'volume']
    time_bar = pd.concat([time_bar, signed_volume], axis=1)
    time_bar = time_bar.reset_index(drop=True)
    return time_bar


def vi_time_full_period(ticktrade, time_period):
    trade_df = ticktrade.copy()
    trade_df = trade_df[trade_df.time // 100 % 10000 > 925][trade_df.time // 100 % 10000 < 1500]
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
        signed_volume = trade_df.groupby(by=['time_flag', 'minute_flag', 'bsc']).volume.sum().unstack()
        signed_volume = signed_volume.fillna(0)
        grouped = trade_df.groupby(['time_flag', 'minute_flag'])
    else:
        if time_period == 'M':
            trade_df['time_flag'] = trade_df.time // 100000000
        else:
            trade_df['time_flag'] = trade_df.time // 1000000
        signed_volume = trade_df.groupby(by=['time_flag', 'bsc']).volume.sum().unstack()
        signed_volume = signed_volume.fillna(0)
        grouped = trade_df.groupby('time_flag')
    time_bar = pd.concat([grouped.time.first(), grouped.time.last(), grouped.price.first(), grouped.price.max(),
                          grouped.price.min(), grouped.price.last(), grouped.volume.sum(), grouped.value.sum()], axis=1)
    time_bar.columns = ['open_time', 'close_time', 'open', 'high', 'low', 'close', 'volume', 'value']
    time_bar = pd.concat([time_bar, signed_volume], axis=1)
    time_bar = time_bar.reset_index(drop=True)
    return time_bar


def volume_specific_timebar_full_period(ticktrade, time_period, reinstate_map):
    def volume_specific(trade_df, group_by):
        signed_volume = trade_df.groupby(by=group_by).volume
        vol_spec_template = pd.DataFrame(index=signed_volume.sum().unstack().index, columns=[b'B', b'S'])
        vol_spec_col = ['buy', 'sell']
        vol_spec_sum = vol_spec_template.copy()
        vol_spec_sum.update(signed_volume.sum().unstack())
        vol_spec_sum = vol_spec_sum.fillna(0)
        vol_spec_count = vol_spec_template.copy()
        vol_spec_count.update(signed_volume.count().unstack())
        vol_spec_count = vol_spec_count.fillna(0)
        vol_spec_mean = vol_spec_template.copy()
        vol_spec_mean.update(signed_volume.mean().unstack())
        vol_spec_mean = vol_spec_mean.fillna(0)
        vol_spec_std = vol_spec_template.copy()
        vol_spec_std.update(signed_volume.std().unstack())
        vol_spec_std[vol_spec_std == 0] = np.nan
        vol_spec_sum.columns = vol_spec_col
        vol_spec_count.columns = vol_spec_col
        vol_spec_mean.columns = vol_spec_col
        vol_spec_std.columns = vol_spec_col
        vol_spec_sum_ratio = vol_spec_sum.buy / (vol_spec_sum.buy + vol_spec_sum.sell)
        vol_spec_count_ratio = vol_spec_count.buy / (vol_spec_count.buy + vol_spec_count.sell)
        vol_spec_std_ratio = vol_spec_std.buy / vol_spec_std.sell
        # vol_spec_std_ratio[vol_spec_std_ratio == np.inf] = np.nan
        # vol_spec_std_ratio[vol_spec_std_ratio == -np.inf] = np.nan
        buy_ir = vol_spec_mean.buy / vol_spec_std.buy
        # buy_ir[buy_ir == np.inf] = np.nan
        # buy_ir[buy_ir == -np.inf] = np.nan
        sell_ir = vol_spec_mean.sell / vol_spec_std.sell
        # sell_ir[sell_ir == np.inf] = np.nan
        # sell_ir[sell_ir == -np.inf] = np.nan
        vol_spec_ir_ratio = buy_ir - sell_ir
        vol_spec_mat = pd.concat([vol_spec_sum_ratio, vol_spec_count_ratio, vol_spec_std_ratio, vol_spec_ir_ratio],
                                 axis=1)
        vol_spec_mat.columns = ['bs_sum_rate', 'bs_count_rate', 'bs_std_ratio', 'bs_ir_diff']
        return vol_spec_mat
    ticktrade = ticktrade[ticktrade.time % 1000000 >= 93000]
    trade_df = ticktrade.copy()
    trade_df = trade_df[trade_df.time // 100 % 10000 > 925][trade_df.time // 100 % 10000 < 1500]
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
        signed_volume = volume_specific(trade_df, ['time_flag', 'minute_flag', 'bsc'])
        grouped = trade_df.groupby(['time_flag', 'minute_flag'])
    else:
        if time_period == 'M':
            trade_df['time_flag'] = trade_df.time // 100000000
        else:
            trade_df['time_flag'] = trade_df.time // 1000000
        signed_volume = volume_specific(trade_df, ['time_flag', 'bsc'])
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
    time_bar = pd.concat([time_bar, signed_volume], axis=1)
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
        time_bar.close_time = time_bar.close_time.apply(lambda x: int(np.ceil(x / time_period / 100) * time_period * 100))
        time_bar.close_time = time_bar.close_time.apply(lambda x: x+4000 if x % 10000 == 6000 else x)
        time_bar.close_time = time_bar.close_time.apply(lambda x: x-17000 if x % 1000000 == 130000 else x)
    time_bar.close_time = time_bar.close_time.apply(lambda x: str(x))
    time_bar.close_time = pd.to_datetime(time_bar.close_time)
    time_bar = time_bar.drop('open_time', axis=1).drop_duplicates(subset='close_time', keep='first').set_index('close_time')
    return time_bar


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


def volume_specific_minute_data(stock_code, start_time, end_time, period):
    ticktrade_data = get_ticktrade(stock_code, start_time, end_time)
    if ticktrade_data.shape[0] == 0:
        return None
    reinstate_map = stock_reinstate(stock_code, start_time.split(' ')[0], end_time.split(' ')[0])
    vol_spec_data = volume_specific_timebar_full_period(ticktrade_data, period, reinstate_map)
    return vol_spec_data


# 获取正常时间戳下的分钟级别数据
def normal_minute_data(stock_code, start_time, end_time, period):
    ticktrade_data = get_ticktrade(stock_code, start_time, end_time)
    if ticktrade_data.shape[0] == 0:
        return None
    reinstate_map = stock_reinstate(stock_code, start_time.split(' ')[0], end_time.split(' ')[0])
    normal_data = normal_timebar(ticktrade_data, period, reinstate_map)
    return normal_data


def save_vsminute_data(stock_code, start_time, end_time, period):
    try:
        combo_data = volume_specific_minute_data(stock_code, start_time, end_time, period)
    except Exception as e:
        log_file = open('m5_log.txt', 'a')
        print('{} in period {} and {} got error!'.format(stock_code.split('.')[0], start_time[:4], end_time[:4]), file=log_file)
        print(e, file=log_file)
        log_file.close()
    if combo_data is None:
        return
    else:
        combo_data.to_csv('{}_{}.csv'.format(stock_code.split('.')[0], start_time[:4]))


def save_normal_minute_data(stock_code, start_time, end_time, period):
    if len(stock_code.split('.')[-1]) == 2:
        sdk_stock_code = stock_code.replace('SH', 'XSHG').replace('SZ', 'XSHE')
    else:
        sdk_stock_code = stock_code
    try:
        combo_data = normal_minute_data(sdk_stock_code, start_time, end_time, period)
    except Exception as e:
        log_file = open('m5_log.txt', 'a')
        print('{} in period {} and {} got error!'.format(stock_code, start_time[:4], end_time[:4]), file=log_file)
        print(e, file=log_file)
        log_file.close()
    if combo_data is None:
        return
    else:
        save_data = combo_data.reset_index()[['close_time', 'open', 'high', 'low', 'close', 'volume', 'value']]
        save_data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Value']
        save_data['Code'] = stock_code
        save_data[['Code', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Value']].to_csv('{}.csv'.format(stock_code))


def VI_tickbar(ticktrade, tick_per_bar):
    trade_df = ticktrade.copy()
    tick_index = pd.Series(range(len(trade_df)), index=trade_df.index)
    tick_index.name = 'tick_index'
    trade_df = pd.concat([trade_df, tick_index], axis=1)
    trade_df['cusum_tick'] = (np.floor(trade_df.tick_index / tick_per_bar)).astype(int)
    signed_volume = trade_df.groupby(by=['cusum_tick', 'bsc']).volume.sum().unstack()
    signed_volume = signed_volume.fillna(0)
    grouped = trade_df.groupby('cusum_tick')
    tick_bar = pd.concat([grouped.time.first(), grouped.time.last(), grouped.price.first(), grouped.price.max(),
                        grouped.price.min(), grouped.price.last(), grouped.volume.sum(), grouped.value.sum()], axis=1)
    tick_bar.columns = ['open_time', 'close_time', 'open', 'high', 'low', 'close', 'volume', 'value']
    tick_bar = pd.concat([tick_bar, signed_volume], axis=1)
    del trade_df
    return tick_bar


def VI_volumebar(ticktrade, volume_per_bar):
    trade_df = ticktrade.copy()
    trade_df['cusum_volume'] = (np.floor(trade_df.volume.cumsum() / volume_per_bar)).astype(int)
    signed_volume = trade_df.groupby(by=['cusum_volume', 'bsc']).volume.sum().unstack()
    signed_volume = signed_volume.fillna(0)
    grouped = trade_df.groupby('cusum_volume')
    volume_bar = pd.concat([grouped.time.first(), grouped.time.last(), grouped.price.first(), grouped.price.max(),
                        grouped.price.min(), grouped.price.last(), grouped.volume.sum(), grouped.value.sum()], axis=1)
    volume_bar.columns = ['open_time', 'close_time', 'open', 'high', 'low', 'close', 'volume', 'value']
    volume_bar = pd.concat([volume_bar, signed_volume], axis=1)
    del trade_df
    return volume_bar


def VI_valuebar(ticktrade, value_per_bar):
    trade_df = ticktrade.copy()
    trade_df['cusum_value'] = (np.floor(trade_df.value.cumsum() / value_per_bar)).astype(int)
    signed_volume = trade_df.groupby(by=['cusum_value', 'bsc']).volume.sum().unstack()
    signed_volume = signed_volume.fillna(0)
    grouped = trade_df.groupby('cusum_value')
    value_bar = pd.concat([grouped.time.first(), grouped.time.last(), grouped.price.first(), grouped.price.max(),
                           grouped.price.min(), grouped.price.last(), grouped.volume.sum(), grouped.value.sum()],
                          axis=1)
    value_bar.columns = ['open_time', 'close_time', 'open', 'high', 'low', 'close', 'volume', 'value']
    value_bar = pd.concat([value_bar, signed_volume], axis=1)
    del trade_df
    return value_bar


def get_vi_bar_given_profile(stock_profile, multiple):
    stock_code = stock_profile[0].split('.')[0]
    sample_ticktrade = adjusted_ticktrade(stock_profile[0], '2013-01-01 09:00:00', '2017-12-31 15:30:00')
    vi_time = VI_timebar(sample_ticktrade, stock_profile[1]*multiple)
    vi_tick = VI_tickbar(sample_ticktrade, stock_profile[2]*multiple)
    vi_volume = VI_volumebar(sample_ticktrade, stock_profile[3]*multiple)
    vi_value = VI_valuebar(sample_ticktrade, stock_profile[4]*multiple)
    vi_time.to_csv('vi_time_'+stock_code+'.csv')
    vi_tick.to_csv('vi_tick_'+stock_code+'.csv')
    vi_volume.to_csv('vi_volume_'+stock_code+'.csv')
    vi_value.to_csv('vi_value_'+stock_code+'.csv')
    print(stock_profile[0]+' Completed!')


def get_vi_bar(stock_profile, starttime, endtime, multiples):
    stock_code = stock_profile[0].split('.')[0]
    sample_ticktrade = adjusted_ticktrade(stock_profile[0], starttime, endtime)

    def straight_service(ticktrade, period):
        vi_time = vi_time_full_period(ticktrade, period)
        bar_num_benchmark = vi_time.shape[0]
        tick_total = ticktrade.shape[0]
        volume_total = ticktrade.volume.sum()
        value_total = ticktrade.value.sum()
        tick_per_bar = int(tick_total // bar_num_benchmark)
        volume_per_bar = int(volume_total // bar_num_benchmark)
        value_per_bar = int(value_total // bar_num_benchmark)
        vi_tick = VI_tickbar(ticktrade, tick_per_bar)
        vi_volume = VI_volumebar(ticktrade, volume_per_bar)
        vi_value = VI_valuebar(ticktrade, value_per_bar)
        vi_time.to_csv('vi_time_' + str(period) + '_' + stock_code + '.csv')
        vi_tick.to_csv('vi_tick_' + str(period) + '_' + stock_code + '.csv')
        vi_volume.to_csv('vi_volume_' + str(period) + '_' + stock_code + '.csv')
        vi_value.to_csv('vi_value_' + str(period) + '_' + stock_code + '.csv')

    if type(multiples) is list:
        for multiple in multiples:
            straight_service(sample_ticktrade, multiple)
    else:
        straight_service(sample_ticktrade, multiples)
    print(stock_profile[0]+' Completed!')


def get_pool_stock_bar(stock_code, starttime, endtime, split_year, periods):

    def nice_number(raw_number):
        return round(raw_number / (10 ** (len(str(raw_number)) - 2))) * (10 ** (len(str(raw_number)) - 2))

    def straight_service(ticktrade, split_point, period):
        vi_time = vi_time_full_period(ticktrade, period)
        first_year = vi_time.close_time[0] // 10000000000
        if first_year > (split_point - 2):
            bar_num_benchmark = vi_time.shape[0]
            tick_total = ticktrade.shape[0]
            volume_total = ticktrade.volume.sum()
            value_total = ticktrade.value.sum()
        else:
            in_sample_vi_time = vi_time[vi_time.close_time // 10000000000 < split_point]
            bar_num_benchmark = in_sample_vi_time.shape[0]
            in_sample_ticktrade = ticktrade[ticktrade.time // 10000000000 < split_point]
            tick_total = in_sample_ticktrade.shape[0]
            volume_total = in_sample_ticktrade.volume.sum()
            value_total = in_sample_ticktrade.value.sum()
        tick_per_bar = int(tick_total // bar_num_benchmark)
        volume_per_bar = int(volume_total // bar_num_benchmark)
        value_per_bar = int(value_total // bar_num_benchmark)
        tick_per_bar = nice_number(tick_per_bar)
        volume_per_bar = nice_number(volume_per_bar)
        value_per_bar = nice_number(value_per_bar)
        vi_tick = VI_tickbar(ticktrade, tick_per_bar)
        vi_volume = VI_volumebar(ticktrade, volume_per_bar)
        vi_value = VI_valuebar(ticktrade, value_per_bar)
        vi_time.to_csv('pool_time_' + str(period) + '_' + stock_code + '.csv')
        vi_tick.to_csv('pool_tick_' + str(period) + '_' + stock_code + '.csv')
        vi_volume.to_csv('pool_volume_' + str(period) + '_' + stock_code + '.csv')
        vi_value.to_csv('pool_value_' + str(period) + '_' + stock_code + '.csv')

    try:
        sample_ticktrade = adjusted_ticktrade(stock_code, starttime, endtime)
    except Exception:
        log_file = open('hs300_data.txt', 'a')
        print(time.asctime()+'Error when fetch ticktrade with: '+stock_code, file=log_file)
        log_file.close()
    try:
        for time_period in periods:
            straight_service(sample_ticktrade, split_year, time_period)
    except Exception as e:
        print(e)
        log_file = open('hs300_data.txt', 'a')
        print(time.asctime() + 'Error when making bars with: ' + stock_code, file=log_file)
        log_file.close()
    global count
    count = count + 1
    print(str(count) + ': ' + stock_code + ' Completed!')


def get_vi_timebar(stock_profile, starttime, endtime, freqs):
    stock_code = stock_profile[0].split('.')[0]
    sample_ticktrade = adjusted_ticktrade(stock_profile[0], starttime, endtime)
    for freq in freqs:
        vi_time = vi_time_full_period(sample_ticktrade, freq)
        vi_time.to_csv('vitime_M' + str(freq) + '_' + stock_code + '.csv')
        del vi_time
    print(stock_code + ' Completed!')


# 构造有向成交量 日度基准K线数据
def dailymark_nontimebar(ticktrade, bar_per_day, bar_type='volume'):
    trade_df = ticktrade.copy()
    trade_df['value'] = trade_df.price * trade_df.volume
    trade_df['day_flag'] = trade_df.time // 1000000
    grouped = trade_df.groupby('day_flag')
    if bar_type == 'tick':
        trade_df['tick'] = 1
        trade_df['cusum_tick'] = grouped.tick.cumsum()
        trade_df['daily_tick'] = trade_df.day_flag.map(grouped.tick.sum())
        trade_df['volume_progress'] = np.ceil(trade_df.cusum_tick / trade_df.daily_tick * bar_per_day).astype(int)
    elif bar_type == 'value':
        trade_df['cusum_value'] = grouped.value.cumsum()
        trade_df['daily_value'] = trade_df.day_flag.map(grouped.value.sum())
        trade_df['volume_progress'] = np.ceil(trade_df.cusum_value / trade_df.daily_value * bar_per_day).astype(int)
    else:
        trade_df['cusum_volume'] = grouped.volume.cumsum()
        trade_df['daily_volume'] = trade_df.day_flag.map(grouped.volume.sum())
        trade_df['volume_progress'] = np.ceil(trade_df.cusum_volume / trade_df.daily_volume * bar_per_day).astype(int)
    signed_volume = trade_df.groupby(by=['day_flag', 'volume_progress', 'bsc']).volume.sum().unstack()
    signed_volume = signed_volume.fillna(0)
    grouped = trade_df.groupby(['day_flag', 'volume_progress'])
    volume_bar = pd.concat([grouped.time.first(), grouped.time.last(), grouped.price.first(), grouped.price.max(),
                            grouped.price.min(), grouped.price.last(), grouped.volume.sum(), grouped.value.sum()], axis=1)
    volume_bar.columns = ['open_time', 'close_time', 'open', 'high', 'low', 'close', 'volume', 'value']
    volume_bar = pd.concat([volume_bar, signed_volume], axis=1)
    return volume_bar.reset_index()


def dailymark_timebar(ticktrade, min_period):
    trade_df = ticktrade.copy()
    trade_df['value'] = trade_df.price * trade_df.volume
    trade_df['day_flag'] = trade_df.time // 1000000
    trade_df['minute_flag'] = trade_df.time % 1000000 // (min_period * 100)
    signed_volume = trade_df.groupby(by=['day_flag', 'minute_flag', 'bsc']).volume.sum().unstack()
    signed_volume = signed_volume.fillna(0)
    grouped = trade_df.groupby(['day_flag', 'minute_flag'])
    volume_bar = pd.concat([grouped.time.first(), grouped.time.last(), grouped.price.first(), grouped.price.max(),
                            grouped.price.min(), grouped.price.last(), grouped.volume.sum(), grouped.value.sum()], axis=1)
    volume_bar.columns = ['open_time', 'close_time', 'open', 'high', 'low', 'close', 'volume', 'value']
    volume_bar = pd.concat([volume_bar, signed_volume], axis=1)
    return volume_bar.reset_index()


def dailymark_bar(stock_profile, bar_per_day, minute_period, starttime, endtime):
    stock_code = stock_profile[0].split('.')[0]
    sample_ticktrade = adjusted_ticktrade(stock_profile[0], starttime, endtime)
    dm_time = dailymark_timebar(sample_ticktrade, minute_period)
    dm_tick = dailymark_nontimebar(sample_ticktrade, bar_per_day, 'tick')
    dm_volume = dailymark_nontimebar(sample_ticktrade, bar_per_day)
    dm_value = dailymark_nontimebar(sample_ticktrade, bar_per_day, 'value')
    dm_time.to_csv('dm_time_' + stock_code + '.csv')
    dm_tick.to_csv('dm_tick_' + stock_code + '.csv')
    dm_volume.to_csv('dm_volume_' + stock_code + '.csv')
    dm_value.to_csv('dm_value_' + stock_code + '.csv')
    print(stock_profile[0] + ' Completed!')


def get_tickdata(stock_code, start_time, end_time):
    print(time.asctime(), ': Start with', stock_code[:6])
    ebq_sample = ebq_data()
    tick_df = ebq_sample.TickData(stock_code, start_time, end_time)
    return tick_df


def get_indextick(index_code, start_time, end_time):
    index_tick = get_tickdata(index_code, start_time, end_time)
    index_tick.value /= 100
    index_tick['ret'] = index_tick.price / index_tick.price.shift(1) - 1
    index_tick['bsc'] = np.sign(index_tick.ret)
    index_tick = index_tick.dropna(axis=0).drop('ret', axis=1)
    return index_tick


def index_timebar(index_tick, time_period):
    trade_df = index_tick.copy()
    # trade_df = trade_df[trade_df.time // 100 % 10000 > 925][trade_df.time // 100 % 10000 < 1500]
    trade_df_am = trade_df[trade_df.time // 100 % 10000 > 930][trade_df.time // 100 % 10000 < 1130]
    trade_df_pm = trade_df[trade_df.time // 100 % 10000 > 1300][trade_df.time // 100 % 10000 < 1500]
    trade_df = pd.concat([trade_df_am, trade_df_pm], axis=0).sort_index()
    if time_period == 60:
        trade_df['time_flag'] = trade_df.time // 1000000
        trade_df['minute_flag'] = (trade_df.time % 1000000 - 84500) // 18500
    else:
        trade_df['time_flag'] = trade_df.time // 10000
        trade_df['minute_flag'] = trade_df.time % 10000 // (time_period * 100)
    signed_volume = trade_df.groupby(by=['time_flag', 'minute_flag', 'bsc']).volume.sum().unstack()
    signed_volume = signed_volume.fillna(0)
    grouped = trade_df.groupby(['time_flag', 'minute_flag'])
    time_bar = pd.concat([grouped.time.first(), grouped.time.last(), grouped.price.first(), grouped.price.max(),
                          grouped.price.min(), grouped.price.last(), grouped.volume.sum(), grouped.value.sum()], axis=1)
    time_bar.columns = ['open_time', 'close_time', 'open', 'high', 'low', 'close', 'volume', 'value']
    time_bar = pd.concat([time_bar, signed_volume], axis=1)
    time_bar = time_bar.reset_index(drop=True)
    time_bar.columns = ['open_time', 'close_time', 'open', 'high', 'low', 'close', 'volume', 'value', b'S', b'N', b'B']
    return time_bar


def index_volumebar(index_tick, volume_per_bar):
    trade_df = index_tick.copy()
    trade_df['cusum_volume'] = (np.floor(trade_df.volume.cumsum() / volume_per_bar)).astype(int)
    signed_volume = trade_df.groupby(by=['cusum_volume', 'bsc']).volume.sum().unstack()
    signed_volume = signed_volume.fillna(0)
    grouped = trade_df.groupby('cusum_volume')
    volume_bar = pd.concat([grouped.time.first(), grouped.time.last(), grouped.price.first(), grouped.price.max(),
                            grouped.price.min(), grouped.price.last(), grouped.volume.sum(), grouped.value.sum()], axis=1)
    volume_bar.columns = ['open_time', 'close_time', 'open', 'high', 'low', 'close', 'volume', 'value']
    volume_bar = pd.concat([volume_bar, signed_volume], axis=1)
    volume_bar.columns = ['open_time', 'close_time', 'open', 'high', 'low', 'close', 'volume', 'value', b'S', b'N', b'B']
    return volume_bar


def index_valuebar(index_tick, value_per_bar):
    trade_df = index_tick.copy()
    trade_df['cusum_value'] = (np.floor(trade_df.value.cumsum() / value_per_bar)).astype(int)
    signed_volume = trade_df.groupby(by=['cusum_value', 'bsc']).volume.sum().unstack()
    signed_volume = signed_volume.fillna(0)
    grouped = trade_df.groupby('cusum_value')
    value_bar = pd.concat([grouped.time.first(), grouped.time.last(), grouped.price.first(), grouped.price.max(),
                           grouped.price.min(), grouped.price.last(), grouped.volume.sum(), grouped.value.sum()],
                          axis=1)
    value_bar.columns = ['open_time', 'close_time', 'open', 'high', 'low', 'close', 'volume', 'value']
    value_bar = pd.concat([value_bar, signed_volume], axis=1)
    value_bar.columns = ['open_time', 'close_time', 'open', 'high', 'low', 'close', 'volume', 'value', b'S', b'N', b'B']
    return value_bar


def vol_direction(data, vol_window, test_start, model='rf', outcome='stats'):
    X = data.copy()
    X['vpin'] = abs(X[b'B'] - X[b'S']) / (X[b'B'] + X[b'S'])
    if type(test_start) is int:
        X.to_csv('X.csv')
        X = pd.read_csv('X.csv', index_col=0, parse_dates=[1, 2])
        X['year'] = [x.year for x in X.open_time]
        if test_start < 2015:
            X_train = X[X.year < 2017]
            X_test = X[X.year >= 2017]
        else:
            X_train = X[X.year < test_start]
            X_test = X[X.year >= test_start]
    elif type(test_start) is float and test_start<1:
        X_train = X[:int(len(X) * test_start)]
        X_test = X[int(len(X) * test_start):]
    else:
        X_train = X[:int(len(X)*0.8)]
        X_test = X[int(len(X)*0.8):]

    # Output
    close_train = X_train['close']
    ret_train = close_train / close_train.shift(1) - 1
    ret_train = ret_train.reset_index(drop=True)
    hl_train = X_train.high.rolling(vol_window).max() / X_train.low.rolling(vol_window).min() - 1
    hl_train = hl_train.reset_index(drop=True)
    vol_train = ret_train.rolling(vol_window).std()
    target_vol_train = vol_train[2*vol_window-1:].reset_index(drop=True)

    close_test = X_test['close']
    ret_test = close_test / close_test.shift(1) - 1
    ret_test = ret_test.reset_index(drop=True)
    hl_test = X_test.high.rolling(vol_window).max() / X_test.low.rolling(vol_window).min() - 1
    hl_test = hl_test.reset_index(drop=True)
    vol_test = ret_test.rolling(vol_window).std()
    target_vol_test = vol_test[2*vol_window-1:].reset_index(drop=True)

    # TrainSet Input
    trainset = pd.DataFrame(0, index=[i for i in range(0, vol_window)], columns=[])
    for i in range(vol_window, (len(X_train) - vol_window+1)):
        x = X_train.vpin[i - vol_window:i, ]
        x = x.reset_index(drop=True)
        trainset[str(i - vol_window)] = x

    # trainset = (trainset - trainset.mean())
    trainset = trainset.T
    trainset['vpin_mean'] = trainset.mean(1)
    trainset['vpin_std'] = trainset.std(1)

    vol_train_last = vol_train[vol_window-1:-vol_window].reset_index(drop=True)
    trainset['vol'] = [x for x in vol_train_last]

    ret_train_last = close_train / close_train.shift(vol_window) - 1
    ret_train_last = ret_train_last[vol_window-1:-vol_window].reset_index(drop=True)
    trainset['ret'] = [x for x in ret_train_last]

    hl_train_last = hl_train[vol_window - 1:-vol_window].reset_index(drop=True)
    trainset['hl_ratio'] = [x for x in hl_train_last]

    time_train = X_train.close_time.reset_index(drop=True)[vol_window-1:-vol_window].reset_index(drop=True)
    trainset['close_time'] = [x for x in time_train]

    # TestSet
    testset = pd.DataFrame(0, index=[i for i in range(0, vol_window)], columns=[])
    for i in range(vol_window, (len(X_test) - vol_window+1)):
        x = X_test.vpin[i - vol_window:i, ]
        x = x.reset_index(drop=True)
        testset[str(i - vol_window)] = x

    # testset = (testset - testset.mean())
    testset = testset.T
    testset['vpin_mean'] = testset.mean(1)
    testset['vpin_std'] = testset.std(1)

    vol_test_last = vol_test[vol_window-1:-vol_window].reset_index(drop=True)
    testset['vol'] = [x for x in vol_test_last]

    ret_test_last = close_test / close_test.shift(vol_window) - 1
    ret_test_last = ret_test_last[vol_window - 1:-vol_window].reset_index(drop=True)
    testset['ret'] = [x for x in ret_test_last]

    hl_test_last = hl_test[vol_window - 1:-vol_window].reset_index(drop=True)
    testset['hl_ratio'] = [x for x in hl_test_last]

    time_test = X_test.close_time.reset_index(drop=True)[vol_window - 1:-vol_window].reset_index(drop=True)
    testset['close_time'] = [x for x in time_test]

    # Clear NaN Data
    trainset['vol_train'] = [x for x in target_vol_train]
    trainset['vol_direction'] = np.sign(trainset.vol_train - trainset.vol)
    trainset = trainset.dropna()
    target_vol_train = trainset['vol_direction']
    predict_time_train = trainset['close_time']
    trainset = trainset[['vpin_mean', 'vpin_std', 'vol', 'ret', 'hl_ratio']]
    # trainset = trainset.drop(labels=['vol_train', 'vol_direction'], axis=1)
    # trainset = trainset.drop(labels=['vol_train', 'vol_direction', 'vol', 'ret'], axis=1)

    testset['vol_test'] = [x for x in target_vol_test]
    testset['vol_direction'] = np.sign(testset.vol_test - testset.vol)
    testset = testset.dropna()
    target_vol_test = testset['vol_direction']
    predict_time_test = testset['close_time']
    testset = testset[['vpin_mean', 'vpin_std', 'vol', 'ret', 'hl_ratio']]
    # testset = testset.drop(labels=['vol_test', 'vol_direction'], axis=1)
    # testset = testset.drop(labels=['vol_test', 'vol_direction', 'vol', 'ret'], axis=1)

    if model == 'lr':
        # LR Model
        regressor = LogisticRegression(C=1000)
        regressor.fit(trainset, target_vol_train)

        y_pred_train = regressor.predict(trainset)
        train_mse = accuracy_score(target_vol_train, y_pred_train)
        y_pred_test = regressor.predict(testset)
        test_mse = accuracy_score(target_vol_test, y_pred_test)
    elif model == 'svm':
        # SVM Model
        regressor = SVC(kernel='rbf', C=1000)
        regressor.fit(trainset, target_vol_train)

        y_pred_train = regressor.predict(trainset)
        train_mse = accuracy_score(target_vol_train, y_pred_train)
        y_pred_test = regressor.predict(testset)
        test_mse = accuracy_score(target_vol_test, y_pred_test)
    else:
        # RF Model
        regressor = RandomForestClassifier(max_depth=3)
        regressor.fit(trainset, target_vol_train)

        y_pred_train = regressor.predict(trainset)
        train_mse = accuracy_score(target_vol_train, y_pred_train)
        y_pred_test = regressor.predict(testset)
        test_mse = accuracy_score(target_vol_test, y_pred_test)
    if outcome == 'pred':
        return pd.Series(y_pred_train, index=predict_time_train), pd.Series(y_pred_test, index=predict_time_test)
    return train_mse, test_mse


def GetData(index_list, data_type, start_date, end_date, period):
    if w.isconnected() is False:
        w.start()
    index_data = w.wsd(index_list, data_type, start_date, end_date, "unit=1;Period="+period+";PriceAdj=F")
    index_pdata = pd.DataFrame(index_data.Data).T
    index_pdata.index = index_data.Times
    index_pdata.index.name = 'datetime'
    if type(index_list) == str:
        index_pdata.columns = [index_list]
    else:
        index_pdata.columns = index_list
    index_pdata.index = pd.to_datetime(index_pdata.index)
    print(data_type+' data of period: '+period+' has successfully retrieved!')
    return index_pdata


def GetUniData(index_code, data_type, start_date, end_date, period):
    index_data = w.wsd(index_code, data_type, start_date, end_date, "unit=1;Period="+period+";PriceAdj=F")
    index_pdata = pd.DataFrame(index_data.Data).T
    index_pdata.index = index_data.Times
    index_pdata.index.name = 'datetime'
    if type(data_type) == str:
        column_names = [name.strip() for name in data_type.split(',')]
        index_pdata.columns = column_names
    else:
        index_pdata.columns = data_type
    index_pdata.index = pd.to_datetime(index_pdata.index)
    print(index_code+' data of period: '+period+' has successfully retrieved!')
    return index_pdata


def ExpoMean(ts_data, alpha=0.2):
    return ts_data.ewm(alpha=alpha, ignore_na=True).mean().dropna().last()


def Signalized(signal, resample_period='D', alpha=0.1):
    ewm_signal = signal.ewm(alpha=alpha, ignore_na=True).mean()
    signalized = ewm_signal.resample(resample_period).last().dropna()
    return signalized


def rotate_nav(signal, index_ret):
    selected = (signal.rank(axis=1).T // signal.rank(axis=1).max(1).T).T.shift(1)
    holding = (selected.T / selected.T.sum()).T
    rotate_index = holding.dropna(axis=0).index.intersection(index_ret.index)
    rotate_return = (index_ret.ix[rotate_index] * holding.ix[rotate_index]).sum(axis=1)
    rotate_nav = (rotate_return + 1).cumprod()
    rotate_nav.name = 'rotate'
    index_nav = (index_ret.ix[rotate_index] + 1).cumprod()
    nav_cmp = pd.concat([index_nav, rotate_nav], axis=1)
    return nav_cmp


def get_return_and_vol(pv_mat, period='W'):
    close_mat = pv_mat[['close_time', 'close']]
    close_mat['ret'] = close_mat.close / close_mat.close.shift(1) - 1
    close_mat['close_time'] = pd.to_datetime([str(x) for x in close_mat.close_time])
    close_mat = close_mat.set_index('close_time')
    close_series = close_mat.resample(period).close.last()
    vol_series = close_mat.resample(period).ret.std()
    combo = pd.concat([close_series, vol_series], axis=1).dropna()
    combo.columns = ['close', 'vol']
    combo['ret'] = combo.close / combo.close.shift(1) - 1
    return combo


def clean_stock_list(stock_list_data):
    stock_list = [str(x + 1000000)[-6:] for x in stock_list_data]
    stock_list = [x+'.XSHG' if x.startswith('6') else x+'.XSHE' for x in stock_list]
    return stock_list


def seperate_time_sample(start_years, end_years):
    start_time_list = [str(x)+'-01-01 09:00:00' for x in start_years]
    end_time_list = [str(x)+'-12-31 15:30:00' for x in end_years]
    return zip(start_time_list, end_time_list)


# 读取需要的文件名
def listdir_me(path, prefix, file_type='.csv', is_pre=True):
    list_name = []   # 带完整路径
    files = []   # 仅文件名
    for file in os.listdir(path):
        file_path = os.path.join(path, file)
        if os.path.isdir(file_path) or os.path.splitext(file)[1] != file_type:
            continue
        else:
            (shortname, extension) = os.path.splitext(file)  # 获取文件名和后缀名
            if is_pre:
                if not shortname.startswith(prefix):
                    continue
            else:
                if not shortname.endswith(prefix):
                    continue
            list_name.append(file_path)
            files.append(shortname+file_type)
    return files


def minute_resample(stock_code, one_minute_data, resample_period):
    data_copy = one_minute_data.copy()
    if resample_period == 60:
        data_copy['time_flag'] = data_copy.time // 1000000
        data_copy['minute_flag'] = (data_copy.time % 1000000 - 84500) // 18500
    elif resample_period == 120:
        data_copy['time_flag'] = data_copy.time // 1000000
        data_copy['minute_flag'] = data_copy.time % 1000000 // 120000
    else:
        data_copy['time_flag'] = data_copy.time // 10000
        data_copy['minute_flag'] = data_copy.time % 10000 // (resample_period * 100)
    grouped = data_copy.groupby(['time_flag', 'minute_flag'])
    time_bar = pd.concat([grouped.time.first(), grouped.open.first(), grouped.high.max(),
                          grouped.low.min(), grouped.close.last(), grouped.volume.sum(), grouped.value.sum()], axis=1)
    time_bar = time_bar.reset_index().drop(labels=['time_flag', 'minute_flag'], axis=1)

    reinstate_map = stock_reinstate(stock_code, start_time.split(' ')[0], end_time.split(' ')[0])
    reinstate_map = reinstate_map.reset_index().drop_duplicates().set_index('index').adjfactor
    time_bar['date'] = time_bar.time // 1000000
    time_bar['adjfactor'] = time_bar.date.map(reinstate_map)
    time_bar['avg'] = time_bar.value / np.where(abs(time_bar.volume) > 0.00001, time_bar.volume, np.nan) * time_bar.adjfactor

    return time_bar.drop(labels=['date', 'adjfactor'], axis=1)


def minute_resample_task(stock_csv):
    use_path = r'one_minute_all_stocks/'
    one_minute_data = pd.read_csv(use_path+stock_csv, index_col=0)
    five_minute = minute_resample(stock_csv, one_minute_data, 5)
    ten_minute = minute_resample(stock_csv, one_minute_data, 10)
    thirty_minute = minute_resample(stock_csv, one_minute_data, 30)
    hour = minute_resample(stock_csv, one_minute_data, 60)
    five_minute.to_csv(r'm5_all_stocks/' + stock_csv)
    ten_minute.to_csv(r'm10_all_stocks/' + stock_csv)
    thirty_minute.to_csv(r'm30_all_stocks/' + stock_csv)
    hour.to_csv(r'm60_all_stocks/' + stock_csv)
    print(stock_csv, ' has processed.')


# 显示循环进度
def show_progress(num, total, name):
    rate = float(num) / total
    rate_num = (int(rate * 100) + 1)
    n = rate_num // 3
    r = '\r %s -- |%s>%s| %d%%' % (name, "=" * n, "-" * (33 - n), rate_num)
    sys.stdout.write(r)
    sys.stdout.flush()
    return


def download_ticktrade(stock_code, ebq_sample, start_time, end_time):
    # print(time.asctime(), ': Start ticktrade data with', stock_code[:6])
    # ebq_sample = ebq_data()
    ticktrade = ebq_sample.TickTrade(stock_code, start_time, end_time)
    ticktrade.to_pickle(r'E:\HJC\EBQ_Data_SDK\zxx\ticktrade_' + stock_code[:6])
    return ticktrade


def processed_stocks(dir_name):
    stocks = []
    for file in os.listdir(dir_name):
        file_path = os.path.join(dir_name, file)
        if os.path.isdir(file_path) or os.path.splitext(file)[1] != '.csv':
            continue
        else:
            (shortname, extension) = os.path.splitext(file)
            stocks.append(shortname)
    return stocks


if __name__ == '__main__':

    multiprocessing.freeze_support()
    PROCESSES = 6
    '''
    hs300_constituents = pd.read_csv('Other_Constituents.csv')
    STOCK_LIST = clean_stock_list(hs300_constituents.STOCK_CODE)
    print('stock list fetched with stock num: '+str(len(STOCK_LIST)))
    '''
    instance = jq_data()
    instance.connect("10.84.137.108", 7000)
    instance.login("13100000002", "gdzq12345")
    STOCK_LIST = [x.decode() for x in instance.get_all_securities(Stock)]

    done_list = processed_stocks('stock_minute_data')
    for stock_code in done_list:
        STOCK_LIST.remove(stock_code)
    # done_name = set([x.split('_')[0] for x in done_list])
    # total_stocks = len(STOCK_LIST)
    # print('Total stock number: {}'.format(total_stocks))
    del instance

    # ebq_sample = ebq_data()
    start_time = '2010-01-01 09:00:00'
    end_time = '2020-04-30 16:00:00'

    # start_years = [2010, 2012, 2014, 2015, 2016, 2017]
    # end_years = [2011, 2013, 2014, 2015, 2016, 2018]

    # STOCK_LIST = pd.read_csv('stock_code_dict.csv', index_col=0).Code.to_list()[434:]
    print('remaining stock num: {}'.format(len(STOCK_LIST)))
    # STOCK_LIST = listdir_me('one_minute_all_stocks', '')
    print('start at: ', time.asctime())
    pool = multiprocessing.Pool(PROCESSES)
    for item in STOCK_LIST:
        # for start_time, end_time in seperate_time_sample(start_years, end_years):
        pool.apply_async(save_normal_minute_data, (item, start_time, end_time, 5, ))
        # pool.apply_async(download_ticktrade, (item, ebq_sample, start_time, end_time, ))
        # pool.apply_async(minute_resample_task, (item, ))
    pool.close()
    pool.join()
    print('end at: ', time.asctime())
    '''
    for item in List:
        get_standard_bar(item, '2013-01-01 09:00:00', '2017-12-31 15:30:00', [5, 10, 30, 60])
    end = time.time()
    print('Total spent time:', end - start)
    '''
