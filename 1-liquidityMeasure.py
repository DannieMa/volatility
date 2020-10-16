# -*- coding: utf-8 -*-

from jq_data_capi import *
import pandas as pd
import numpy as np
import itertools
from multiprocessing import Pool
import time
import statsmodels.api as sm
import warnings

warnings.filterwarnings('ignore')

class liquid_funcs(object):

    def __init__(self):
        pass

    def get_amihud(self, df_min):
        """
        :param df_min: 5min
        :return: amihud
        """
        df_min = df_min.loc[df_min.volume > 0, ]
        df_min['ret'] = abs(df_min.close / df_min.open - 1)
        amihud = (df_min.ret / df_min.amount).mean()
        return amihud

    def get_Qspread(self, df_tick):
        """
        计算报价价差
        :param df_tick: tick,level 2
        :return: q_spread
        """
        df_tick = df_tick.loc[(df_tick.volume > 0) & (df_tick.price > 0) & (df_tick.ap1 > 0) & (df_tick.bp1 > 0), ]
        spread_ls = 2 * (df_tick.ap1 - df_tick.bp1) / (df_tick.ap1 + df_tick.bp1)
        weight_ls = df_tick.volume / df_tick.volume.sum()
        q_spread = (spread_ls * weight_ls).sum()
        return q_spread

    def get_Espread(self, df_tick):
        """
        计算有效价差
        :param df_tick: tick, level2
        :return: e_spread
        """
        df_tick = df_tick.loc[(df_tick.volume > 0) & (df_tick.price > 0)& (df_tick.ap1 > 0)& (df_tick.bp1 > 0),]
        spread_ls = 2 * abs(np.log(df_tick.price) - np.log(0.5 * df_tick.ap1 + 0.5 * df_tick.bp1))
        weight_ls = df_tick.volume / df_tick.volume.sum()
        e_spread = (spread_ls * weight_ls).sum()
        return e_spread

    def get_slope(self, df_tick):
        """
        计算订单簿斜率
        :param df_tick: tick,level 2
        :return: slope
        """
        df_tick = df_tick.loc[(df_tick.volume > 0) & (df_tick.price > 0),]
        df_tick = df_tick.set_index('time')

        df = df_tick[['av10', 'av9', 'av8', 'av7', 'av6', 'av5', 'av4', 'av3', 'av2', 'av1',
                      'bv1', 'bv2', 'bv3', 'bv4', 'bv5', 'bv6', 'bv7', 'bv8', 'bv9', 'bv10',
                      'ap10', 'ap9', 'ap8', 'ap7', 'ap6', 'ap5', 'ap4', 'ap3', 'ap2', 'ap1',
                      'bp1', 'bp2', 'bp3', 'bp4', 'bp5', 'bp6', 'bp7', 'bp8', 'bp9', 'bp10']]
        df = df.unstack().reset_index()
        df.columns = ['temp', 'time', 'v']
        df['temp_i'] = [int(y[2:]) for y in df.temp]
        df['temp_v'] = [x[:2] for x in df.temp]
        df = pd.pivot_table(df, index=['time', 'temp_i'], columns=['temp_v'], values='v')
        df = df.reset_index()

        df_add = df.loc[df.temp_i == 1, ]
        df_add.temp_i = 0
        df_add.ap = (df_add.ap + df_add.bp) / 2
        df_add.bp = (df_add.ap + df_add.bp) / 2
        df_add.av = df_add.av / (df_add.av + 1)
        df_add.bv = df_add.bv / (df_add.bv + 1)

        df_add2 = df.loc[df.temp_i == 1, ]
        df_add2.temp_i = -1
        df_add2.ap, df_add2.bp, df_add2.av, df_add2.bv = np.nan, np.nan, np.nan, np.nan

        df = df.append(df_add).append(df_add2)
        df = df.sort_values(['time', 'temp_i'])

        df['sa'] = (df.av / df.av.shift() - 1) / (df.ap / df.ap.shift() - 1)
        df['sb'] = (df.bv / df.bv.shift() - 1) / (df.bp / df.bp.shift() - 1)
        df = df.drop(df.loc[(df.temp_i == -1) | (df.temp_i == 0)].index)

        df = df[['time', 'sa', 'sb']].groupby('time').mean()
        df['slope'] = 2 * (df.sa - df.sb) / (df.sa + df.sb)

        weight_ls = df_tick.volume / df_tick.volume.sum()
        slope = (df.slope * weight_ls).sum()

        return slope

    def get_oib(self, df_trade):
        """
        根据逐笔成交数据，计算订单流不平衡
        :param df_trade: trade
        :return: oib_num,买卖交易笔数不平衡
        """
        df_trade = df_trade.loc[(df_trade.price > 0) & (df_trade.volume > 0),]
        df_trade['amount'] = df_trade.price * df_trade.volume
        if len(df_trade) == 0:
            return np.nan, np.nan, np.nan
        oib_num = (len(df_trade.loc[df_trade.bsc == 'B', :]) - len(df_trade.loc[df_trade.bsc == 'S', :])) / len(
            df_trade)
        oib_amount = (df_trade.loc[df_trade.bsc == 'B', 'amount'].sum() - df_trade.loc[
            df_trade.bsc == 'S', 'amount'].sum()) / df_trade.amount.sum()
        oib_volume = (df_trade.loc[df_trade.bsc == 'B', 'volume'].sum() - df_trade.loc[
            df_trade.bsc == 'S', 'volume'].sum()) / df_trade.volume.sum()

        return oib_num, oib_amount, oib_volume

    def get_oib_1min(self, df_trade):
        df_trade = df_trade.loc[(df_trade.price > 0) & (df_trade.volume > 0),]
        df_trade['amount'] = df_trade.price * df_trade.volume
        df_trade['min1'] = [x // 100000 for x in df_trade.time]
        df_trade = df_trade[['min1', 'bsc', 'amount']].groupby(['min1', 'bsc']).sum().reset_index()
        df_trade = df_trade.pivot(index='min1', columns='bsc', values='amount')
        df_trade['oib'] = (df_trade.B - df_trade.S) / (df_trade.B + df_trade.S)
        df_trade = df_trade.reset_index()

        return df_trade[['min1', 'oib']]

    def get_liquid_cost(self, df_tick, df_trade):
        """
        计算流动性成本
        :param df_tick: tick,level2
        :return: marginal_cost,liquid_cost
        """
        df_tick = df_tick.loc[(df_tick.volume > 0) & (df_tick.price > 0),]
        df_tick.price /= 10000
        df_tick = df_tick.set_index('time')
        weight_ls = df_tick.volume / df_tick.volume.sum()

        sellPrice = df_tick[['ap' + str(x) for x in range(1, 11)]] / 10000
        sellX = df_tick[['av' + str(x) for x in range(1, 11)]]
        col_a = ['a' + str(x) for x in range(1, 11)]
        sellX.columns = col_a
        sellPrice.columns = col_a
        sellXSum = sellX.cumsum(axis=1)
        sellY = (sellPrice * sellX).cumsum(axis=1) / sellXSum

        buyPrice = df_tick[['bp' + str(x) for x in range(1, 11)]] / 10000
        buyX = df_tick[['bv' + str(x) for x in range(1, 11)]]
        col_b = ['b' + str(x) for x in range(1, 11)]
        buyX.columns = col_b
        buyPrice.columns = col_b
        buyXSum = buyX.cumsum(axis=1)
        buyY = (buyPrice * buyX).cumsum(axis=1) / buyXSum

        Y = pd.concat([sellY, buyY], axis=1).T
        Y = (Y - Y.mean()) / Y.std()
        X = pd.concat([sellXSum, buyXSum], axis=1).T
        X = (X - X.mean()) / X.std()

        lambda_ls = ((X * Y).sum() - ((X.sum()) * (Y.sum())) / len(X)) / (
                (X ** 2).sum() - ((X.sum()) ** 2) / len(X))
        marginal_cost = (lambda_ls * weight_ls).sum()

        lambda_df = pd.DataFrame(lambda_ls, columns=['lambda_t'])
        lambda_df = lambda_df.reset_index()
        lambda_df['min1'] = [x // 100000 for x in lambda_df.time]
        lambda_df = lambda_df[['min1', 'lambda_t']].groupby('min1').mean()
        lambda_df = lambda_df.reset_index()

        oib_df = self.get_oib_1min(df_trade)

        df1min = df_tick[['price', 'volume']]
        df1min['min1'] = [x // 100000 for x in df1min.index]
        vol1min = df1min[['min1', 'volume']].groupby('min1').sum()
        df1min = df1min[['min1', 'price']].groupby('min1').ohlc()['price']
        df1min['ret'] = df1min.close / df1min.open - 1
        df1min['weight_1min'] = vol1min / vol1min.sum()
        df1min = df1min.reset_index()
        df1min = df1min[['min1', 'close', 'ret','weight_1min']]
        df1min = df1min.merge(lambda_df, on=['min1'], how='inner').merge(oib_df, on=['min1'], how='inner')
        df1min['ilc'] = df1min.lambda_t * df1min.oib / (df1min.close ** 2)
        df1min=df1min.dropna()

        orth_cost = (df1min.weight_1min * (sm.OLS(df1min.ilc, sm.add_constant(df1min.oib)).fit()).resid).sum()

        return marginal_cost, orth_cost

# def get_stock_liquid(self,symbol, start_date, end_date):
#     """
#     计算某只股票在一段时间内的流动性指标
#     :param symbol: eg. '000001.XSHE'
#     :param start_date: eg. '2010-01-01'
#     :param end_date: eg. '2010-01-01'
#     :return: df
#     """
#     start_t = ' 09:30:00'
#     end_t = ' 14:57:00'
#     min_data_path='E:/JT/SDK_test/stock_minute_data/'
#
#     tick_df = self.get_lv2_data(symbol, start_date + start_t, end_date + end_t)
#     tick_df['date']=tick_df.time.apply(lambda x: str(x)[:8])
#     trade_df = self.get_trade_df(symbol, start_date + start_t, end_date + end_t)
#     trade_df['date']=trade_df.time.apply(lambda x: str(x)[:8])
#     min_df=pd.read_csv(min_data_path+symbol+'.csv',index_col=0)
#     min_df['date']=min_df.Date.apply(lambda x: x[:10].replace('-',''))
#
#     day_ls=list(set(tick_df.date))
#
#     for day in day_ls:
#
#
#     return df


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

    def get_lv2_data(self, symbol, start_time, end_time):
        tick_data = self.instance.get_stock_ticks(symbol, start_time, end_time)
        tick_data = [x for x in tick_data if  x.current > 0]
        lv2_data = [[x.time, x.current, x.volume, x.bought.a10v, x.bought.a9v, x.bought.a8v, x.bought.a7v, x.bought.a6v,
                     x.bought.a5v,
                     x.bought.a4v, x.bought.a3v, x.bought.a2v, x.bought.a1v, x.bought.b1v, x.bought.b2v, x.bought.b3v,
                     x.bought.b4v, x.bought.b5v, x.bought.b6v, x.bought.b7v, x.bought.b8v, x.bought.b9v, x.bought.b10v,
                     x.bought.a10p, x.bought.a9p, x.bought.a8p, x.bought.a7p, x.bought.a6p, x.bought.a5p,
                     x.bought.a4p, x.bought.a3p, x.bought.a2p, x.bought.a1p, x.bought.b1p, x.bought.b2p, x.bought.b3p,
                     x.bought.b4p, x.bought.b5p, x.bought.b6p, x.bought.b7p, x.bought.b8p, x.bought.b9p, x.bought.b10p]
                    for x in tick_data]
        lv2_columns = ['time', 'price', 'volume'] + ['av{}'.format(x) for x in range(10, 0, -1)] + ['bv{}'.format(x) for
                                                                                                    x in
                                                                                                    range(1, 11)] + [
                          'ap{}'.format(x) for x in range(10, 0, -1)] + ['bp{}'.format(x) for x in range(1, 11)]
        lv2_frame = pd.DataFrame(lv2_data, columns=lv2_columns)
        lv2_frame['date'] = lv2_frame.time.apply(lambda x: int(str(x)[:8]))
        lv2_frame['code'] = symbol
        return lv2_frame

    def trade_days(self, start_date, end_date):
        day_ls = self.instance.get_trade_days(start_date, end_date)
        day_ls = [d.decode() for d in day_ls]
        return day_ls

    def get_order_df(self, symbol, start_time, end_time):
        order_data = self.instance.get_orders(symbol, start_time, end_time)
        df = []
        for x in order_data:
            df.append([x.time, x.side, x.price, x.volume, x.count, x.orders])
        df = pd.DataFrame(df, columns=['time', 'side', 'price', 'volume', 'num', 'orders'])
        df['date'] = df.time.apply(lambda x: int(str(x)[:8]))
        df['code'] = symbol
        return df

    def get_trade_df(self, symbol, start_time, end_time):
        order_data = self.instance.get_trades(symbol, start_time, end_time)
        df = []
        for x in order_data:
            df.append([x.time, x.index, x.bsc.decode(), x.price, x.volume, x.ask, x.bid])
        df = pd.DataFrame(df, columns=['time', 'index', 'bsc', 'price', 'volume', 'ask', 'bid'])
        df['date'] = df.time.apply(lambda x: int(str(x)[:8]))
        df['code'] = symbol
        return df

    def get_min_df(self, symbol, start_time, end_time, fq, freq):

        min_data = self.instance.get_minute_price(symbol, start_time, end_time, fq, freq)
        df = [[x.time, x.open, x.close, x.volume, x.money] for x in min_data]
        df = pd.DataFrame(df, columns=['time', 'open', 'close', 'volume', 'money'])
        df['date'] = df.time.apply(lambda x: int(str(x)[:8]))
        df['code'] = symbol
        return df

def get_liquid(day,tick_sub,trade_sub,min_sub):

    tick_sub.time = tick_sub.time.apply(lambda x: x % 1000000000)
    tick_sub = tick_sub.loc[(tick_sub.time >= 93000000) & (tick_sub.time <= 145700000),]
    trade_sub.time = trade_sub.time.apply(lambda x: x % 1000000000)
    trade_sub = trade_sub.loc[(trade_sub.time >= 93000000) & (trade_sub.time <= 145700000),]
    min_sub.time = min_sub.time.apply(lambda x: x % 1000000000)
    min_sub = min_sub.loc[(min_sub.time >= 93000000) & (min_sub.time <= 145700000),]

    if len(tick_sub) == 0:
        return [day,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan]
    liquid = liquid_funcs()
    amihud = liquid.get_amihud(min_sub)
    q_spread = liquid.get_Qspread(tick_sub)
    e_spread = liquid.get_Espread(tick_sub)
    slope = liquid.get_slope(tick_sub)
    oib_num, oib_amount, oib_volume = liquid.get_oib(trade_sub)
    marginal_cost, orth_cost = liquid.get_liquid_cost(tick_sub, trade_sub)

    return [day, amihud, q_spread, e_spread, slope, oib_num, oib_amount, oib_volume, marginal_cost, orth_cost]

if __name__ == '__main__':

    start_d = '2016-01-01'
    end_d = '2017-01-05'

    start_t = ' 09:30:00'
    end_t = ' 14:57:00'

    min_data_path='E:/JT/SDK_test/stock_minute_data/'
    PROCESS = 6
    # symbol='000001.XSHE'

    ebq = ebq_data()
    # day_ls = ebq_sample.trade_days(start_d, end_d)
    stock_ls = [x.decode() for x in ebq.instance.get_all_securities(Stock)]

    print('start at: ', time.asctime())
    # pool = Pool(PROCESS)
    # para_ls = pool.starmap(get_liquid, itertools.product(day_ls[:1], stock_ls[:20]))
    # pool.close()
    # pool.join()
    # para_df = pd.DataFrame(para_ls,
    #                        columns=['date', 'code', 'amihud', 'q_spread', 'e_spread', 'slope','oib_num', 'oib_amount',
    #                                 'oib_volume', 'marginal_cost', 'orth_cost'])
    #
    # para_df.to_csv('processData/liquidity_paras.csv')

    for symbol in stock_ls[:2]:

        tick_df = ebq.get_lv2_data(symbol, start_d + start_t, end_d + end_t)
        tick_df['date'] = tick_df.time.apply(lambda x: str(x)[:8])
        trade_df = ebq.get_trade_df(symbol, start_d + start_t, end_d + end_t)
        trade_df['date'] = trade_df.time.apply(lambda x: str(x)[:8])
        min_df = pd.read_csv(min_data_path + symbol + '.csv', index_col=0)

        print('reading at: ', time.asctime())

        min_df['date'] = min_df.Date.apply(lambda x: x[:10].replace('-', ''))
        min_df.columns=['code','time','open','high','low','close','volume','amount','date']
        min_df.time=min_df.time.apply(lambda x: int(x.replace('-','').replace(':','').replace(' ','')+'000'))
        day_ls = list(set(tick_df.date))
        para_ls=[]

        # for day in day_ls:
        #     para_ls.append(get_liquid(day))
        #     break

        #pool = Pool(PROCESS)
        for day in day_ls[:20]:
            tick_sub = tick_df.loc[tick_df.date == day,]
            trade_sub = trade_df.loc[trade_df.date == day,]
            min_sub = min_df.loc[min_df.date == day,]
            #r = pool.apply_async(get_liquid, (day, tick_sub, trade_sub, min_sub)).get()
            r=get_liquid(day, tick_sub, trade_sub, min_sub)
            para_ls.append(r)
            break
        # pool.close()
        # pool.join()

        para_df=pd.DataFrame(para_ls,columns=['date', 'amihud', 'q_spread', 'e_spread', 'slope','oib_num', 'oib_amount',
                                'oib_volume', 'marginal_cost', 'orth_cost'])
        para_df['code']=symbol
        para_df.to_csv('liquid_by_code/%s.csv'%symbol)

        print('end at: ', time.asctime())
        break

