'''
计算流动性指标
'''

import pandas as pd
import numpy as np
import statsmodels.api as sm

import warnings

warnings.filterwarnings('ignore')


class liquid_funcs(object):

    def __init__(self):
        pass

    def get_amihud(self, df_10min):
        """
        :param df: 10min
        :return: amihud
        """
        df_10min = df_10min.loc[df_10min.volume > 0,]
        df_10min['ret'] = abs(df_10min.close / df_10min.open - 1)
        amihud = (df_10min.ret / df_10min.amount).mean() * 10000000000
        return amihud

    def get_Qspread(self, df_tick):
        """
        计算报价价差
        :param df: tick,level 2
        :return: q_spread
        """
        df_tick = df_tick.loc[(df_tick.volume > 0) & (df_tick.price > 0),]
        spread_ls = 2 * (df_tick.ap1 - df_tick.bp1) / (df_tick.ap1 + df_tick.bp1)
        weight_ls = df_tick.volume / df_tick.volume.sum()
        q_spread = (spread_ls * weight_ls).sum()
        return q_spread

    def get_Espread(self, df_tick):
        """
        计算有效价差
        :param df: tick, level2
        :return: e_srepad
        """
        df_tick = df_tick.loc[(df_tick.volume > 0) & (df_tick.price > 0)& (df_tick.ap1 > 0)& (df_tick.bp1 > 0),]
        spread_ls = 2 * abs(np.log(df_tick.price) - np.log(0.5 * df_tick.ap1 + 0.5 * df_tick.bp1))
        weight_ls = df_tick.volume / df_tick.volume.sum()
        e_srepad = (spread_ls * weight_ls).sum()
        return e_srepad

    # def get_slope_by_tick(self, v):
    #     df = pd.DataFrame({'av': [v.av1, v.av2, v.av3, v.av4, v.av5, v.av6, v.av7, v.av8, v.av9, v.av10],
    #                        'ap': [v.ap1, v.ap2, v.ap3, v.ap4, v.ap5, v.ap6, v.ap7, v.ap8, v.ap9, v.ap10],
    #                        'bv': [v.bv1, v.bv2, v.bv3, v.bv4, v.bv5, v.bv6, v.bv7, v.bv8, v.bv9, v.bv10],
    #                        'bp': [v.bp1, v.bp2, v.bp3, v.bp4, v.bp5, v.bp6, v.bp7, v.bp8, v.bp9, v.bp10]
    #                        })
    #     df = df.shift()
    #     m = (df.loc[1, 'ap'] + df.loc[1, 'bp']) / 2
    #     df.loc[0, :] = [df.loc[1, 'av'] / (df.loc[1, 'av'] + 1), m, df.loc[1, 'bv'] / (df.loc[1, 'bv'] + 1), m]
    #     sa = ((df.av / df.av.shift() - 1) / (df.ap / df.ap.shift() - 1)).mean()
    #     sb = ((df.bv / df.bv.shift() - 1) / (df.bp / df.bp.shift() - 1)).mean()
    #     slope_tick = 2 * (sa - sb) / (sa + sb)
    #     return slope_tick

    def get_slope(self, df_tick):
        """
        计算订单簿斜率
        :param df_tick: tick,level 2
        :return: slope
        """
        df_tick = df_tick.loc[(df_tick.volume > 0) & (df_tick.price > 0),]
        df_tick = df_tick.set_index('time')

        df=df_tick[['av10', 'av9', 'av8', 'av7', 'av6', 'av5', 'av4', 'av3', 'av2', 'av1',
                    'bv1', 'bv2', 'bv3', 'bv4', 'bv5', 'bv6', 'bv7', 'bv8', 'bv9', 'bv10',
                    'ap10', 'ap9', 'ap8', 'ap7', 'ap6', 'ap5', 'ap4', 'ap3', 'ap2', 'ap1',
                    'bp1', 'bp2', 'bp3', 'bp4', 'bp5', 'bp6', 'bp7', 'bp8', 'bp9', 'bp10']]
        df=df.unstack().reset_index()
        df.columns=['temp','time','v']
        df['temp_i']=[int(y[2:]) for y in df.temp]
        df['temp_v']=[x[:2] for x in df.temp]
        df=pd.pivot_table(df,index=['time','temp_i'],columns=['temp_v'],values='v')
        df=df.reset_index()

        df_add=df.loc[df.temp_i==1,]
        df_add.temp_i=0
        df_add.ap=(df_add.ap+df_add.bp)/2
        df_add.bp=(df_add.ap+df_add.bp)/2
        df_add.av=df_add.av / (df_add.av + 1)
        df_add.bv=df_add.bv / (df_add.bv + 1)

        df_add2=df.loc[df.temp_i==1,]
        df_add2.temp_i=-1
        df_add2.ap,df_add2.bp,df_add2.av, df_add2.bv=np.nan,np.nan,np.nan,np.nan

        df=df.append(df_add).append(df_add2)
        df=df.sort_values(['time','temp_i'])

        df['sa'] = (df.av / df.av.shift() - 1) / (df.ap / df.ap.shift() - 1)
        df['sb'] = (df.bv / df.bv.shift() - 1) / (df.bp / df.bp.shift() - 1)
        df=df.drop(df.loc[(df.temp_i==-1)|(df.temp_i==0)].index)

        df=df[['time','sa','sb']].groupby('time').mean()
        df['slope']=2 * (df.sa - df.sb) / (df.sa + df.sb)

        weight_ls = df_tick.volume / df_tick.volume.sum()
        slope = (df.slope * weight_ls).sum()

        return slope

    def get_oib(self, df_trade):
        """
        根据逐笔成交数据，计算订单流不平衡
        :param df_trade: trade
        :return:
            oib_num,买卖交易笔数不平衡

        """
        df_trade = df_trade.loc[(df_trade.price > 0) & (df_trade.volume > 0),]
        df_trade['amount'] = df_trade.price * df_trade.volume

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
        df_trade['min1'] = [x // 100 for x in df_trade.time]
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
        df_tick.price /= 100
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
        lambda_df['min1'] = [x // 100 for x in lambda_df.time]
        lambda_df = lambda_df[['min1', 'lambda_t']].groupby('min1').mean()
        lambda_df = lambda_df.reset_index()

        oib_df = self.get_oib_1min(df_trade)

        df1min = df_tick[['price', 'volume']]
        df1min['min1'] = [x // 100 for x in df1min.index]
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


if __name__ == '__main__':


    min10_df = pd.read_csv('processData/000100.XSHE.csv', index_col=0)
    min10_df.columns=['code','datetime','open','high','low','close','volume','amount']
    min10_df['date']=min10_df.datetime.apply(lambda x:int(x[:10].replace('-','')))
    min10_df['time']=min10_df.datetime.apply(lambda x:int(x[-8:-2].replace(':','')))
    min10_df=min10_df.loc[(min10_df.time>=930)&(min10_df.time<=1500)]
    min10_df=min10_df.loc[(min10_df.date>=20160101)&(min10_df.date<=20160131)]
    min10_df=min10_df.loc[min10_df.date==20160105,]

    tick_df = pd.read_csv('processData/000100.XSHE_sample_lv2.csv', index_col=0)
    tick_df['time']=tick_df.time%1000000000//1000
    tick_df=tick_df.loc[(tick_df.time>=93000)&(tick_df.time<=145700)]
    tick_df=tick_df.loc[tick_df.date==20160105,]

    trade_df = pd.read_csv('processData/000100.XSHE_sample.csv', index_col=0)
    trade_df['time']=trade_df.time%1000000000//1000
    trade_df=trade_df.loc[(trade_df.time>=93000)&(trade_df.time<=145700)]
    trade_df=trade_df.loc[trade_df.date==20160105,]

    liquid = liquid_funcs()
    amihud = liquid.get_amihud(min10_df)
    q_spread = liquid.get_Qspread(tick_df)
    e_spread = liquid.get_Espread(tick_df)
    slope = liquid.get_slope(tick_df)
    oib_num, oib_amount, oib_volume = liquid.get_oib(trade_df)
    marginal_cost, orth_cost = liquid.get_liquid_cost(tick_df, trade_df)



