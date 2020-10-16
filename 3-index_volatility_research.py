# -*- coding: utf-8 -*-
"""
@Time    : 2020/8/12 9:20
@Author  : Jicong Hu
@FileName: index_volatility_research.py
@Software: PyCharm
"""

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import seaborn as sns

plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

# 将价格数据转化为年化的波动率数据
def annualized_hist_vol_series(price_frame, sample_period=1):
    """

    :param price_frame:
    :param sample_period:
    :return:
    """
    index_list = price_frame.columns.drop(['date', 'time'])
    return_frame = \
        pd.concat([price_frame[['date', 'time']], price_frame.groupby('date')[index_list].pct_change()], axis=1)
    annualized_vol = return_frame.groupby('date')[index_list].std() * 240 / np.sqrt(sample_period)
    valid_map = return_frame.groupby('date')[index_list].count() / (240 / sample_period * 0.9)
    valid_map[valid_map >= 1] = 1
    valid_map[valid_map < 1] = np.nan
    annualized_vol *= valid_map
    annualized_vol.index = pd.to_datetime([str(x) for x in annualized_vol.index])
    return annualized_vol


# 以分钟数据为频率计算每日分时波动特征
def intraday_vol_data(price_frame):
    """

    :param price_frame:
    :return:
    """
    # TODO:
    #   1. resample by hour freq
    #   2. calculate hist_vol in sample
    #   3. stats of hist_vol groupby hour
    index_list = price_frame.columns.drop(['date', 'time'])
    return_frame = \
        pd.concat([price_frame[['date', 'time']], price_frame.groupby('date')[index_list].pct_change()], axis=1)
    return_frame['group'] = np.searchsorted([1030, 1130, 1400], return_frame.time, 'left')
    sample_vol = return_frame.groupby(['date', 'group'])[index_list].std().reset_index()
    vol_data = sample_vol[index_list] * 240
    vol_validity = return_frame.groupby(['date', 'group'])[index_list].count().reset_index()[index_list]
    vol_validity = np.floor(vol_validity / 50)
    vol_validity[vol_validity == 0] = np.nan
    sample_vol = pd.concat([sample_vol[['date', 'group']], vol_data * vol_validity], axis=1)
    return sample_vol


def intraday_vol_stats(intraday_vol):
    index_list = intraday_vol.columns.drop(['date', 'group'])
    vol_mean = intraday_vol.groupby('group')[index_list].mean()
    vol_std = intraday_vol.groupby('group')[index_list].std()
    return vol_mean, vol_std


def intraday_vol_plot(intraday_vol):
    plt.figure()
    for group_num in intraday_vol.group.value_counts().index:
        group_data = intraday_vol[intraday_vol.group == group_num].drop(['date', 'group'], axis=1)
        ax = plt.subplot(2, 2, group_num+1)
        ax.set_title('日内第%d个小时的波动率' % (group_num+1))
        sns.boxplot(data=group_data)


index_list_map = {'000016.XSHG': '上证50', '000905.XSHG': '中证500', '000906.XSHG': '中证800',
                  '000852.XSHG': '中证1000', '399006.XSHE': '创业板指'}

close_data_min1 = pd.read_csv('close_min1.csv', index_col=0, parse_dates=True)
close_data_min5 = pd.read_csv('close_min5.csv', index_col=0, parse_dates=True)
close_data_min10 = pd.read_csv('close_min10.csv', index_col=0, parse_dates=True)
close_data_min30 = pd.read_csv('close_min30.csv', index_col=0, parse_dates=True)
close_data_min60 = pd.read_csv('close_min60.csv', index_col=0, parse_dates=True)

vol_min1 = annualized_hist_vol_series(close_data_min1, 1)
vol_min5 = annualized_hist_vol_series(close_data_min1, 5)
vol_min10 = annualized_hist_vol_series(close_data_min1, 10)
vol_min1_sheet = vol_min1.stack().reset_index().rename(columns={'level_0': 'date', 'level_1': 'index_code', 0: 'vol'})
vol_min1_sheet['cal_period'] = '01分钟计算'
vol_min5_sheet = vol_min5.stack().reset_index().rename(columns={'level_0': 'date', 'level_1': 'index_code', 0: 'vol'})
vol_min5_sheet['cal_period'] = '05分钟计算'
vol_min10_sheet = vol_min10.stack().reset_index().rename(columns={'level_0': 'date', 'level_1': 'index_code', 0: 'vol'})
vol_min10_sheet['cal_period'] = '10分钟计算'
vol_min_all = pd.concat([vol_min1_sheet, vol_min5_sheet, vol_min10_sheet])

for code in index_list_map:
    vol_frame = vol_min_all[vol_min_all.index_code == code].pivot('date', 'cal_period', 'vol')
    if code == '000852.XSHG':
        vol_frame = vol_frame.loc['2016-01-01':]
    vol_frame.plot(title='{}指数波动率'.format(index_list_map[code]))

vol_period_mean = \
    vol_min_all.groupby(['index_code', 'cal_period']).vol.mean().reset_index().pivot('cal_period', 'index_code', 'vol')
vol_period_mean = vol_period_mean.rename(columns=index_list_map)

intraday_vol = intraday_vol_data(close_data_min1)
intraday_vol = intraday_vol.rename(columns=index_list_map)
intraday_vol_mean, intraday_vol_std = intraday_vol_stats(intraday_vol)

writer = pd.ExcelWriter('intraday_vol_stats.xlsx')
intraday_vol_mean.to_excel(writer, 'mean')
intraday_vol_std.to_excel(writer, 'std')
writer.save()
