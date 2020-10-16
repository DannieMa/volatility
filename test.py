

import pandas as pd
import numpy as np

def my_qcut(x):
    if len(x.dropna()<15) or len(x.drop_duplicates())<1:
        x=np.nan
    else:
        x=pd.qcut(x,5,labels=False)
    return x

def func(factor_sub,ret_sub):

    ret_sub=ret_sub.unstack().reset_index()
    ret_sub.columns=['code','date','zdf']

    group_df=factor_sub.apply(lambda x: pd.qcut(x,5,labels=False) if len(x.dropna()>15) and len(x.drop_duplicates())>1 else np.nan).unstack().reset_index()

    group_df.columns=['code','date','group']
    ret_sub=ret_sub.merge(group_df,on=['code','date'])
    ret_grouped=ret_sub[['code','group','zdf']].groupby(['code','group']).mean().reset_index()
    ret_grouped=ret_grouped.pivot(index='code',columns='group',values='zdf')
    ret_grouped['long-short']=ret_grouped[0]-ret_grouped[4]

    result=ret_grouped[['long-short']].T
    result.index=[factor_sub.index[-1]]

    return result


df=pd.read_excel('data.xlsx')
factor_df=df.pivot(index='date',columns='code',values='traderatio')

factor_df=factor_df.unstack().reset_index()

ret_df=df.pivot(index='date',columns='code',values='zdf')

result_df=[]
for i in range(10,len(factor_df),1):
    factor_sub=factor_df.iloc[i-10:i]
    ret_sub=ret_df.iloc[i-10:i]
    r=func(factor_sub,ret_sub)
    result_df.append(r)

result_df=pd.concat(result_df)

