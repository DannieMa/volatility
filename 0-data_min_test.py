
import pandas as pd
import numpy as np
if __name__=='__main__':
    freq_ls=[1,5,10,30,60]
    for freq in freq_ls:
        flag=pd.read_csv('data_min_index/close_min%d.csv'%freq,index_col=0)
        flag=flag[['date','time','000016.XSHG']]
        flag=flag.set_index(['date','time'])
        flag[flag==0]=np.nan
        flag[flag>=0]=1
        flag=flag.replace({1:np.nan,np.nan:1})
        flag=flag.dropna(how='all')
        flag=flag.reset_index()
        break