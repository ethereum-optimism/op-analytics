#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import requests as r
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta, date
import numpy as np
import time
import os
import asyncio, aiohttp, nest_asyncio
from aiohttp_retry import RetryClient, ExponentialRetry
import defillama_utils as dfl
nest_asyncio.apply()
header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:71.0) Gecko/20100101 Firefox/71.0'}


# In[ ]:


#https://stackoverflow.com/questions/23267409/how-to-implement-retry-mechanism-into-python-requests-library

import logging
import requests

from requests.adapters import HTTPAdapter, Retry

# logging.basicConfig(level=logging.DEBUG)

s = requests.Session()
retries = Retry(total=10, backoff_factor=1, status_forcelist=[ 502, 503, 504 ])
s.mount('http://', HTTPAdapter(max_retries=retries))

pwd = os.getcwd()
if 'L2 TVL' in pwd:
    prepend = ''
else:
    prepend = 'L2 TVL/'


# In[ ]:


# date ranges to build charts for
drange = [0, 1, 7, 30, 90, 180, 365]
# Do we count net flows marked at the lastest token price (1) or the price on each day (0)
# By default, we opt to 1, so that price movement isn't accidentally counted as + or - flow remainder
mark_at_latest_price = 1 #some errors with missing token prices we need to fix first (i.e. rage trade on arbi marks usdc as 0)

trailing_num_days = max(drange)
# print(trailing_num_days)

start_date = date.today()-timedelta(days=trailing_num_days +1)
print(start_date)

# start_date = datetime.strptime('2022-07-13', '%Y-%m-%d').date()



# In[ ]:


#get all apps > 5 m tvl
min_tvl = 5_000_000

# if TVL by token is not available, do we fallback on raw TVL (sensitive to token prices)?
is_fallback_on_raw_tvl = True#False

df_df = dfl.get_all_protocol_tvls_by_chain_and_token(min_tvl, is_fallback_on_raw_tvl)


# In[ ]:


# display(df_df)
df_df_all = df_df.copy()
df_df_all.head()
# df_df_all[(df_df_all['protocol'] == 'magpie') & (df_df_all['date'] == '2023-01-27')]


# In[ ]:


# display(df_df_all)
df_df_all2 = df_df_all.copy()
# df_df_all2['token_value'] = df_df_all2['token_value'].fillna(0)
df_df_all2['token_value'] = df_df_all2['token_value'].astype('float64')
df_df_all2['usd_value'] = df_df_all2['usd_value'].astype('float64')
# display(df_df_all2)


# In[ ]:


#create an extra day to handle for tokens dropping to 0

print(df_df_all2.dtypes)

df_df_all_u = df_df_all2.fillna(0)
df_df_shift = df_df_all_u.copy()
df_df_shift['date'] = df_df_shift['date'] + timedelta(days=1)
df_df_shift['token_value'] = 0.0
df_df_shift['usd_value'] = 0.0

#merge back in
df_df_all = pd.concat([df_df_all_u,df_df_shift])

print(df_df_all.dtypes)

# display(df_df_all)
df_df_all = df_df_all[df_df_all['date'] <= pd.to_datetime("today") ]

df_df_all['token_value'] = df_df_all['token_value'].fillna(0)
df_df_all = df_df_all.groupby(['date','token','chain','protocol','name','category','parent_protocol']).sum(['usd_value','token_value'])

# display(df_df_all)
df_df_all = df_df_all.reset_index()
df_df_shift = []
# display(df_df_all)


# In[ ]:


# df_df_all = pd.concat(df_df_all)
# print(df_df_all[2])
print("done api")
# display(df_df_all[df_df_all['protocol'] == 'velodrome'])
# display(df_df_all)


# In[ ]:


#filter down a bit so we can do trailing comp w/o doing every row
df_df = df_df_all[df_df_all['date'].dt.date >= start_date-timedelta(days=1) ]

#trailing comp
df_df['last_token_value'] = df_df.groupby(['token','protocol','chain'])['token_value'].shift(1)
#now actually filter
df_df = df_df[df_df['date'].dt.date >= start_date ]
# display(df_df[df_df['protocol'] == 'velodrome'])


# In[ ]:


# display(df_df[(df_df['chain'] == 'Arbitrum') & (df_df['protocol'] == 'rage-trade') & (df_df['date'] > '2022-12-01')])


# In[ ]:


# display(df_df)
# sample = df_df[(df_df['protocol'] == 'uniswap') & (df_df['chain'] == 'Optimism')]
# sample = sample.sort_values(by='date',ascending=False)
# # display(sample)
# sample.to_csv('check_uni_error.csv')


# In[ ]:


data_df = df_df.copy()
data_df = data_df.sort_values(by='date')
# data_df['token_value'] = data_df['token_value'].replace(0, np.nan) #keep zeroes
# price = usd value / num tokens
data_df['price_usd'] = data_df['usd_value']/data_df['token_value']
data_df['last_price_usd'] = data_df.groupby(['token','protocol', 'chain'])['price_usd'].shift(1)

# If first instnace of token, make sure there's no price diff
data_df['last_price_usd'] = data_df[['last_price_usd', 'price_usd']].bfill(axis=1).iloc[:, 0]
#Forward fill if token drops off
data_df['price_usd'] = data_df[['price_usd','last_price_usd']].bfill(axis=1).iloc[:, 0]

# data_df.sample(20)
# display(data_df[data_df['protocol'] == 'velodrome'])


# In[ ]:


data_df['token_rank_desc'] = data_df.groupby(['chain','token'])['date'].\
                            rank(method='dense',ascending=False).astype(int)
data_df['token_rank_desc_prot'] = data_df.groupby(['chain','token','protocol'])['date'].\
                            rank(method='dense',ascending=False).astype(int)

data_df['token_rank_desc_prot_gt0'] = data_df.query('token_value > 0')\
                                    .groupby(['chain', 'token', 'protocol'])['date']\
                                    .rank(method='first', ascending=False)

# get latest price either by protocol or in aggregate
# if we don't have a match by protocol, then select in aggregate.

latest_prices_df_raw_prot = data_df[~data_df['price_usd'].isna()][['token','chain','protocol','price_usd']][data_df['token_rank_desc_prot'] ==1]
latest_prices_df_raw = data_df[~data_df['price_usd'].isna()][['token','chain','price_usd']][data_df['token_rank_desc'] ==1]
latest_prices_df_raw_prot_gt0 = data_df[~data_df['price_usd'].isna()][['token','chain','price_usd','protocol']][data_df['token_rank_desc_prot_gt0'] ==1]

latest_prices_df_prot = latest_prices_df_raw_prot.groupby(['token','chain','protocol']).median('price_usd')
latest_prices_df_prot = latest_prices_df_prot.rename(columns={'price_usd':'latest_price_usd_prot'})

latest_prices_df = latest_prices_df_raw.groupby(['token','chain']).median('price_usd')
latest_prices_df = latest_prices_df.rename(columns={'price_usd':'latest_price_usd_raw'})

latest_prices_df_prot_gt0 = latest_prices_df_raw_prot_gt0.groupby(['token','chain','protocol']).median('price_usd')
latest_prices_df_prot_gt0 = latest_prices_df_prot_gt0.rename(columns={'price_usd':'latest_price_usd_prot_gt0'})

latest_prices_df_prot = latest_prices_df_prot.reset_index()
latest_prices_df = latest_prices_df.reset_index()
latest_prices_df_prot_gt0 = latest_prices_df_prot_gt0.reset_index()

prices_df = data_df[['chain','protocol','token']].drop_duplicates()
prices_df = prices_df.merge(latest_prices_df_prot,on=['token','chain','protocol'], how='left')
prices_df = prices_df.merge(latest_prices_df,on=['token','chain'], how='left')
prices_df = prices_df.merge(latest_prices_df_prot_gt0,on=['token','chain','protocol'], how='left')
prices_df['latest_price_usd'] = \
        prices_df['latest_price_usd_prot'].where(prices_df['latest_price_usd_prot'] > 0, \
        prices_df['latest_price_usd_raw'].where(prices_df['latest_price_usd_raw'] > 0, \
        prices_df['latest_price_usd_prot_gt0']))
    # prices_df.loc[prices_df[['latest_price_usd_prot', 'latest_price_usd_raw', 'latest_price_usd_prot_gt0']].first_valid_index()]
# prices_df['latest_price_usd'] = prices_df['latest_price_usd_prot'].combine_first(prices_df['latest_price_usd_raw'])

prices_df = prices_df[['chain','protocol','token','latest_price_usd']]#,'latest_price_usd_prot','latest_price_usd_raw','latest_price_usd_prot_gt0']]

# display(prices_df)
prices_df = prices_df[~prices_df['latest_price_usd'].isna()]


data_df = data_df.merge(prices_df,on=['token','chain','protocol'], how='left')


# In[ ]:


# data_df[(data_df['protocol'] == 'concentrator') & (data_df['token'] == 'FXS') ]
# prices_df[(prices_df['protocol'] == 'concentrator') & (prices_df['token'] == 'FXS')]


# In[ ]:


# latest_prices_df_prot_gt0[latest_prices_df_prot_gt0['token'] == 'FXS']


# In[ ]:


data_df.sort_values(by='date',inplace=True)

# net token change
data_df['net_token_flow'] = data_df['token_value'] - data_df['last_token_value']
# net token change * current price
data_df['net_dollar_flow'] = data_df['net_token_flow'] * data_df['price_usd']
# net token change * latest price
data_df['net_dollar_flow_latest_price'] = data_df['net_token_flow'] * data_df['latest_price_usd']


data_df = data_df[abs(data_df['net_dollar_flow']) < 50_000_000_000] #50 bil error bar for bad prices
data_df = data_df[~data_df['net_dollar_flow'].isna()]


# In[ ]:


# Handle for errors where a token price went to zero (i.e. magpie ANKRBNB 2023-01-27)
data_df['net_dollar_flow_latest_price'] = np.where(
    data_df['net_dollar_flow'] == 0, 0, data_df['net_dollar_flow_latest_price']
)
# data_df.to_csv('csv_outputs/latest_tvl_app_trends_by_token.csv')  #csv too large, only run this manually
# data_df[(data_df['protocol'] == 'magpie') & (data_df['date'] >= '2023-01-27') ]
# data_df[data_df['protocol'] == 'velodrome']


# In[ ]:


netdf_df = data_df[['date','protocol','chain','name','category','parent_protocol','net_dollar_flow','usd_value','net_dollar_flow_latest_price']]
netdf_df = netdf_df.fillna(0)
netdf_df = netdf_df.sort_values(by='date',ascending=True)
netdf_df = netdf_df.groupby(['date','protocol','chain','name','category','parent_protocol']).sum(['net_dollar_flow','usd_value','net_dollar_flow_latest_price']) ##agg by app

#usd_value is the TVL on a given day
netdf_df = netdf_df.groupby(['date','protocol','chain','usd_value','name','category','parent_protocol']).sum(['net_dollar_flow','net_dollar_flow_latest_price'])
netdf_df.reset_index(inplace=True)

# netdf_df['cumul_net_dollar_flow'] = netdf_df[['protocol','chain','net_dollar_flow']]\
#                                     .groupby(['protocol','chain']).cumsum()
# netdf_df['cumul_net_dollar_flow_7d'] = netdf_df[['protocol','chain','net_dollar_flow']]\
#                                     .groupby(['protocol','chain'])['net_dollar_flow'].rolling(7, min_periods=1).sum()\
#                                     .reset_index(drop=True)
# netdf_df['cumul_net_dollar_flow_30d'] = netdf_df[['protocol','chain','net_dollar_flow']]\
#                                     .groupby(['protocol','chain'])['net_dollar_flow'].rolling(30, min_periods=1).sum()\
#                                     .reset_index(drop=True)
netdf_df.reset_index(inplace=True)
netdf_df.drop(columns=['index'],inplace=True)
# display(netdf_df[netdf_df['protocol']=='makerdao'])



# In[ ]:


# tmp = netdf_df[(netdf_df['protocol']=='rage-trade') & (netdf_df['chain']=='Arbitrum') & (netdf_df['date'] > '2022-12-01')]
# tmp.to_csv('check.csv')
# netdf_df[(netdf_df['protocol']=='shibaswap') & (netdf_df['chain']=='Ethereum') & (netdf_df['date'] > '2022-12-01')]

# netdf_df[netdf_df['protocol'] == 'velodrome'].groupby('protocol').sum()


# In[ ]:


#get latest
netdf_df['rank_desc'] = netdf_df.groupby(['protocol', 'chain'])['date'].\
                            rank(method='dense',ascending=False).astype(int)
# display(netdf_df[netdf_df['protocol'] == 'lyra'])
netdf_df = netdf_df[  #( netdf_df['rank_desc'] == 1 ) &\
                        (~netdf_df['chain'].str.contains('-borrowed')) &\
                        (~netdf_df['chain'].str.contains('-staking')) &\
                        (~netdf_df['chain'].str.contains('-pool2')) &\
                            (~netdf_df['chain'].str.contains('-treasury')) &\
                        (~( netdf_df['chain'] == 'treasury') ) &\
                        (~( netdf_df['chain'] == 'borrowed') ) &\
                        (~( netdf_df['chain'] == 'staking') ) &\
                            (~( netdf_df['chain'] == 'treasury') ) &\
                        (~( netdf_df['chain'] == 'pool2') ) &\
                        (~( netdf_df['protocol'] == 'polygon-bridge-&-staking') )  &\
                            (~(netdf_df['protocol'].str[-4:] == '-cex') )
#                         & (~( netdf_df['chain'] == 'Ethereum') )
                        ]
# display(netdf_df[netdf_df['protocol']=='makerdao'])


# In[ ]:


# display(netdf_df)


# In[ ]:


summary_df = netdf_df.copy()

summary_df = summary_df.sort_values(by='date',ascending=True)

# Mark at latest price if chosen
if mark_at_latest_price == 1:
        summary_df['mark_at_latest_price'] = mark_at_latest_price
        summary_df['net_dollar_flow'] = summary_df['net_dollar_flow_latest_price']
        titleval_append = ' - At Latest Prices'
else:
        titleval_append = ''

# Cast 'net_dollar_flow' to float64 data type
summary_df['net_dollar_flow'] = summary_df['net_dollar_flow'].astype('float64')

# summary_df = summary_df[(summary_df['chain'] == 'Solana') & (summary_df['protocol'] == 'uxd')]
for i in drange:
        if i == 0:
                summary_df['cumul_net_dollar_flow'] = summary_df[['protocol','chain','net_dollar_flow']]\
                                    .groupby(['protocol','chain']).cumsum()
                summary_df['flow_direction'] = np.where(summary_df['cumul_net_dollar_flow']*1.0 >= 0, 1,-1)
                summary_df['abs_cumul_net_dollar_flow'] = abs(summary_df['cumul_net_dollar_flow'])
                #latest price
                
                # display(summary_df)
        else:
                col_str = 'cumul_net_dollar_flow_' + str(i) + 'd'
                tvl_str = 'daily_avg_tvl_' + str(i) + 'd'
                # print(col_str)
                # summary_df[col_str] = summary_df[['protocol','chain','net_dollar_flow']]\
                #                     .groupby(['protocol','chain'])['net_dollar_flow'].transform(lambda x: x.rolling(i, min_periods=1).sum() )
                #chatgpt version
                summary_df[col_str] = summary_df.groupby(['protocol','chain'])['net_dollar_flow']\
                                        .apply(lambda x: x.rolling(i, min_periods=1).sum())

                summary_df[tvl_str] = summary_df[['protocol','chain','usd_value']]\
                                    .groupby(['protocol','chain'])['usd_value'].transform(lambda x: x.rolling(i, min_periods=1).mean() )
                
                summary_df['flow_direction_' + str(i) + 'd'] = np.where(summary_df[col_str]*1.0 >= 0, 1, -1)
                summary_df['abs_cumul_net_dollar_flow_' + str(i) + 'd'] = abs(summary_df[col_str])
                # display(summary_df)
                # display(summary_df[(summary_df['chain'] == 'Optimism') & (summary_df['protocol'] == 'yearn-finance')] )
# display(summary_df[netdf_df['protocol']=='makerdao'])
# display(summary_df[(summary_df['chain'] == 'Optimism') & (summary_df['protocol'] == 'qidao')].iloc[-7: , :15] )
# display(summary_df[(summary_df['protocol']=='stargate') & (summary_df['chain']=='Optimism')])
summary_df['pct_of_tvl'] = 100* summary_df['net_dollar_flow'] / summary_df['usd_value']
final_summary_df = summary_df[(summary_df['rank_desc'] == 1) & (summary_df['date'] >= pd.to_datetime("today") -timedelta(days=7))]
final_summary_df = final_summary_df[final_summary_df['cumul_net_dollar_flow']< 1e20] #weird error handling


os.makedirs('csv_outputs', exist_ok=True)

final_summary_df.to_csv(prepend + 'csv_outputs/latest_tvl_app_trends.csv', mode='w', index=False, encoding='utf-8')


# In[ ]:


# display(summary_df)
for i in drange:
        fig = ''
        if i == 0:
                yval = 'abs_cumul_net_dollar_flow'
                hval = 'cumul_net_dollar_flow'
                cval = 'flow_direction'
                saveval = 'net_app_flows'
                saveval_app = 'net_app_flows_by_app'
                titleval = "App Net Flows Change by Chain -> App - Last " + str(trailing_num_days) + \
                            " Days - (Apps with > $" + str(min_tvl/1e6) + "M TVL Shown)" + titleval_append
                titleval_app = "App Net Flows Change by App -> Chain - Last " + str(trailing_num_days) + \
                            " Days - (Apps with > $" + str(min_tvl/1e6) + "M TVL Shown)" + titleval_append
        else:
                yval = 'abs_cumul_net_dollar_flow_' + str(i) +'d'
                hval = 'cumul_net_dollar_flow_' + str(i) +'d'
                cval = 'flow_direction_' + str(i) +'d'
                saveval = 'net_app_flows_' + str(i) +'d'
                saveval_app = 'net_app_flows_by_app_' + str(i) +'d'
                titleval = "App Net Flows Change by Chain -> App - Last " + str(i) + \
                            " Days - (Apps with > $" + str(min_tvl/1e6) + "M TVL Shown)" + titleval_append
                titleval_app = "App Net Flows Change by App -> Chain - Last " + str(i) + \
                                " Days - (Apps with > $" + str(min_tvl/1e6) + "M TVL Shown)" + titleval_append
        if is_fallback_on_raw_tvl:
                subtitle = "<br><sup>*For apps where DefiLlama didn't have flows by token, we use their total change in TVL (including token price change)</sup>"
        else:
                subtitle = ""
        # print(yval)
        # print(cval)
        # print(titleval)
        # print(hval)
        fig = px.treemap(final_summary_df[final_summary_df[yval] !=0], \
                 path=[px.Constant("all"), 'chain', 'protocol'], \
#                  path=[px.Constant("all"), 'token', 'chain', 'protocol'], \
                 values=yval, color=cval
#                 ,color_discrete_map={'-1':'red', '1':'green'})
                ,color_continuous_scale='Spectral'
                     , title = titleval + subtitle
                
                , hover_data = [hval]
                )
        
        fig_app = px.treemap(final_summary_df[final_summary_df[yval] !=0], \
                 path=[px.Constant("all"), 'protocol','chain'], \
#                  path=[px.Constant("all"), 'token', 'chain', 'protocol'], \
                 values=yval, color=cval
#                 ,color_discrete_map={'-1':'red', '1':'green'})
                ,color_continuous_scale='Spectral'
                     , title = titleval_app + subtitle
                
                , hover_data = [hval]
                )
        
        fig.update_traces(root_color="lightgrey")
        fig.update_layout(margin = dict(t=50, l=25, r=25, b=25))
        fig_app.update_traces(root_color="lightgrey")
        fig_app.update_layout(margin = dict(t=50, l=25, r=25, b=25))

        fig.write_image(prepend + "img_outputs/svg/" + saveval + ".svg") #prepend + 
        fig.write_image(prepend + "img_outputs/png/" + saveval + ".png") #prepend + 
        fig.write_html(prepend + "img_outputs/" + saveval + ".html", include_plotlyjs='cdn')

        fig_app.write_image(prepend + "img_outputs/svg/" + saveval_app + ".svg") #prepend + 
        fig_app.write_image(prepend + "img_outputs/png/" + saveval_app + ".png") #prepend + 
        fig_app.write_html(prepend + "img_outputs/" + saveval_app + ".html", include_plotlyjs='cdn')

        if i == 30:
                fig.show()
# fig.data[0].textinfo = 'label+text+value'
# fig.update_layout(tickprefix = '$')


# In[ ]:


# display( summary_df[(summary_df['chain'] == 'Arbitrum') & (summary_df['protocol'] == 'rage-trade') & (summary_df['rank_desc'] < 30)][['date','usd_value','protocol','net_dollar_flow','cumul_net_dollar_flow_30d']])

#test sample
# summary_df[summary_df['protocol'] == 'magpie']
summary_df[summary_df['protocol'] == 'velodrome*']


# In[ ]:


# test_df= netdf_df[(netdf_df['chain'] == 'Arbitrum') & (netdf_df['protocol'] == 'rage-trade')][['chain','protocol','date','net_dollar_flow','rank_desc']]
# test_df = test_df.sort_values(by='date',ascending=True)
# test_df['test'] = test_df[['protocol','chain','net_dollar_flow']]\
#     .groupby(['protocol','chain'])['net_dollar_flow'].transform(lambda x: x.rolling(30, min_periods=1).sum() )
# display(test_df[test_df['rank_desc'] < 45])
# # display(summary_df[summary_df['protocol']=='makerdao'].iloc[: , :15])


# In[ ]:


# fig_app = px.treemap(final_summary_df[final_summary_df['abs_cumul_net_dollar_flow'] !=0], \
#                 #  path=[px.Constant("all"), 'chain', 'protocol'], \
# #                  path=[px.Constant("all"), 'token', 'chain', 'protocol'], \
#                         path=[px.Constant("all"), 'protocol','chain'], \
#                  values='abs_cumul_net_dollar_flow', color='flow_direction'
# #                 ,color_discrete_map={'-1':'red', '1':'green'})
#                 ,color_continuous_scale='Spectral'
#                 , title = "App Net Flows Change by Chain -> App - Last " + str(trailing_num_days) + \
#                             " Days - (Apps with > $" + str(min_tvl/1e6) + "M TVL Shown)"
#                 ,hover_data=['cumul_net_dollar_flow']
#                 )
# # fig.data[0].textinfo = 'label+text+value'
# fig_app.update_traces(root_color="lightgrey")
# fig_app.update_layout(margin = dict(t=50, l=25, r=25, b=25))
# fig.show()


# In[ ]:


# fig_app.write_image(prepend + "img_outputs/svg/net_app_flows_by_app.svg") #prepend + 
# fig_app.write_image(prepend + "img_outputs/png/net_app_flows_by_app.png") #prepend + 
# fig_app.write_html(prepend + "img_outputs/net_app_flows_by_app.html", include_plotlyjs='cdn')


# In[ ]:


# ! jupyter nbconvert --to python total_app_net_flows_async.ipynb

