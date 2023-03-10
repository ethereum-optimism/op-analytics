#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta, date
import numpy as np
import os
import sys
sys.path.append('../helper_functions')
import defillama_utils as dfl


# In[ ]:


# date ranges to build charts for
drange = [0, 1, 7, 30, 90, 180, 365]
# Do we count net flows marked at the lastest token price (1) or the price on each day (0)
# By default, we opt to 1, so that price movement isn't accidentally counted as + or - flow remainder
mark_at_latest_price = 1 #some errors with missing token prices we need to find solves for first

trailing_num_days = max(drange)
# print(trailing_num_days)

start_date = date.today()-timedelta(days=trailing_num_days +1)
print(start_date)


# In[ ]:


#get all apps > 5 m tvl
min_tvl = 5_000_000

# if TVL by token is not available, do we fallback on raw TVL (sensitive to token prices)?
is_fallback_on_raw_tvl = True#False

df_df = dfl.get_all_protocol_tvls_by_chain_and_token(min_tvl, is_fallback_on_raw_tvl)


# In[ ]:


df_df.head()

# Test for errors
# df_df_all[(df_df_all['protocol'] == 'app_name') & (df_df_all['date'] == '2023-01-27')]


# In[ ]:


# display(df_df_all)

# df_df_all2['token_value'] = df_df_all2['token_value'].fillna(0)
df_df['token_value'] = df_df['token_value'].astype('float64')
df_df['usd_value'] = df_df['usd_value'].astype('float64')
# display(df_df_all2)


# In[ ]:


#create an extra day to handle for tokens dropping to 0

df_df = df_df.fillna(0)
df_df_shift = df_df.copy()
df_df_shift['date'] = df_df_shift['date'] + timedelta(days=1)
df_df_shift['token_value'] = 0.0
df_df_shift['usd_value'] = 0.0

#merge back in
df_df = pd.concat([df_df,df_df_shift])

# print(df_df_all.dtypes)

df_df = df_df[df_df['date'] <= pd.to_datetime("today") ]

df_df['token_value'] = df_df['token_value'].fillna(0)
df_df_all = df_df.groupby(['date','token','chain','protocol','name','category','parent_protocol']).sum(['usd_value','token_value'])


df_df = df_df.reset_index()
df_df_shift = []


# In[ ]:


# df_df_all = pd.concat(df_df_all)
# print(df_df_all[2])
print("done api")
# display(df_df_all[df_df_all['protocol'] == 'velodrome'])
# display(df_df_all)


# In[ ]:


#filter down a bit so we can do trailing comparisons w/o doing every row
df_df = df_df[df_df['date'].dt.date >= start_date-timedelta(days=1) ]

#trailing comparison
df_df['last_token_value'] = df_df.groupby(['token','protocol','chain'])['token_value'].shift(1)
#now actually filter
df_df = df_df[df_df['date'].dt.date >= start_date ]
# display(df_df[df_df['protocol'] == 'velodrome'])


# In[ ]:


data_df = df_df.copy()
data_df = data_df.sort_values(by='date')

# price = usd value / num tokens
data_df['price_usd'] = data_df['usd_value']/data_df['token_value']
data_df['last_price_usd'] = data_df.groupby(['token','protocol', 'chain'])['price_usd'].shift(1)

# If first instnace of token, make sure there's no price diff
data_df['last_price_usd'] = data_df[['last_price_usd', 'price_usd']].bfill(axis=1).iloc[:, 0]
#Forward fill if token drops off
data_df['price_usd'] = data_df[['price_usd','last_price_usd']].bfill(axis=1).iloc[:, 0]

data_df.sample(10)


# In[ ]:


# Find what is the latest token price. This sometimes gets skewed if tokens disappear or supply locked goes to 0
# Token's recency rank by chain - For calculating prices
data_df['token_rank_desc'] = data_df.groupby(['chain','token'])['date'].\
                            rank(method='dense',ascending=False).astype(int)
# Token's recency rank by chain & app - For calculating prices
data_df['token_rank_desc_prot'] = data_df.groupby(['chain','token','protocol'])['date'].\
                            rank(method='dense',ascending=False).astype(int)
# Token's recency rank by chain & app if > 0 - For calculating prices
data_df['token_rank_desc_prot_gt0'] = data_df.query('token_value > 0')\
                                    .groupby(['chain', 'token', 'protocol'])['date']\
                                    .rank(method='first', ascending=False)

# get latest price either by protocol or in aggregate
# if we don't have a match by protocol, then select in aggregate.
# This section is messy
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
#Select the latest price we want in priority order
prices_df['latest_price_usd'] = \
        prices_df['latest_price_usd_prot'].where(prices_df['latest_price_usd_prot'] > 0, \
        prices_df['latest_price_usd_raw'].where(prices_df['latest_price_usd_raw'] > 0, \
        prices_df['latest_price_usd_prot_gt0']))

#Filter down
prices_df = prices_df[['chain','protocol','token','latest_price_usd']]

prices_df = prices_df[~prices_df['latest_price_usd'].isna()]

#Merge back in to the data dataframe
data_df = data_df.merge(prices_df,on=['token','chain','protocol'], how='left')


# In[ ]:


# Sort in date order
data_df.sort_values(by='date',inplace=True)

# get net token change
data_df['net_token_flow'] = data_df['token_value'] - data_df['last_token_value']
# get net token change * current price
data_df['net_dollar_flow'] = data_df['net_token_flow'] * data_df['price_usd']
# get net token change * latest price
data_df['net_dollar_flow_latest_price'] = data_df['net_token_flow'] * data_df['latest_price_usd']

#Filter out weird errors and things
data_df = data_df[abs(data_df['net_dollar_flow']) < 50_000_000_000] #50 bil error bar for bad prices
data_df = data_df[~data_df['net_dollar_flow'].isna()]


# In[ ]:


# Handle for errors where a token price went to zero (i.e. magpie ANKRBNB 2023-01-27)
data_df['net_dollar_flow_latest_price'] = np.where(
    data_df['net_dollar_flow'] == 0, 0, data_df['net_dollar_flow_latest_price']
)


# In[ ]:


# Get net flows by protocol

netdf_df = data_df[['date','protocol','chain','name','category','parent_protocol','net_dollar_flow','usd_value','net_dollar_flow_latest_price']]
netdf_df = netdf_df.fillna(0)
netdf_df = netdf_df.sort_values(by='date',ascending=True)
netdf_df = netdf_df.groupby(['date','protocol','chain','name','category','parent_protocol']).sum(['net_dollar_flow','usd_value','net_dollar_flow_latest_price']) ##agg by app

#usd_value is the TVL on a given day
netdf_df = netdf_df.groupby(['date','protocol','chain','usd_value','name','category','parent_protocol']).sum(['net_dollar_flow','net_dollar_flow_latest_price'])

netdf_df.reset_index(inplace=True)
netdf_df.head()

# Drop index column if it exists
try:
        netdf_df.drop(columns=['index'],inplace=True)
except:
        pass
# display(netdf_df[netdf_df['protocol']=='makerdao'])



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

for i in drange:
        if i == 0:
                summary_df['cumul_net_dollar_flow'] = summary_df[['protocol','chain','net_dollar_flow']]\
                                    .groupby(['protocol','chain']).cumsum()
                summary_df['flow_direction'] = np.where(summary_df['cumul_net_dollar_flow']*1.0 >= 0, 1,-1)
                summary_df['abs_cumul_net_dollar_flow'] = abs(summary_df['cumul_net_dollar_flow'])

        else:
                col_str = 'cumul_net_dollar_flow_' + str(i) + 'd'
                tvl_str = 'daily_avg_tvl_' + str(i) + 'd'
                
                #chatgpt version
                summary_df[col_str] = summary_df.groupby(['protocol','chain'])['net_dollar_flow']\
                                        .apply(lambda x: x.rolling(i, min_periods=1).sum())

                summary_df[tvl_str] = summary_df[['protocol','chain','usd_value']]\
                                    .groupby(['protocol','chain'])['usd_value'].transform(lambda x: x.rolling(i, min_periods=1).mean() )
                
                summary_df['flow_direction_' + str(i) + 'd'] = np.where(summary_df[col_str]*1.0 >= 0, 1, -1)
                summary_df['abs_cumul_net_dollar_flow_' + str(i) + 'd'] = abs(summary_df[col_str])

summary_df['pct_of_tvl'] = 100* summary_df['net_dollar_flow'] / summary_df['usd_value']
final_summary_df = summary_df[(summary_df['rank_desc'] == 1) & (summary_df['date'] >= pd.to_datetime("today") -timedelta(days=7))]
final_summary_df = final_summary_df[final_summary_df['cumul_net_dollar_flow']< 1e20] #weird error handling


os.makedirs('csv_outputs', exist_ok=True)
os.makedirs('img_outputs', exist_ok=True)
os.makedirs('img_outputs/png', exist_ok=True)
os.makedirs('img_outputs/svg', exist_ok=True)
os.makedirs('img_outputs/html', exist_ok=True)

final_summary_df.to_csv('csv_outputs/latest_tvl_app_trends.csv', mode='w', index=False, encoding='utf-8')


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

        fig.write_image("img_outputs/svg/" + saveval + ".svg") #
        fig.write_image("img_outputs/png/" + saveval + ".png") #
        fig.write_html("img_outputs/html/" + saveval + ".html", include_plotlyjs='cdn')

        fig_app.write_image("img_outputs/svg/" + saveval_app + ".svg") #
        fig_app.write_image("img_outputs/png/" + saveval_app + ".png") #
        fig_app.write_html("img_outputs/html/" + saveval_app + ".html", include_plotlyjs='cdn')

        if i == 30:
                fig.show()
# fig.data[0].textinfo = 'label+text+value'
# fig.update_layout(tickprefix = '$')


# In[ ]:


# ! jupyter nbconvert --to python total_app_net_flows_async.ipynb

