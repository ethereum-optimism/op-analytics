#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests as r
import pandas as pd

import sys
sys.path.append("../helper_functions")
import duneapi_utils as d
import metabaseapi_utils as mb
sys.path.pop()

import time
import os
import dotenv

dotenv.load_dotenv()
mb_name = os.environ["MS_METABASE_NAME"]
mb_pw = os.environ["MS_METABASE_PW"]

mb_url_base = "https://dash.goldsky.com"

# https://goldsky.notion.site/SHARED-Lightweight-API-Documentation-for-Goldsky-Dashboarding-5cde15ba222844f485c31a4426f6ed53


# In[ ]:


chain_mappings = {
        'zora': 'Zora',
        'pgn': 'Public Goods Network',
        'base': 'Base'
        # Add more mappings as needed
    }


# In[ ]:


session_id = mb.get_session_id(mb_url_base, mb_name, mb_pw)


# In[ ]:


# Run Query

query_nums = [
        # ## Chain, Daily-Level
        #  [21,'opchain_activity_by_day_gs'
        #         ,'Basic Daily Activity for OP Chains - Zora & PGN (from Goldsky)']     #https://dash.goldsky.com/question/21-op-chains-activity-by-day
         [544, 'base_chain_activity_by_day_gs'
                ,'Basic Daily Activity for OP Chains - Base (from Goldsky)']    #https://dash.goldsky.com/question/544-base-activity-by-day
        ,[545, 'zora_chain_activity_by_day_gs'
                ,'Basic Daily Activity for OP Chains - Zora (from Goldsky)']    #https://dash.goldsky.com/question/545-zora-activity-by-day
        ,[244, 'mode_chain_activity_by_day_gs'
                ,'Basic Daily Activity for OP Chains - Mode (from Goldsky)']    #https://dash.goldsky.com/question/244-mode-activity-by-day
        ,[379, 'pgn_chain_activity_by_day_gs'
                ,'Basic Daily Activity for OP Chains - PGN (from Goldsky)']    #https://dash.goldsky.com/question/379-lyra-activity-by-day
        ,[129, 'lyra_chain_activity_by_day_gs'
                ,'Basic Daily Activity for OP Chains - Lyra (from Goldsky)']    #https://dash.goldsky.com/question/129-lyra-activity-by-day

        ### Other Stuff
        ,[86,'opchain_pgn_alltime_contracts_created'
                ,'PGN All-Time Contracts Created(from Goldsky)']     #https://dash.goldsky.com/question/86-pgn-num-alltime-contracts-created
        ,[107,'pgn_usage_by_contract_by_month'
                ,'Monthly Contract Usage Data for PGN (from Goldsky)']     #https://dash.goldsky.com/question/107-pgn-usage-by-contract-by-month
        ,[106,'pgn_usage_by_contract_7_vs_30'
                ,'Contract Usage Data for PGN - Last 7 days vs prior 30 (from Goldsky)']     #https://dash.goldsky.com/question/106-pgn-usage-by-contract-7-vs-30
        # ,[118,'pgn_usage_by_contract_by_day_gt_1_tx'
        #         ,'Daily Contract Usage Data for PGN - > 1 Tx per Day (from Goldsky)']     #https://dash.goldsky.com/question/118-pgn-usage-by-contract-by-day-t365d-gt-1-tx
        ,[247,'mode_usage_by_contract_by_month'
                ,'Monthly Contract Usage Data for Mode (from Goldsky)']     #https://dash.goldsky.com/question/247-mode-usage-by-contract-by-month
        ,[248,'mode_usage_by_contract_7_vs_30'
                ,'Contract Usage Data for Mode - Last 7 days vs prior 30 (from Goldsky)']     #https://dash.goldsky.com/question/248-mode-usage-by-contract-7-vs-30
        ,[245,'mode_usage_by_contract_by_day_gt_1_tx'
                ,'Daily Contract Usage Data for Mode - > 1 Tx per Day (from Goldsky)']     #https://dash.goldsky.com/question/245-mode-usage-by-contract-by-day-t365d-gt-1-tx
        
        # ,[78,'opchain_fee_gen_contracts_dev_creators_t365d_gs'
        #         ,'Basic Fee Generating Contracts & Developer Methodology (T365D) for PGN (from Goldsky)']     #https://dash.goldsky.com/question/78-daily-used-contracts-with-traces-logs
        # ,[20,'opchain_active_dev_creators_gs'
        #         ,'Basic Active Developer Methodology for Zora & PGN (from Goldsky)']     #https://dash.goldsky.com/question/20-get-kr1-active-developers
        # ,[35,'opchain_used_contracts_creators_gs'
        #         ,'Basic Used Contracts Methodology for Zora & PGN (from Goldsky)']     #https://dash.goldsky.com/question/35-kr2-intermediate-get-daily-is-used-contracts
        
]


# In[ ]:


for q in query_nums:
        try: #don't break if one query fails
                query_num = q[0]
                table_name = q[1]
                table_description = q[2]

                print(str(query_num) + ' : ' + table_name)
                
                df = mb.query_response_to_df(session_id, mb_url_base, query_num)

                # Re-Format MB Dates if necessary
                if 'dt' in df.columns:
                        # df['dt'] = pd.to_datetime(df['dt'], format='%B %d, %Y, %H:%M')
                        df['dt'] = pd.to_datetime(df['dt'])

                # display(df)

                # Write to csv
                df.to_csv('outputs/chain_data/' + q[1] + '.csv', index=False)
                
                # Write to Dune
                df['chain_raw'] = df['chain']
                df['chain'] = df['chain'].replace(chain_mappings)
                d.write_dune_api_from_pandas(df, table_name,table_description)

                df = None #Free memory
                
                time.sleep(3)

        except:
                continue


# In[ ]:


print('done mb')

