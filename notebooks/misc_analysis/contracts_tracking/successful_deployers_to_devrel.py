#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd

import sys
sys.path.append("../helper_functions")
import duneapi_utils as d
import airtable_utils as a
import pandas_utils as p
sys.path.pop()

import dotenv
import os
dotenv.load_dotenv()

rerun_query = 1

at_base_id = os.environ["AIRTABLE_DEVREL_BASE_ID"]

excl_projects = [
    'Op',
    'Optimism Governor'
]


# In[ ]:


query_id = 2457627
name = "Contract Deployments Usage Threshold - Creator List"

if rerun_query == 1:
    deployers = pd.read_csv('csv_outputs/' + name + '.csv')
else:
    # Dune query: https://dune.com/queries/2457627
    deployers = d.get_dune_data(query_id, name = name)


# In[ ]:


deployers = deployers[~deployers['Team'].isin(excl_projects)]


# In[ ]:


#reformat col names
deployers = p.csv_columns_to_formatted(deployers)

datestr = p.get_datestring_from_datetime(deployers['Date'].iloc[0])
datestr = datestr.replace('-', '_')

p.mkdir_if_not_exists('uploads')
# deployers.to_csv('uploads/deployer_data_' + datestr + '.csv', index=False)


# In[ ]:


# Delete Existing Data
# print('deleting existing data')S
a.delete_all_records(at_base_id,'OP Deployer Data')
# Append New Data
print('appending new data')
# deployers = deployers[deployers['Team'] == 'Cozy Finance'] #Test specific team
a.update_database(at_base_id,'OP Deployer Data', deployers)
print('done')

