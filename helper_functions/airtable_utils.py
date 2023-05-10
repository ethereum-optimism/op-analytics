from airtable import airtable
import pandas as pd
import numpy as np

import dotenv
import os
dotenv.load_dotenv()

at_api_key = os.environ["AIRTABLE_API_TOKEN"]

# Read an airtable database in to a pandas dataframe
def get_dataframe_from_airtable_database(at_base_id, base_name):
        at = airtable.Airtable(at_base_id, at_api_key)
        data = at.get(base_name)
        df = pd.json_normalize(data, record_path='records')
        # Rename all columns that start with 'fields.'
        df.rename(columns=lambda x: x.replace('fields.', ''), inplace=True)

        return df

def is_get_successful(at, table_name):
    try:
        at.get(table_name)
        return True
    except airtable.AirtableError:
        return False

def upsert_record_dt_contract_creator(at, table_name, record):
    # Search for a matching record
	# Build the formula to filter the records
	formula = "AND(LEFT(dt,10)='{dt}', contract_address='{contract_address}', creator_address='{creator_address}')".format(
		dt=record['fields']['dt'][:10], 
		contract_address=record['fields']['contract_address'], 
		creator_address=record['fields']['creator_address']
		)
	for existing_record in at.iterate(table_name,
				   filter_by_formula = formula
				   ):
		# print(existing_record)
		if (existing_record['fields']['dt'][:10] == record['fields']['dt'][:10] and
			existing_record['fields']['contract_address'] == record['fields']['contract_address'] and
			existing_record['fields']['creator_address'] == record['fields']['creator_address']):
			# Update the matching record
			print('update existing row')
			at.update(table_name, existing_record['id'], record['fields'])
			return
    # If no matching record was found, create a new one
	print('add new record')
	# at.create(table_name, record['fields'])


def update_database(at_base_id, table_name, df):
    # Create an instance of the Airtable class
	at = airtable.Airtable(at_base_id, at_api_key)

	df.replace(np.nan, None, inplace=True)

    # Check if the base already exists
	if is_get_successful(at, table_name):
		print(f"The base '{at_base_id}' already exists, appending new records...")

    # Otherwise, create the base
	else:
		print(f"The base '{at_base_id}' doesn't exist, create it in the Airtable UI.")
		return

	# Replace NaN with None in the dataframe
    # Iterate through the DataFrame rows and upsert each record to Airtable
	for _, row in df.iterrows():

		# Convert the row to an Airtable record
		record = {'fields': row.to_dict()}
		# print(record)
		# Upsert the record to Airtable
		upsert_record_dt_contract_creator(at, table_name, record)



