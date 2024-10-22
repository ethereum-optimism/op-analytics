from airtable import airtable
from collections import OrderedDict

from datetime import datetime
import json

import pandas as pd
import numpy as np

import dotenv
import os
dotenv.load_dotenv()

at_api_key = os.environ["AIRTABLE_API_TOKEN"]
# https://github.com/josephbestjames/airtable.py#get

# Read an airtable database in to a pandas dataframe
def get_dataframe_from_airtable_database(at_base_id, base_name, filter='1'):

	at = airtable.Airtable(at_base_id, at_api_key)
	data_iter = at.iterate(base_name, batch_size=100, filter_by_formula = '1=' + filter)
	# example with similar results of at.get
	data = { "records": [] }
	for r in data_iter:
		data["records"].append(r)

	df = pd.json_normalize(data, record_path='records')

	# Rename all columns that start with 'fields.'
	df.rename(columns=lambda x: x.replace('fields.', ''), inplace=True)
	# Convert timestamp columns to string representation
	timestamp_columns = ['Date']  # Add more columns if needed
	for col in timestamp_columns:
		if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
			df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')

	return df

def is_get_successful(at, table_name):
    try:
        at.get(table_name)
        return True
    except airtable.AirtableError:
        return False

def create_new_record_id(at, table_name, field_name, value, result, formula):
	to_append = ''
	print('create')
	record_field = {field_name:value}
	print(record_field)
	at.create(table_name, record_field)#, True)
	for r in at.iterate(table_name, filter_by_formula = formula):
		to_append = r['id']
		result.append(r['id'])
	return to_append

def get_linked_record_id(at, table_name, field_name, value):
	formula = "lower({@field_name@})=lower('@value@')"
	formula = formula.replace('@field_name@',field_name)
	formula = formula.replace('@value@',value)
	#If does not exist, create it
	try: 
		result = [] #{ "records": [] }
		for r in at.iterate(table_name, filter_by_formula = formula):
			result.append(r['id'])
		
	except:
		to_append = create_new_record_id(at, table_name, field_name, value, result, formula)
		result.append(to_append)

	if not result: #empty string
		to_append = create_new_record_id(at, table_name, field_name, value, result, formula)
		result.append(to_append)
	result = list(set(result)) #remove duplicates
	print(result)
	return result

def convert_timestamps_to_strings(data):
    converted_data = {}
    for key, value in data.items():
        if isinstance(value, datetime):
            converted_data[key] = value.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(value, dict):
            converted_data[key] = convert_timestamps_to_strings(value)
        else:
            converted_data[key] = value
    return converted_data

def replace_nans(record):
    record_fields = record['fields']
    for key, value in record_fields.items():
        if value == 'NaN': #value is None or 
            record_fields[key] = 0
    return record

def upsert_record_dt_contract_creator(at, table_name, record):
    # Search for a matching record
	# Build the formula to filter the records
	dt_date = record['fields']['Date'][:10]
	contract_address =record['fields']['Contract Address']
	creator_address = record['fields']['Creator Address']
	if creator_address is None:
		creator_address = ''

	linked_field_name = 'Team Name'

	# Generate Search Query
	formula = "AND(LEFT(Date,10)='@Date@', {Contract Address}='@contract_address@', {Creator Address}='@creator_address@')"
	formula = formula.replace('@Date@',dt_date)
	formula = formula.replace('@contract_address@',contract_address)
	formula = formula.replace('@creator_address@',creator_address)

	# Get Linked Team Name
	if (record['fields']['Team'] != '') and (record['fields']['Team'] is not None):
		linked_id = get_linked_record_id(at,'Teams',linked_field_name, record['fields']['Team'])
		record['fields'][linked_field_name] = linked_id

	#Handle for timestamps
	record = convert_timestamps_to_strings(record)
	#Handle for nulls
	record = replace_nans(record)
	
	# Check if we update
	for existing_record in at.iterate(table_name,
				   filter_by_formula = formula
				   ):
		# print(existing_record)
		if (existing_record['fields']['Date'][:10] == record['fields']['Date'][:10] and
			existing_record['fields']['Contract Address'] == record['fields']['Contract Address'] and
			existing_record['fields']['Creator Address'] == record['fields']['Creator Address']):
			# Update the matching record
			print('update existing row')

			print(record['fields'])
			at.update(table_name, existing_record['id'], record['fields'])

			return
	
    # If no matching record was found, create a new one
	print('add new record')
	# print(record['fields'])
	
	at.create(table_name, record['fields'])

def upsert_record_dt_team(at, table_name, record):
    # Search for a matching record
	# Build the formula to filter the records
	dt_date = record['fields']['Date'][:10]
	team =record['fields']['Team']
	# creator_address = record['fields']['Creator Address']
	if (team is None):
		team = ''

	linked_field_name = 'Team Name'

	# Generate Search Query
	formula = "AND(LEFT(Date,10)='@Date@', {Team}='@team@')"
	formula = formula.replace('@Date@',dt_date)
	formula = formula.replace('@team@',team)
	# formula = formula.replace('@creator_address@',creator_address)

	# Get Linked Team Name
	if (record['fields']['Team'] != '') and (record['fields']['Team'] is not None and (record['fields']['Team'] != 'Unmapped Address') ):
		linked_id = get_linked_record_id(at,'Teams',linked_field_name, record['fields']['Team'])
		record['fields'][linked_field_name] = linked_id

	#Handle for timestamps
	record = convert_timestamps_to_strings(record)
	#Handle for nulls
	record = replace_nans(record)
	
	# Check if we update
	for existing_record in at.iterate(table_name,
				   filter_by_formula = formula
				   ):
		# print(existing_record)
		if (existing_record['fields']['Date'][:10] == record['fields']['Date'][:10] and
			existing_record['fields']['Team'] == record['fields']['Team']
			# and existing_record['fields']['Creator Address'] == record['fields']['Creator Address']
			):
			# Update the matching record
			print('update existing row')

			print(record['fields'])
			at.update(table_name, existing_record['id'], record['fields'])

			return
	
    # If no matching record was found, create a new one
	print('add new record')
	# print(record['fields'])
	
	at.create(table_name, record['fields'])


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
		if table_name == 'OP Contract Data':
			upsert_record_dt_contract_creator(at, table_name, record)
		elif table_name == 'OP Project Data':
			upsert_record_dt_team(at, table_name, record)

def delete_all_records(at_base_id, table_name):
	remaining = 1
	while remaining > 0:
		# Create an instance of the Airtable class
		# at = airtable.Airtable(at_base_id, at_api_key)
		at = airtable.Airtable(at_base_id, at_api_key)

		tbl = at.get(table_name)
		# records = tbl['records']['id']
		ids = [record['id'] for record in tbl['records']]

		print(len(ids))

		for i in ids:
			at.delete(table_name, i)

		#check
		tbl_end = at.get(table_name)
		ids_end = [record['id'] for record in tbl['records']]

		remaining = len(ids_end)
		# Confirm that all records have been deleted
		if remaining == 0:
			print("All records have been deleted")
		else:
			print("Some records remain")



