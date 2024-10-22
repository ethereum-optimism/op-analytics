#Given an OP Stack chain folder/repo, iterate through the files to extract the addresses used
import pandas as pd
import os
import glob
import json


def get_op_chain_addresses_from_local_folder(file_location, chain_name = 'Chain Name',source_name = 'Source Name'):
    original_location = os.getcwd()
    os.chdir(file_location)

    # DO STUFF
    #THanks chatgpt
    # Find all JSON files in the directory
    json_files = glob.glob("*.json")

    # Initialize an empty list to store the data
    data = []

    # Loop through each JSON file
    for file in json_files:
        # Load the JSON data
        with open(file, "r") as f:
            json_data = json.load(f)
        
        # Get the address from the JSON data
        address = json_data["address"]
        
        # Get the contract name from the file name
        contract_name = os.path.splitext(file)[0]
        
        # Append the address and contract name to the data list
        data.append({"address": address, "contract_name": contract_name})

    # Create a pandas DataFrame from the data list
    df = pd.DataFrame(data)

    df['chain'] = chain_name
    df['source'] = source_name 

    os.chdir(original_location)

    return df