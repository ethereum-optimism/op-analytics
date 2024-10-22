# csv_utils.py
import os
import pandas as pd

def df_to_warehouse_uploads_csv(dataframe, filename):
    # Define the directory relative to the main repository
    relative_path = '../data_warehouse_uploads/files_to_upload'

    # Ensure the filename ends with .csv
    if not filename.endswith('.csv'):
        filename += '.csv'
    
    # Get the absolute path of the directory
    absolute_path = os.path.join(os.path.dirname(__file__), relative_path)
    
    # Ensure the directory exists
    os.makedirs(absolute_path, exist_ok=True)
    
    # Construct the full file path
    file_path = os.path.join(absolute_path, filename)
    
    # Save the DataFrame to CSV
    dataframe.to_csv(file_path, index=False)
    
    print(f"File saved to: {file_path}")
