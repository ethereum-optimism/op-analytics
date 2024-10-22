import pandas as pd
import datetime
import ast
import requests as r
import json
import re
from datetime import date
import os
from datetime import datetime, timezone, timedelta

def api_json_to_df(api_url):
    inf = pd.DataFrame( r.get(api_url).json() )
    return inf

def get_element_from_json_column(col,element):
    col = col.apply(json.dumps)
    # extract the 'name' element from the JSON data
    return pd.json_normalize(col.apply(json.loads))[element]

def parse_json_fields(df):
    json_fields = ['risks', 'badges', 'purposes', 'tvl_associatedTokens']
    for field in json_fields:
        if field in df.columns:
            df[field] = df[field].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
    return df

def is_repeated_field(series):
    return series.apply(lambda x: isinstance(x, (list, dict))).any()

# https://www.thepythoncode.com/article/convert-pandas-dataframe-to-html-table-python

def generate_html(dataframe: pd.DataFrame):
    # get the table HTML from the dataframe
    table_html = dataframe.to_html(table_id="table")
    # construct the complete HTML with jQuery Data tables
    # You can disable paging or enable y scrolling on lines 20 and 21 respectively
    html = f"""
    <html>
    <header>
        <link href="https://cdn.datatables.net/1.11.5/css/jquery.dataTables.min.css" rel="stylesheet">
    </header>
    <body>
    {table_html}
    <script src="https://code.jquery.com/jquery-3.6.0.slim.min.js" integrity="sha256-u7e5khyithlIdTpu22PHhENmPcRdFiHRjhAuHcs05RI=" crossorigin="anonymous"></script>
    <script type="text/javascript" src="https://cdn.datatables.net/1.11.5/js/jquery.dataTables.min.js"></script>
    <script>
        $(document).ready( function () {{
            $('#table').DataTable({{
                // paging: false,    
                // scrollY: 400,
                pageLength: 25
            }});
        }});
    </script>
    </body>
    </html>
    """
    # return the html
    return html


# https://nbviewer.org/gist/alubbock/e5d915397179b9626ae63a55244f510d
import uuid
import numpy

def DataTable(df):
    """ Prints a pandas.DataFrame using jQuery DataTable plugin """
    from IPython.display import HTML
    output = """<div id="datatable-%(uuid)s">%(html)s
            <script type="text/javascript">
                $(document).ready(function() {
                    require(['dataTables'], function() {
                        $('#datatable-%(uuid)s').find('table.datatable').dataTable({
                        columnDefs: [{ targets: %(sci_cols)s, type: 'scientific' }]});
                    });
                });
            </script>
        </div>
    """ % {'uuid': uuid.uuid1(), 'html': df.to_html(classes="datatable display"),
          'sci_cols': '[%s]' % ",".join([str(i) for i, _ in enumerate(df.dtypes == numpy.float64)])}
    return HTML(output)

def format_num(x, prepend):
    try:
        x = float(x)
        if x >= 1e6:
            mil_format = prepend + '{:.1f}M'
            return mil_format.format(x / 1e6)
        elif x >= 1e3:
            k_format = prepend + '{:.1f}k'
            return k_format.format(x / 1e3)
        else:
            return x
    except:
        return x
    
def format_num(x, prefix=''):
    if x == '': #if null, make it 0
        x=0
    x = float(x) #just cast for safety
    if x >= 1e6:
        return f'{prefix}{x / 1e6:,.1f}M'
    elif x >= 1e3:
        return f'{x / 1e3:,.1f}k'
    if x < -1e6:
        return f'-{prefix}{abs(x) / 1e6:,.1f}M'
    elif x < -1e3:
        return f'-{prefix}{abs(x) / 1e3:,.1f}k'
    elif x < 0:
        return f'-{prefix}{x:,.1f}'
    
    return f'{prefix}{x:,.1f}'
def format_pct(x):
    if x == '': #if null, return it
        return x
    else:
        x = float(x) #just cast for safety
        return '{:.1%}'.format(x)

#ChatGPT wrote this lol
def get_unix_timestamp(trailing_days):
    # get the current date at the start of the day
    today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    # calculate the date for the trailing_days ago
    trailing_date = today_start - timedelta(days=trailing_days)

    # calculate the Unix timestamp for the trailing date
    unix_timestamp = int(trailing_date.timestamp())

    return unix_timestamp

#chatgpt again
# define a function to convert strings to lists
def str_to_list(s):
    if pd.isna(s):
        return None
    else:
        return ast.literal_eval(s)

def formatted_columns_to_csv_format(df):
    df.rename(columns=lambda x: re.sub(r'\W+', '_', x.lower()))
    return df

def csv_columns_to_formatted(df):
    new_columns = []
    for column in df.columns:
        new_column = column.replace('_', ' ').title()
        new_columns.append(new_column)
    df.columns = new_columns
    return df

def get_datestring_from_datetime(datestring):
    try:
        date_string = datestring.strftime('%Y-%m-%d')
    except:
        date_string = datestring[:10]
    return date_string

def convert_text_timestamp_to_int(text_timestamp):
    try:
        datetime_obj = datetime.strptime(text_timestamp, "%Y-%m-%d %H:%M:%S")
        datetime_obj_utc = datetime_obj.replace(tzinfo=timezone.utc)
        timestamp = int(datetime_obj_utc.timestamp())
        return timestamp
    except ValueError:
        print("Invalid timestamp format. Expected format: YYYY-MM-DD HH:MM:SS")
        return None
    
def datetime_to_unix_timestamp(input_value):
    try:
        # Attempt to parse the input_value as a datetime object
        dt = datetime.strptime(input_value, '%Y-%m-%d %H:%M:%S')
        # Convert the datetime object to a Unix timestamp
        return int(dt.timestamp())
    except ValueError:
        # If the parsing fails, assume input_value is already a Unix timestamp
        return int(input_value)
       
def get_datetime_from_input(input_value):
    if isinstance(input_value, str):
        try:
            datetime_obj = datetime.strptime(input_value, "%Y-%m-%d %H:%M:%S")
            return datetime_obj
        except ValueError:
            print("Invalid date string format. Expected format: YYYY-MM-DD HH:MM:SS")
            return None
    elif isinstance(input_value, int):
        try:
            datetime_obj = datetime.fromtimestamp(input_value)
            return datetime_obj
        except OSError:
            print("Invalid timestamp value.")
            return None
    else:
        print("Invalid input type. Expected string or integer.")
        return None

def get_today_as_string():
    today = date.today()
    date_string = get_datestring_from_datetime(today)
    return date_string


def mkdir_if_not_exists(dir):
    if not os.path.exists(dir):
        os.mkdir(dir)

def format_datetime_columns(df, format='%Y-%m-%dT%H:%M:%S.%fZ'):
    for column in df.columns:
        if df[column].dtype == 'object':
            try:
                df[column] = pd.to_datetime(df[column], format=format).dt.strftime(format)
            except ValueError:
                pass
    return df

def float_format_func(x):
    if isinstance(x, (int, float)):
        x_str = '{:.18f}'.format(x)
        decimal_places = len(re.search(r'\.(\d+?)0*$', x_str).group(1))
        return '{:,.{}f}'.format(x, min(decimal_places, 18))
    else:
        return str(x)
    
def flatten_nested_data(df, column_name):
    # Function to convert null to empty dict
    def null_to_empty(x):
        if pd.isna(x) or x is None:
            return {}
        elif isinstance(x, dict) or isinstance(x, list):
            return x
        else:
            return {}

    # Apply the null_to_empty function
    df[column_name] = df[column_name].apply(null_to_empty)

    # Check if the column contains dictionaries or arrays
    if df[column_name].apply(lambda x: isinstance(x, dict) or isinstance(x, list)).any():
        flat_df = pd.json_normalize(df[column_name])
        flat_df = flat_df.add_prefix(f"{column_name}_")
        
        # Replace NaN with empty string
        flat_df = flat_df.fillna('')
        
        return pd.concat([df.drop(column_name, axis=1), flat_df], axis=1)
    else:
        return df
