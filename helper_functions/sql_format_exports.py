
#df = datadrame
#int_cols = columns we should treat as integers
#dec_cols = columns we should treat as decimals
import pandas as pd
import os
import numpy as np

def to_sql_format(df, prepend = '', int_cols = [], dec_cols = []):
        #for file export
        if prepend != '':
                prepend = prepend + '_'

        for col in df.columns:
                if col in int_cols:
                        pass #do nothing
                        # df[col] = df[col].fillna(-1)
                        # df[col] = pd.to_numeric(df[col], downcast='signed')
                        # df[col] = ',' + df[col].astype(str)
                elif col in dec_cols:
                        pass #do nothing
                else:
                        df[col] = df[col].fillna('')
                        df[col] = ',\'' + df[col].astype(str) + '\''
                
                df[col] = ',' + df[col].astype(str)

        # Add parenthases for SQL
        df.iloc[:, 0] = '(' + df.iloc[:, 0].str.replace(',','') #replace leading comma
        # Add comma for all except the first row
        df.iloc[1:, 0] = ',' + df.iloc[1:, 0].astype(str)
        # add parenthases to last row
        df.iloc[:, -1] = df.iloc[:, -1].astype(str) + ')'
        # df['blockchain'] = ',(' + df['blockchain'].astype(str)

        col_names = ['`' + c + '`' for c in df.columns]
        col_names = ",".join(col_names)


        if not os.path.exists('outputs'):
                os.makedirs('outputs')
                
        #write sql file
        df.to_csv('outputs/' + prepend + 'to_sql.txt', sep='\t', index=False)
        #write cols
        text_file = open('outputs/' + prepend + 'sql_column_names.txt', "w")
        n = text_file.write(col_names)
        text_file.close()