estimated_size_sql = \
'''receipt_l1_fee /
(
 16*COALESCE(receipt_l1_fee_scalar,receipt_l1_base_fee_scalar)*cast(receipt_l1_gas_price AS Nullable(Float64))/1e6
 + COALESCE( receipt_l1_blob_base_fee_scalar*cast(receipt_l1_blob_base_fee AS Nullable(Float64))/1e6 , 0)
)'''

calldata_gas_sql = \
'''16 * (length(replace(toString(unhex(input)), '\0', '')) - 1)
            + 4 * ((length(unhex(input)) - 1) - (length(replace(toString(unhex(input)), '\0', '')) - 1))'''

num_zero_bytes_sql = \
'''((length(unhex(input)) - 1) - (length(replace(toString(unhex(input)), '\0', '')) - 1))'''
num_nonzero_bytes_sql = \
'''(length(replace(toString(unhex(input)), '\0', '')) - 1)'''

byte_length_sql = \
'''(length(unhex(input)) - 1)'''

gas_fee_sql = \
'''cast((gas_price * t.receipt_gas_used) + receipt_l1_fee AS Nullable(Float64))'''
import re

def process_goldsky_sql(query_sql):
    # Dictionary of SQL formulas with their placeholders
    sql_formulas = {
        "@estimated_size_sql@": estimated_size_sql,
        "@calldata_gas_sql@": calldata_gas_sql,
        "@byte_length_sql@": byte_length_sql,
        "@gas_fee_sql@": gas_fee_sql
    }

    # Function to replace placeholder with formula
    def replace_formula(match):
        placeholder = match.group(0)
        formula = sql_formulas.get(placeholder)
        return f"({formula})" if formula else placeholder

    # Replace all placeholders in the query
    processed_query = re.sub("|".join(map(re.escape, sql_formulas.keys())), replace_formula, query_sql)

    return processed_query