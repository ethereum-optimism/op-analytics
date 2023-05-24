import numpy as np

def count_calldata_gas(input_data):
    # Remove the '0x' prefix
    input_data = input_data[2:]

    # Calculate gas usage
    gas_usage = 0
    for i in range(0, len(input_data), 2):
        if input_data[i:i+2] == '00':
            gas_usage += 4
        else:
            gas_usage += 16

    return gas_usage



