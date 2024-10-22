# These functions should all log intermediate data points (i.e. blobgas used, # blobgas transactions) & write to an output runs csv

import chain as c

def initialize_chain():
    sample_chain = 0 #Maybe define a chain() object, so we can just access its values, versus tracking what functions we return

    return sample_chain
# This Function returns the total expected Revenue from projected L2 Execution
def total_revenue_execution(chain):
    total_revenue_execution_eth = 0

    # Generate calcs
    l2_gas_used = chain.l2_transactions * chain.l2_gas_per_tx
    l2_execution_basefee_eth = chain.l2_basefee_gwei * l2_gas_used / 1e9
    l2_execution_priorityfee_eth = chain.l2_priorityfee_gwei * l2_gas_used / 1e9
    l2_revenue_execution_eth = l2_execution_basefee_eth + l2_execution_priorityfee_eth

    # Logging - No formulas here, these should already be calculated
    c.set_l2_gas_used(chain, l2_gas_used)
    c.set_l2_execution_basefee_eth(chain,l2_execution_basefee_eth)
    c.set_l2_execution_priorityfee_eth(chain,l2_execution_priorityfee_eth)
    c.set_l2_execution_revenue_eth(chain,l2_revenue_execution_eth)
    
    # Return modified chain - maybe we don't need to return though?
    # return chain

# ----------

# This Function returns the total expected Revenue from projected DA
def total_revenue_da():
    total_revenue_da = 0
    # Generate calcs
    return total_revenue_da

# If Using Blobs: This Function returns the total expected cost from projected Blob data
def total_cost_blobspace():
    # Input for L1 DA vs alt-da?
    total_cost_blobspace = 0
    # Generate calcs
    return total_cost_blobspace

# If Using Blobs: This Function returns the total expected cost from projected Blob data
def total_cost_blob_commitments():
    # Input for L1 DA vs alt-da?
    total_cost_blob_commitments = 0
    # Generate calcs
    return total_cost_blob_commitments

# If Using Calldata: This Function returns the total expected cost from projected L1 calldata
def total_cost_l1_calldata():
    # Input for L1 DA vs alt-da?
    total_cost_l1_calldata = 0
    # Generate calcs
    return total_cost_l1_calldata

# If Using Calldata: This Function returns the total expected overhead cost from L1 Calldata
def total_cost_l1_overhead():
    # Input for L1 DA vs alt-da?
    total_cost_l1_overhead = 0 #21k * # L1 DA Transactions
    # Generate calcs
    return total_cost_l1_overhead

# If This Function returns the total expected cost from projected state proposals
def total_cost_state_proposals():
    # Does this need to handle for L2s only (state proposals on L1) or also L3s (state proposals on L2)
    total_cost_state_proposals = 0
    # Generate calcs
    return total_cost_state_proposals

# This function returns expected transaction revenue on L2
def get_l2_revenue():
    total_revenue_eth = 0
    da_revenue_eth = total_revenue_da()
    total_revenue_execution_eth, l2_execution_basefee_eth, l2_execution_priorityfee_eth  = total_revenue_execution()
    # Generate calcs
    # Return total revenue, L1/Blob DA Revenue, L2 Execution Revenue
    return total_revenue_eth, da_revenue_eth, total_revenue_execution_eth, l2_execution_basefee_eth, l2_execution_priorityfee_eth

# This function returns expected costs on DA layer
    # da_type = {blobs, l1_calldata}
def get_da_costs(da_type):
    total_da_eth = 0

    if da_type == 'blobs':
        total_da_overhead_eth = total_cost_blob_commitments()
        total_da_txdata_eth = total_cost_blobspace()
    elif da_type == 'l1_calldata':
        total_da_overhead_eth = total_cost_l1_overhead()
        total_da_txdata_eth = total_cost_l1_calldata()

    total_cost_state_proposals = total_cost_state_proposals()


    # Generate calcs
    # Return total revenue, L1/Blob DA Revenue, L2 Execution Revenue
    return total_da_eth

