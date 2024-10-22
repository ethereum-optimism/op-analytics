# Define a chain parameter.
# We don't specify any time range here, a goal is that if you wanted to simulate (this chain for 1 hour, then this chain for 1 hour, we can do that)
class Chain:
    def __init__(self
                    , num_transactions
                    , l2_gas_per_tx, calldata_gas_used_per_tx
                    , l2_priorityfee_gwei
                    , l1BaseFeeScalar, l1BlobBaseFeeScalar
                    , l2_basefee_gwei
                    , l1gas_basefee_gwei, l1blob_basefee_gwei
                ):
        
        # Chain Activity
        self.num_transactions = num_transactions
        self.l2_gas_per_tx = l2_gas_per_tx
        self.calldata_gas_used_per_tx = calldata_gas_used_per_tx
        self.compressedtxsize_per_tx = calldata_gas_used_per_tx + 68*16
        self.l2_priorityfee_gwei = l2_priorityfee_gwei
        # Chain parameters
        self.l1BaseFeeScalar = l1BaseFeeScalar
        self.l1BlobBaseFeeScalar = l1BlobBaseFeeScalar
        # External Market Data
        self.l2_basefee_gwei = l2_basefee_gwei
        self.l1gas_basefee_gwei = l1gas_basefee_gwei
        self.l1blob_basefee_gwei = l1blob_basefee_gwei
        
        # Values that start as None and can be modified later
        self.l2_gas_used = None
        self.l2_execution_revenue_eth = None
        self.l2_execution_basefee_eth = None
        self.l2_execution_priorityfee_eth = None
        

    def set_l2_gas_used(self, l2_gas_used):
        self.l2_gas_used = l2_gas_used
    def set_l2_execution_revenue_eth(self, l2_execution_revenue_eth):
        self.l2_execution_revenue_eth = l2_execution_revenue_eth
    def set_l2_execution_basefee_eth(self, l2_execution_basefee_eth):
        self.l2_execution_basefee_eth = l2_execution_basefee_eth
    def set_l2_execution_priorityfee_eth(self, l2_execution_priorityfee_eth):
        self.l2_execution_priorityfee_eth = l2_execution_priorityfee_eth

# Example usage
# chain = Chain(num_transactions=1000, l2_base_fee=0.001)
# print("Initial L2 Execution Revenue ETH:", chain.l2_execution_revenue_eth)
# print("Initial Total L2 Gas Used:", chain.total_l2_gas_used)

# # Modifying values later
# chain.set_l2_execution_revenue_eth(10)
# chain.set_total_l2_gas_used(500000)
# print("Modified L2 Execution Revenue ETH:", chain.l2_execution_revenue_eth)
# print("Modified Total L2 Gas Used:", chain.total_l2_gas_used)
