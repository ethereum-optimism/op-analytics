"""
Example usage of the refactored wallet balance puller.

This shows how to use both the generic WalletBalancePuller for simple queries
and the BatchWalletBalancePuller for complex operations.
"""

from .main import WalletBalancePuller, BatchWalletBalancePuller


def simple_balance_query_example():
    """Example of using the generic WalletBalancePuller for a single query."""
    
    # Create the core puller
    puller = WalletBalancePuller(
        rpc_endpoint="https://mainnet.optimism.io/",
        rate_limit_delay=0.1
    )
    
    # Query a single wallet-token balance
    result = puller.get_balance(
        wallet_address="0xe80519596014E0D02C34ceEE1f76e10DF5e0B07e",
        token_address="0x4200000000000000000000000000000000000042",  # OP token
        token_symbol="OP",
        token_decimals=18
    )
    
    print(f"Wallet: {result.wallet}")
    print(f"Token: {result.token_symbol}")
    print(f"Raw Balance: {result.balance}")
    print(f"Formatted Balance: {result.balance_formatted}")
    
    return result


def batch_operations_example():
    """Example of using BatchWalletBalancePuller for complex operations."""
    
    # Create the core puller
    puller = WalletBalancePuller(
        rpc_endpoint="https://mainnet.optimism.io/",
        rate_limit_delay=0.1
    )
    
    # Create the batch orchestrator
    batch_puller = BatchWalletBalancePuller(puller)
    
    # You can now use all the CSV reading, Safe API, and batch operations
    # This is what the CLI command uses internally
    
    return batch_puller


if __name__ == "__main__":
    # Run the simple example
    print("=== Simple Balance Query ===")
    simple_balance_query_example()
    
    print("\n=== Batch Operations Available ===")
    batch_puller = batch_operations_example()
    print(f"Batch puller created with core puller: {type(batch_puller.puller)}")
