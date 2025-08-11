import csv
import time
from dataclasses import dataclass
from typing import Optional

import pandas as pd
import requests
import stamina
from requests.exceptions import JSONDecodeError

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.request import new_session

log = structlog.get_logger()


class RateLimit(Exception):
    """Raised when a rate limit error is encountered on the JSON-RPC response."""
    pass


class BalanceQueryError(Exception):
    """Raised when we fail to query balance from RPC."""
    pass


@dataclass(frozen=True)
class WalletAddress:
    """A wallet address to query."""
    address: str
    label: Optional[str] = None


@dataclass(frozen=True)
class TokenAddress:
    """An ERC20 token address to query."""
    address: str
    symbol: Optional[str] = None
    decimals: Optional[int] = None


@dataclass
class WalletTokenBalance:
    """Balance result for a wallet-token combination."""
    wallet: str
    wallet_label: Optional[str]
    token: str
    token_symbol: Optional[str]
    balance: Optional[str]  # Raw balance string to preserve precision
    decimals: Optional[int]
    balance_formatted: Optional[float]  # Human-readable balance


class WalletBalancePuller:
    """Main class for pulling wallet token balances."""
    
    def __init__(self, rpc_endpoint: str, rate_limit_delay: float = 0.1):
        self.rpc_endpoint = rpc_endpoint
        self.rate_limit_delay = rate_limit_delay
        self.session = new_session()
    
    def read_wallet_addresses(self, csv_path: str) -> list[WalletAddress]:
        """Read wallet addresses from CSV file.
        
        Expected CSV format:
        - Column 'address' (required)
        - Column 'label' (optional)
        """
        wallets = []
        with open(csv_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if 'address' not in row:
                    raise ValueError("CSV must contain 'address' column")
                
                address = row['address'].strip()
                if not address:
                    continue
                    
                label = row.get('label', '').strip() or None
                wallets.append(WalletAddress(address=address, label=label))
        
        log.info(f"Loaded {len(wallets)} wallet addresses from {csv_path}")
        return wallets
    
    def read_token_addresses(self, csv_path: str) -> list[TokenAddress]:
        """Read token addresses from CSV file.
        
        Expected CSV format:
        - Column 'address' (required)
        - Column 'symbol' (optional)
        - Column 'decimals' (optional)
        """
        tokens = []
        with open(csv_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if 'address' not in row:
                    raise ValueError("CSV must contain 'address' column")
                
                address = row['address'].strip()
                if not address:
                    continue
                    
                symbol = row.get('symbol', '').strip() or None
                decimals_str = row.get('decimals', '').strip()
                decimals = int(decimals_str) if decimals_str.isdigit() else None
                
                tokens.append(TokenAddress(
                    address=address,
                    symbol=symbol,
                    decimals=decimals
                ))
        
        log.info(f"Loaded {len(tokens)} token addresses from {csv_path}")
        return tokens
    
    @stamina.retry(
        on=(requests.exceptions.RequestException, requests.exceptions.Timeout), 
        attempts=3, 
        wait_initial=5
    )
    def fetch_safe_multisig_addresses(self, safe_api_url: str) -> list[WalletAddress]:
        """Fetch Safe multisig addresses from API endpoint.
        
        Args:
            safe_api_url: API endpoint that returns {"safes": ["0x123...", "0x456..."]}
        
        Returns:
            List of WalletAddress objects with 'locked multisig' labels
        """
        try:
            response = self.session.get(safe_api_url, timeout=60)
            
            if response.status_code != 200:
                log.error(f"Failed to fetch Safe addresses: HTTP {response.status_code}")
                return []
            
            data = response.json()
            
            if 'safes' not in data:
                log.error("API response missing 'safes' field")
                return []
            
            safe_addresses = data['safes']
            wallets = []
            
            for address in safe_addresses:
                if address and isinstance(address, str):
                    # Clean up address format
                    clean_address = address.strip()
                    if clean_address.startswith('0x'):
                        wallets.append(WalletAddress(
                            address=clean_address,
                            label="locked multisig"
                        ))
            
            log.info(f"Fetched {len(wallets)} Safe multisig addresses from API")
            return wallets
            
        except Exception as e:
            log.error(f"Error fetching Safe addresses: {str(e)}")
            return []
    
    def combine_wallet_sources(
        self, 
        csv_wallets: list[WalletAddress], 
        safe_wallets: list[WalletAddress]
    ) -> list[WalletAddress]:
        """Combine wallet addresses from multiple sources and remove duplicates.
        
        Args:
            csv_wallets: Wallets from CSV file
            safe_wallets: Wallets from Safe API
            
        Returns:
            Deduplicated list of wallet addresses
        """
        # Use a dict to track unique addresses and preserve the first label encountered
        unique_wallets = {}
        
        # Add CSV wallets first (they take priority for labeling)
        for wallet in csv_wallets:
            unique_wallets[wallet.address.lower()] = wallet
        
        # Add Safe API wallets, but don't overwrite existing ones
        for wallet in safe_wallets:
            addr_key = wallet.address.lower()
            if addr_key not in unique_wallets:
                unique_wallets[addr_key] = wallet
        
        result = list(unique_wallets.values())
        
        log.info(f"Combined wallet sources: {len(csv_wallets)} from CSV + {len(safe_wallets)} from API = {len(result)} unique wallets")
        
        return result
    
    @stamina.retry(
        on=(RateLimit, requests.exceptions.RequestException, requests.exceptions.Timeout), 
        attempts=5, 
        wait_initial=2,
        wait_jitter=1
    )
    def get_erc20_balance(self, wallet_address: str, token_address: str) -> Optional[str]:
        """Get ERC20 token balance for a wallet using JSON-RPC call."""
        
        # ERC20 balanceOf method signature: balanceOf(address)
        # Method ID: 0x70a08231
        # Padded wallet address (32 bytes)
        wallet_padded = wallet_address[2:].lower().zfill(64) if wallet_address.startswith('0x') else wallet_address.lower().zfill(64)
        data = f"0x70a08231{wallet_padded}"
        
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [
                {
                    "to": token_address,
                    "data": data
                },
                "latest"
            ],
            "id": 1
        }
        
        try:
            response = self.session.post(
                self.rpc_endpoint,
                json=payload,
                timeout=60  # Increased timeout for better reliability
            )
            
            if response.status_code != 200:
                log.error(f"HTTP {response.status_code} for balance query", 
                         wallet=wallet_address, token=token_address)
                if response.status_code == 429:  # Rate limit
                    raise RateLimit("Rate limit exceeded")
                return None
            
            try:
                result = response.json()
            except JSONDecodeError as ex:
                raise BalanceQueryError(f"Invalid JSON response: {ex}")
            
            if "error" in result:
                error_msg = result["error"].get("message", "Unknown error")
                log.error(f"RPC error: {error_msg}", wallet=wallet_address, token=token_address)
                return None
            
            if "result" in result and result["result"] is not None:
                # Convert hex result to decimal string
                balance_hex = result["result"]
                if balance_hex == "0x":
                    return "0"
                try:
                    balance_int = int(balance_hex, 16)
                    return str(balance_int)
                except ValueError:
                    log.error(f"Invalid hex balance: {balance_hex}", 
                             wallet=wallet_address, token=token_address)
                    return None
            
            return None
            
        except Exception as e:
            log.warning(f"Request failed, will retry: {str(e)}", wallet=wallet_address, token=token_address)
            raise  # Re-raise to trigger retry
    
    @stamina.retry(
        on=(RateLimit, requests.exceptions.RequestException, requests.exceptions.Timeout), 
        attempts=5, 
        wait_initial=2,
        wait_jitter=1
    )
    def get_token_decimals(self, token_address: str) -> Optional[int]:
        """Get token decimals using JSON-RPC call."""
        
        # ERC20 decimals method signature: decimals()
        # Method ID: 0x313ce567
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [
                {
                    "to": token_address,
                    "data": "0x313ce567"
                },
                "latest"
            ],
            "id": 1
        }
        
        try:
            response = self.session.post(
                self.rpc_endpoint,
                json=payload,
                timeout=60  # Increased timeout for better reliability
            )
            
            if response.status_code != 200:
                if response.status_code == 429:  # Rate limit
                    raise RateLimit("Rate limit exceeded")
                return None
            
            result = response.json()
            
            if "error" in result or "result" not in result:
                return None
            
            decimals_hex = result["result"]
            if decimals_hex and decimals_hex != "0x":
                try:
                    return int(decimals_hex, 16)
                except ValueError:
                    return None
            
            return None
            
        except Exception as e:
            log.warning(f"Failed to get decimals, will retry: {str(e)}", token=token_address)
            raise  # Re-raise to trigger retry
    
    def query_wallet_token_balance(
        self, 
        wallet: WalletAddress, 
        token: TokenAddress
    ) -> WalletTokenBalance:
        """Query balance for a specific wallet-token combination."""
        
        # Get balance
        balance_raw = self.get_erc20_balance(wallet.address, token.address)
        
        # Get or use token decimals
        decimals = token.decimals
        if decimals is None:
            decimals = self.get_token_decimals(token.address)
        
        # Calculate formatted balance
        balance_formatted = None
        if balance_raw is not None and decimals is not None:
            try:
                balance_int = int(balance_raw)
                balance_formatted = balance_int / (10 ** decimals)
            except (ValueError, ZeroDivisionError):
                pass
        
        # Rate limiting
        time.sleep(self.rate_limit_delay)
        
        return WalletTokenBalance(
            wallet=wallet.address,
            wallet_label=wallet.label,
            token=token.address,
            token_symbol=token.symbol,
            balance=balance_raw,
            decimals=decimals,
            balance_formatted=balance_formatted
        )
    
    def pull_all_balances(
        self, 
        wallets: list[WalletAddress], 
        tokens: list[TokenAddress]
    ) -> list[WalletTokenBalance]:
        """Pull balances for all wallet-token combinations."""
        
        results = []
        total_combinations = len(wallets) * len(tokens)
        log.info(f"Starting balance queries for {total_combinations} wallet-token combinations")
        
        count = 0
        for wallet in wallets:
            for token in tokens:
                count += 1
                log.info(f"Querying balance {count}/{total_combinations}", 
                        wallet=wallet.address, token=token.address)
                
                balance_result = self.query_wallet_token_balance(wallet, token)
                results.append(balance_result)
        
        log.info(f"Completed balance queries: {len(results)} results")
        return results
    
    def save_results_to_csv(self, results: list[WalletTokenBalance], output_path: str):
        """Save results to CSV file."""
        
        data = []
        for result in results:
            data.append({
                'wallet': result.wallet,
                'wallet_label': result.wallet_label or '',
                'token': result.token,
                'token_symbol': result.token_symbol or '',
                'balance': result.balance or '',
                'decimals': result.decimals or '',
                'balance_formatted': result.balance_formatted or ''
            })
        
        df = pd.DataFrame(data)
        df.to_csv(output_path, index=False)
        log.info(f"Saved {len(data)} balance records to {output_path}")


def execute_wallet_balance_pull(
    wallets_csv: str,
    tokens_csv: str,
    output_csv: str,
    rpc_endpoint: str,
    rate_limit_delay: float = 0.1,
    safe_api_url: Optional[str] = None
):
    """Execute a complete wallet balance pull operation."""
    
    log.info("Starting wallet balance pull",
             wallets_csv=wallets_csv,
             tokens_csv=tokens_csv,
             output_csv=output_csv,
             rpc_endpoint=rpc_endpoint,
             safe_api_url=safe_api_url)
    
    puller = WalletBalancePuller(
        rpc_endpoint=rpc_endpoint,
        rate_limit_delay=rate_limit_delay
    )
    
    # Load input data
    csv_wallets = puller.read_wallet_addresses(wallets_csv)
    tokens = puller.read_token_addresses(tokens_csv)
    
    # Optionally fetch Safe multisig addresses
    safe_wallets = []
    if safe_api_url:
        safe_wallets = puller.fetch_safe_multisig_addresses(safe_api_url)
    
    # Combine and deduplicate wallet sources
    wallets = puller.combine_wallet_sources(csv_wallets, safe_wallets)
    
    # Pull balances
    results = puller.pull_all_balances(wallets, tokens)
    
    # Save results
    puller.save_results_to_csv(results, output_csv)
    
    log.info("Wallet balance pull completed successfully")
    return results
