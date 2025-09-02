from dataclasses import dataclass
from typing import Dict, Optional, List

# Your source list (unchanged)
chain_config: List[List[str]] = [
    ['bitcoin','Bitcoin','COUNT_IF(fee > 0)','COUNT(DISTINCT block_height)','transactions','fee','BTC','NULL'],
    ['near','Near','COUNT(distinct case when gas_price > 0 then tx_hash else null end)','COUNT(DISTINCT block_height)','actions','cast(NULL as double)','NULL','397'],
    ['aptos','Aptos','COUNT_IF(gas_used>0)','COUNT(DISTINCT block_height)','user_transactions','(gas_used*gas_unit_price/1e8)','APT','NULL'],
    # ['stellar','Stellar', ...]  # skipped in your list
    ['kaia','Kaia','COUNT_IF(gas_price>0)','COUNT(DISTINCT block_number)','transactions','cast(NULL as double)','NULL','8217'],
    ['ton','TON','COUNT_IF(compute_gas_fees>0)','COUNT(DISTINCT block_seqno)','transactions','cast(NULL as double)','NULL','NULL'],
    ['berachain','Berachain','COUNT_IF(gas_price>0)','COUNT(DISTINCT block_number)','transactions','cast(gas_price/1e9*gas_used/1e9 as double)','BERA','80094'],
    ['sonic','Sonic','COUNT_IF(gas_price>0)','COUNT(DISTINCT block_number)','transactions','cast(gas_price/1e9*gas_used/1e9 as double)','S','146'],
]

@dataclass(frozen=True)
class ChainOverride:
    display_name: Optional[str] = None
    tx_fee_currency: Optional[str] = None
    chain_id: Optional[int] = None

def _parse_int_or_none(s: str | int | None) -> Optional[int]:
    if s is None:
        return None
    if isinstance(s, int):
        return s
    s = str(s).strip()
    if s.upper() == "NULL" or s == "":
        return None
    try:
        return int(s)
    except Exception:
        return None

def build_overrides(cfg: List[List[str]]) -> Dict[str, ChainOverride]:
    """
    Returns a map: normalized blockchain key (lowercase) -> ChainOverride
    Columns in cfg are interpreted as:
      0: blockchain
      1: display_name
      6: tx_fee_currency
      7: chain_id
    """
    out: Dict[str, ChainOverride] = {}
    for row in cfg:
        bc = str(row[0]).strip().lower()
        dn = str(row[1]).strip() if len(row) > 1 and row[1] and str(row[1]).upper() != "NULL" else None
        cur = str(row[6]).strip() if len(row) > 6 and row[6] and str(row[6]).upper() != "NULL" else None
        cid = _parse_int_or_none(row[7] if len(row) > 7 else None)
        out[bc] = ChainOverride(display_name=dn, tx_fee_currency=cur, chain_id=cid)
    return out

CHAIN_OVERRIDES = build_overrides(chain_config)

def normalize_chain_key(blockchain: str, chain_id: Optional[int], display_name: Optional[str]) -> str:
    """
    Mirrors your SQL: lower(replace(coalesce(NULLIF(chain_id,'0'), display_name, blockchain),' ',''))
    If chain_id present and > 0 -> use chain_id as string
    else use display_name or blockchain; then lower and remove spaces.
    """
    if chain_id and chain_id > 0:
        base = str(chain_id)
    else:
        base = (display_name or blockchain or "").strip()
    return base.replace(" ", "").lower()
