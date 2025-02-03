/**

Contract create traces that

*/
INSERT INTO _placeholder_


WITH create_traces AS (
  SELECT
    dt
    , chain
    , chain_id
    , network
    , 'SuperchainERC20' AS token_type
    , block_timestamp
    , block_number
    , transaction_hash
    , transaction_index
    , tr_from_address AS tr_from_address
    , tx_from_address AS tx_from_address
    , contract_address
    , trace_address
    , trace_type
    , gas
    , gas_used
    , (output LIKE '%2b8c49e3%' AND output LIKE '%18bf5077%') AS has_crosschain_methods
    , output LIKE '%363d3d37363d34f0%' AS is_from_proxy
  FROM
    blockbatch_gcs.read_date(
      rootpath = 'blockbatch/contract_creation/create_traces_v1'
      , chain = '*'
      , dt = { dtparam: Date }
    )
  WHERE
    dt = { dtparam: Date }
    AND (
    -- crosschainBurn(address,uint256): 0x2b8c49e3
    -- crosschainMint(address,uint256): 0x18bf5077
    has_crosschain_methods

    -- Traces from contract creation proxies
    -- https://gist.github.com/Aboudjem/3ae8cc89ebff8c086e9b9111b1d06e6d#file-create3-sol-L25
    OR is_from_proxy
    )
)

-- 7802 contracts that are created with create2.
, create2 AS (
  SELECT c0.*
  FROM create_traces AS c0
  WHERE
    has_crosschain_methods
    AND c0.trace_type = 'create2'
)

-- 7802 contracts that are created via a proxy.
, create3 AS (
  SELECT c0.*
  FROM create_traces AS c0
  INNER JOIN create_traces AS c2
    ON
      c0.chain_id = c2.chain_id
      AND c0.block_number = c2.block_number
      AND c0.transaction_hash = c2.transaction_hash
      --
      -- The create2 to address is the create from address.
      -- See example:
      -- https://basescan.org/tx/0xc852edafee6f2d049967a182d75b64933395146f465f22812b00d1bfa847e2d8#internal
      AND c0.tr_from_address = c2.contract_address
  WHERE
    -- Trace output has crosschainBurn/crosschainMint methods.
    c0.has_crosschain_methods
    AND c0.trace_type LIKE 'create%'
    AND c0.trace_type != 'create2'

    -- Traces output indicates creation is made by a proxy.
    -- https://gist.github.com/Aboudjem/3ae8cc89ebff8c086e9b9111b1d06e6d#file-create3-sol-L25
    AND c2.is_from_proxy
    AND c2.trace_type = 'create2'
)

SELECT
  'create0/3' AS deployment_type
  , c3.*
FROM create3 AS c3

UNION ALL

SELECT
  'create2' AS deployment_type
  , c2.*
FROM create2 AS c2


SETTINGS use_hive_partitioning = 1
