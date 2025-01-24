# Contract ABIs obtained here:
#
# Entry Point 0.6.0
# https://basescan.org/address/0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789
#
# Entry Point 0.7.0
# https://basescan.org/address/0x0000000071727de22e5e9d8baf0edac6f37da032#code

from .abi_v0_6_0 import ABI_V0_6_0
from .abi_v0_7_0 import ABI_V0_7_0

ENTRYPOINT_V0_6_0_ADDRESS = "0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789"
ENTRYPOINT_V0_7_0_ADDRESS = "0x0000000071727de22e5e9d8baf0edac6f37da032"

ERC4337_ENTRYPOINTS = f"('{ENTRYPOINT_V0_6_0_ADDRESS}', '{ENTRYPOINT_V0_7_0_ADDRESS}')"


HANDLE_OPS_FUNCTION_ABI_v0_6_0 = [_ for _ in ABI_V0_6_0 if _.get("name") == "handleOps"][0]
HANDLE_OPS_FUNCTION_ABI_v0_7_0 = [_ for _ in ABI_V0_7_0 if _.get("name") == "handleOps"][0]

INNER_HANDLE_OP_FUNCTION_ABI_v0_6_0 = [_ for _ in ABI_V0_6_0 if _.get("name") == "innerHandleOp"][0]
INNER_HANDLE_OP_FUNCTION_ABI_v0_7_0 = [_ for _ in ABI_V0_7_0 if _.get("name") == "innerHandleOp"][0]


# Method ids obtained from:
# https://basescan.org/methodidconverter
HANDLE_OPS_FUNCTION_METHOD_ID_v0_6_0 = "0x1fad948c"
INNER_HANDLE_OP_FUNCTION_METHOD_ID_v0_6_0 = "0x1d732756"

HANDLE_OPS_FUNCTION_METHOD_ID_v0_7_0 = "0x765e827f"
INNER_HANDLE_OP_FUNCTION_METHOD_ID_v0_7_0 = "0x0042dc53"
