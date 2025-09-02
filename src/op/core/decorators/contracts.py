from typing import Mapping, Sequence, Tuple, Any, Type
from ..defs.node_contracts import NodeContracts
from ..defs.ports import Port
from ..defs.product_contract import ProductContract


def contracts(
    *,
    requires: Mapping[str, ProductContract] | Sequence[ProductContract],
    provides: Mapping[str, ProductContract] | Sequence[ProductContract],
):
    def _norm(seq_or_map, default_prefix: str) -> Tuple[Port, ...]:
        if isinstance(seq_or_map, Mapping):
            return tuple(Port(name=k, contract=v) for k, v in seq_or_map.items())
        seq = list(seq_or_map)  # type: ignore[arg-type]
        return tuple(Port(name=f"{default_prefix}{i}", contract=pc) for i, pc in enumerate(seq))

    def _wrap(cls: Type[Any]) -> Type[Any]:
        req_ports = _norm(requires, "req")
        pro_ports = _norm(provides, "out")
        setattr(cls, "__node_contracts__", NodeContracts(requires=req_ports, provides=pro_ports))
        return cls
    return _wrap
