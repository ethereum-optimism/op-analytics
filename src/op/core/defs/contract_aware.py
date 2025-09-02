from typing import Dict, Optional
from ..defs.node_contracts import NodeContracts
from ..types.product_ref import ProductRef
from ..types.dataset import Dataset

class ContractAware:
    @property
    def contracts(self) -> NodeContracts:
        nc = getattr(self, "__node_contracts__", None)
        if nc is None:
            raise AttributeError(f"{self.__class__.__name__} missing __node_contracts__ (apply @contracts)")
        return nc

    def req_product(self, name: Optional[str] = None) -> ProductRef:
        ports = self.contracts.requires
        if name is None:
            if len(ports) != 1:
                raise ValueError("Multiple required ports; specify a name")
            return ports[0].contract.product
        for p in ports:
            if p.name == name:
                return p.contract.product
        raise KeyError(f"Required port '{name}' not found")

    def out_product(self, name: Optional[str] = None) -> ProductRef:
        ports = self.contracts.provides
        if name is None:
            if len(ports) != 1:
                raise ValueError("Multiple provided ports; specify a name")
            return ports[0].contract.product
        for p in ports:
            if p.name == name:
                return p.contract.product
        raise KeyError(f"Provided port '{name}' not found")

    def req_ds(self, inputs: Dict[ProductRef, Dataset], name: Optional[str] = None) -> Dataset:
        return inputs[self.req_product(name)]

    def out_map(self, rows, name: Optional[str] = None) -> Dict[ProductRef, Dataset]:
        prod = self.out_product(name)
        ds = rows if isinstance(rows, Dataset) else Dataset(rows=list(rows))
        return {prod: ds}
