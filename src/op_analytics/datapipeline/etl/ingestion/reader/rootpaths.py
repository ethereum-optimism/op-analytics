from dataclasses import dataclass

from op_analytics.datapipeline.chains.goldsky_chains import determine_network, ChainNetwork


@dataclass(frozen=True, order=True)
class RootPath:
    """The root path for a dataset in GCS.

    Root paths are toggled for TESTNET chains. Which is why we need to wrap
    them under a custom class instead of dealing with them as plain strings.
    """

    # Single word before the first "/"
    prefix: str

    # Everything after the first "/". Can be a single word or "/"-separated.
    suffix: str

    @classmethod
    def of(cls, root_path: str) -> "RootPath":
        """Create a new RootPath object."""
        if "/" not in root_path:
            raise ValueError(f"Invalid root path format: {root_path}")
        prefix, suffix = root_path.split("/", 1)

        return cls(
            prefix=prefix,
            suffix=suffix,
        )

    @property
    def root_path(self) -> str:
        """The full root path."""
        return f"{self.prefix}/{self.suffix}"

    def physical_for_chain(self, chain: str) -> str:
        """Get the root path adapted for the given network type.

        For testnet chains, appends "_testnets" to the prefix.
        For mainnet chains, returns the original root path.
        """

        network = determine_network(chain)

        if network == ChainNetwork.TESTNET:
            return f"{self.prefix}_testnets/{self.suffix}"

        else:
            return self.root_path
