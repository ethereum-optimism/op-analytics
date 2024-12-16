from dataclasses import dataclass


from .goldsky_chains import determine_network, ChainNetwork


@dataclass
class TestnetRootPathAdapter:
    """Utility to help us with TESTNET data which is stored in a different physical path."""

    chains: list[str]
    root_path_prefix: str

    # Root paths that will be read. Logical names which may be different
    # than physical names where the data is actualy stored.
    root_paths_to_read: list[str]

    def root_path_mapping(self, chain: str) -> dict[str, str]:
        """Mapping from actual physical paths that will be read to logical paths.

        Returns a dictionary where keys are the updated root_paths and values are the
        original root_paths.

        Root paths are different for mainnet and testnet:

            MAINNET :  ingestion/
            TESTNET :  ingestion_testnets/

        We update the root paths to account for TESTNET chains, keeping a reference to the
        original root_path that was requested to the reader.

        This lets model implementations always refer to data with the mainnet path, for example
        "ingestion/traces_v1". When running on testnet we use the correct location for the data.
        """
        root_paths = self.root_paths_to_read

        network = determine_network(chain)

        updated_root_paths = {}
        if network == ChainNetwork.TESTNET:
            for path in root_paths:
                if path.startswith(f"{self.root_path_prefix}/"):
                    updated_root_paths[
                        f"{self.root_path_prefix}_testnets"
                        + path.removeprefix(self.root_path_prefix)
                    ] = path
                else:
                    updated_root_paths[path] = path
        else:
            for path in root_paths:
                updated_root_paths[path] = path

        return updated_root_paths

    def root_paths_physical(self, chain: str) -> list[str]:
        """Physical root paths that are checked and read from storage."""
        return sorted(self.root_path_mapping(chain).keys())

    def root_paths_query_filter(self):
        """Root path filter used when querying markers."""
        physical_root_paths = set()
        for chain in self.chains:
            physical_root_paths.update(self.root_paths_physical(chain))
        return sorted(physical_root_paths)

    def data_paths(
        self,
        chain,
        physical_data_paths: dict[str, list[str]] | None,
    ) -> dict[str, list[str]]:
        """Updates keys to be logical paths.

        The input dictionary is a map from physical root path to physical data paths.
        The output dictionary replaces the keys with logical root paths.
        """
        updated_dataset_paths = {}
        for root_path, data_paths in (physical_data_paths or {}).items():
            updated_dataset_paths[self.root_path_mapping(chain)[root_path]] = data_paths
        return updated_dataset_paths
