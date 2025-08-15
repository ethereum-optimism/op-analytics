# catalog/

Dataset publication/consumption registry.
- A **sink** may implement `Publishable` to expose a `ProductRef` for its output.
- The **pipeline** registers that product in the `DatasetCatalog` after a successful materialization.
- A **CatalogSourceStep** can resolve a `ProductRef` from the catalog and serve as a source for other pipelines.
