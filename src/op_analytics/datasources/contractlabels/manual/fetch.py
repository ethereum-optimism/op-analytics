import json
import os
from dataclasses import dataclass

import polars as pl


DIRNAME = os.path.dirname(__file__)


@dataclass
class ManualLabels:
    @classmethod
    def fetch(cls) -> pl.DataFrame:
        with open(os.path.join(DIRNAME, "labels.json"), "r") as fobj:
            data = json.load(fobj)
            df = pl.DataFrame(data)

        # TODO: Fixes, and adapt the schema to what it should be for the common table.
        return df
