from typing import Literal, Union
from pydantic import BaseModel, Field


class GoldskySource(BaseModel):
    source_type: Literal["goldsky"] = "goldsky"


class LocalFileSource(BaseModel):
    rootpath: str

    source_type: Literal["localfile"] = "localfile"


class DataSource(BaseModel):
    source: Union[GoldskySource, LocalFileSource] = Field(..., discriminator="source_type")

    @classmethod
    def from_spec(cls, source_spec: str) -> "DataSource":
        if source_spec.startswith("goldsky"):
            return GoldskySource()

        if source_spec.startswith("local:"):
            return LocalFileSource(rootpath=source_spec.removeprefix("local:"))

        raise NotImplementedError()
