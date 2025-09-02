from abc import ABC, abstractmethod
from datetime import date
from typing import Mapping, Sequence

from ..types.file_marker import FileMarker
from ..types.run_marker import RunMarker


def partition_key_str(values: Mapping[str,str]) -> str:
    return "|".join(f"{k}={values[k]}" for k in sorted(values))


class MarkerStore(ABC):
    @abstractmethod
    def begin_run(self, run: RunMarker) -> None: ...
    @abstractmethod
    def finish_run(self, run_id: str, status: str, finished_at, error_msg: str | None) -> None: ...
    @abstractmethod
    def write_file_markers(self, markers: Sequence[FileMarker]) -> None: ...
    @abstractmethod
    def latest_files(self, product_id: str, where: Mapping[str,str] = {}) -> list[FileMarker]: ...
    # Planning helpers
    @abstractmethod
    def missing_partitions(self, producer: str, consumer: str, dims: Mapping[str,str], lookback_days: int) -> list[str]: ...
    @abstractmethod
    def coverage(self, product_id: str, dims: Mapping[str,str], window: tuple[date,date]) -> list[dict]: ...
