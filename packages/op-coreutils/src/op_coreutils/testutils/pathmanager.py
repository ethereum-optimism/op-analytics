import os
from dataclasses import dataclass


@dataclass
class PathManager:
    """Utitlity to help manage location of data files used in tests."""

    abspath: str

    @classmethod
    def at(cls, file_path: str) -> "PathManager":
        """Initialize at the same directory of the provided file path.

        Used in unit tests as PathManager.at(__file__)
        """
        return cls(os.path.abspath(os.path.dirname(file_path)))

    def path(self, basename):
        """Return abspath for the given basename on the TestCase directory."""

        return os.path.join(self.abspath, basename)
