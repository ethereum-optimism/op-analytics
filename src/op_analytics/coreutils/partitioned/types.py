from typing import NewType

# Root path for a partitioned dataframe output.
PartitionedRootPath = NewType("PartitionedRootPath", str)

# A single object path in a partitioned dataframe output (includes the base bath).
PartitionedFullPath = NewType("PartitionedFullPath", str)

# A single object path for a sink marker. Markers are light objects that are used to
# indicate a sink output has been written successfully.
PartitionedMarkerPath = NewType("PartitionedMarkerPath", str)
