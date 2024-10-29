from typing import NewType

# Root path for a partitioned dataframe output.
SinkOutputRootPath = NewType("SinkOutputRootPath", str)

# A single object path in a partitioned dataframe output (includes the base bath).
SinkOutputPath = NewType("SinkOutputPath", str)

# A single object path for a sink marker. Markers are light objects that are used to
# indicate a sink output has been written successfully.
SinkMarkerPath = NewType("SinkMarkerPath", str)
