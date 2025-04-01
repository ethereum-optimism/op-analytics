from .historical import HistoricalCPI
from .powerindex import ConcentrationOfPowerIndex
from .dataaccess import DaoPowerIndex


def execute_pull():
    powerindex = ConcentrationOfPowerIndex.fetch()
    historical = HistoricalCPI.fetch()

    DaoPowerIndex.CPI_SNAPSHOTS.write(powerindex.snapshots_df)
    DaoPowerIndex.CPI_COUNCIL_PERCENTAGES.write(powerindex.council_percentages_df)
    DaoPowerIndex.CPI_HISTORICAL.write(historical.df)
