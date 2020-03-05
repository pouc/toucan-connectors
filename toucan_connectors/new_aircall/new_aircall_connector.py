import pandas as pd

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class New_aircallDataSource(ToucanDataSource):
    query: str


class New_aircallConnector(ToucanConnector):
    data_source_model: New_aircallDataSource

    username: str
    password: str

    def _retrieve_data(self, data_source: New_aircallDataSource) -> pd.DataFrame:
        pass
