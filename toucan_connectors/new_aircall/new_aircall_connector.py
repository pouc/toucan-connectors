# AirCall Business-in-a-Box connector
# In development

# ***** IMPORTS *****

# VENDOR
import asyncio
import pandas as pd

# CONNECTOR MODULES
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource

BASE_AIRCALL_API_URL = "https://api.aircall.io/v1"

def build_aircall_request_url(route):
    return f'{BASE_AIRCALL_API_URL}/{route}'

class New_aircallDataSource(ToucanDataSource):
    query: str


class New_aircallConnector(ToucanConnector):
    data_source_model: New_aircallDataSource

    # username: str
    # password: str

    def _retrieve_data(self, data_source: New_aircallDataSource) -> pd.DataFrame:
        pass

    def set_up_request(self, route):
        aircall_request_url = build_aircall_request_url(route)
        return aircall_request_url