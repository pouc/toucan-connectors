import os
from enum import Enum
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

from toucan_connectors.oauth2_connector.oauth2connector import (
    OAuth2Connector,
    OAuth2ConnectorConfig,
)
from toucan_connectors.toucan_connector import (
    ConnectorSecretsForm,
    ToucanConnector,
    ToucanDataSource,
)

AUTHORIZATION_URL: str = 'https://app.hubspot.com/oauth/authorize'
# TODO: list scopes
SCOPE: str = 'oauth contacts content forms business-intelligence'
TOKEN_URL: str = 'https://api.hubapi.com/oauth/v1/token'
HUBSPOT_ENDPOINTS: dict = {
    'contacts': 'https://api.hubapi.com/crm/v3/objects/contacts',
    'content': 'https://api.hubapi.com/crm/v3/objects/content',
    'forms': 'https://api.hubapi.com/crm/v3/objects/forms',
    'products': 'https://api.hubapi.com/crm/v3/objects/forms',
    'web-analytics': 'https://api.hubapi.com/events/v3/events?objectType=contact',
}


class HubspotDataset(str, Enum):
    contacts = 'contacts'
    content = 'content'
    products = 'products'
    webanalytics = 'web-analytics'


class HubspotDataSource(ToucanDataSource):
    query: str
    dataset: HubspotDataset = 'contacts'


class HubspotConnector(ToucanConnector):
    _auth_flow = 'oauth2'
    auth_flow_id: Optional[str]

    data_source_model: HubspotDataSource

    def __init__(self, **kwargs) -> None:
        super().__init__(
            **{k: v for k, v in kwargs.items() if k not in OAuth2Connector.init_params}
        )
        self.__dict__['_oauth2_connector'] = OAuth2Connector(
            auth_flow_id=self.auth_flow_id,
            authorization_url=AUTHORIZATION_URL,
            scope=SCOPE,
            token_url=TOKEN_URL,
            secrets_keeper=kwargs['secrets_keeper'],
            redirect_uri=kwargs['redirect_uri'],
            config=OAuth2ConnectorConfig(
                client_id=kwargs['client_id'],
                client_secret=kwargs['client_secret'],
            ),
        )

    @staticmethod
    def get_connector_secrets_form() -> ConnectorSecretsForm:
        return ConnectorSecretsForm(
            documentation_md=(Path(os.path.dirname(__file__)) / 'doc.md').read_text(),
            secrets_schema=OAuth2ConnectorConfig.schema(),
        )

    def retrieve_tokens(self, authorization_response: str):
        """
        In the Aircall oAuth2 authentication process, client_id & client_secret
        must be sent in the body of the request so we have to set them in
        the mother class. This way they'll be added to her get_access_token method
        """
        return self.__dict__['_oauth2_connector'].retrieve_tokens(authorization_response)

    def build_authorization_url(self, **kwargs):
        return self.__dict__['_oauth2_connector'].build_authorization_url(**kwargs)

    def _get_access_token(self):
        return self.__dict__['_oauth2_connector'].get_access_token()

    def _retrieve_data(self, data_source: HubspotDataSource) -> pd.DataFrame:
        endpoint = HUBSPOT_ENDPOINTS[data_source.dataset]
        headers = {'authorization': f'Bearer {self._get_access_token()}'}
        res = requests.get(endpoint, headers=headers)

        print(res.json())

        return pd.DataFrame(res.json()['results'])['properties']
