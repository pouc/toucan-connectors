from abc import ABC, abstractmethod
from time import time
from typing import Any
from urllib import parse as url_parse

from authlib.integrations.requests_client import OAuth2Session


class SecretsKeeper(ABC):
    @abstractmethod
    def save(self, key: str, value):
        """
        Save secrets in a secrets repository
        """

    @abstractmethod
    def load(self, key: str) -> Any:
        """
        Load secrets from the secrets repository
        """


class OAuth2Connector:
    init_params = ['client_secret', 'client_id', 'redirect_uri', 'secrets_keeper']

    def __init__(
        self,
        name: str,
        authorization_url: str,
        scope: str,
        client_id: str,
        client_secret: str,
        secrets_keeper: SecretsKeeper,
        redirect_uri: str,
        token_url: str,
    ):
        self._connector_name = name
        self.authorization_url = authorization_url
        self.scope = scope
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.secrets_keeper = secrets_keeper
        self.token_url = token_url

    def build_authorization_url(self) -> str:
        """Build an authorization request that will be sent to the client."""
        client = OAuth2Session(
            client_id=self.client_id,
            client_secret=self.client_secret,
            redirect_uri=self.redirect_uri,
            scope=self.scope,
        )
        uri, state = client.create_authorization_url(self.authorization_url)

        self.secrets_keeper.save(self._connector_name, {'state': state})
        return uri

    def retrieve_tokens(self, authorization_response: str, **kwargs):
        url = url_parse.urlparse(authorization_response)
        url_params = url_parse.parse_qs(url.query)
        client = OAuth2Session(
            client_id=self.client_id,
            client_secret=self.client_secret,
            redirect_uri=self.redirect_uri,
        )
        assert self.secrets_keeper.load(self._connector_name)['state'] == url_params['state'][0]
        token = client.fetch_token(
            self.token_url, authorization_response=authorization_response, **kwargs
        )
        self.secrets_keeper.save(self._connector_name, token)

    def get_access_token(self) -> str:
        """
        Returns the access_token to use to access resources
        If necessary, this token will be refreshed
        """
        token = self.secrets_keeper.load(self._connector_name)

        if 'expires_at' in token and token['expires_at'] < time():
            if 'refresh_token' not in token:
                raise NoOAuth2RefreshToken
            client = OAuth2Session(
                client_id=self.client_id,
                client_secret=self.client_secret,
            )
            new_token = client.refresh_token(self.token_url, refresh_token=token['refresh_token'])
            self.secrets_keeper.save(self._connector_name, new_token)
        return self.secrets_keeper.load(self._connector_name)['access_token']



class NoOAuth2RefreshToken(Exception):
    """
    Raised when no refresh token is available to get new access tokens
    """
