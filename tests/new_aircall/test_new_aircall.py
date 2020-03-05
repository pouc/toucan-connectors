import pytest

from toucan_connectors.new_aircall.new_aircall_connector import New_aircallConnector, New_aircallDataSource, build_aircall_request_url, BASE_AIRCALL_API_URL


def test_get_df():
    pass

def test_set_up_request(mocker):
    """
    This tests basic request builder
    """
    fake_conn = New_aircallConnector(name="Baz")
    mah_string = fake_conn.set_up_request("foo")

    assert mah_string == "https://api.aircall.io/v1/foo"