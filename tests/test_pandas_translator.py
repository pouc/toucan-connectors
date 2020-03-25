import pytest

from toucan_connectors.pandas_translator import (
    permission_condition_to_pandas_clause,
    permission_conditions_to_pandas_query,
)


def test_permission_condition_to_pandas_clause():
    # works with list
    c = {'column': 'city name', 'operator': 'in', 'value': ['Paris', 'London']}
    assert permission_condition_to_pandas_clause(c) == "`city name` in ['Paris', 'London']"
    # works with numbers
    c = {'column': 'population', 'operator': 'eq', 'value': 42}
    assert permission_condition_to_pandas_clause(c) == '`population` == 42'
    # override enclosing field char
    assert permission_condition_to_pandas_clause(c, "'") == "'population' == 42"
    # put strings between ''
    c = {'column': 'country', 'operator': 'eq', 'value': 'France'}
    assert permission_condition_to_pandas_clause(c) == "`country` == 'France'"
    # looking for 100% code coverage
    with pytest.raises(KeyError):
        permission_condition_to_pandas_clause({'column': 'population', 'operator': 'eq'})
    with pytest.raises(KeyError):
        permission_condition_to_pandas_clause({'column': 'population', 'value': 42})
    with pytest.raises(KeyError):
        permission_condition_to_pandas_clause({'operator': 'eq', 'value': 42})
    with pytest.raises(ValueError):
        permission_condition_to_pandas_clause(
            {'column': 'population', 'operator': 'unsupported', 'value': 42}
        )
    with pytest.raises(Exception):
        permission_condition_to_pandas_clause(
            {'column': 'population', 'operator': 'matches', 'value': 42}
        )


def test_permission_conditions_to_pandas_query():
    c = {
        'and': [
            {'column': 'country', 'operator': 'eq', 'value': 'France'},
            {
                'or': [
                    {'column': 'city name', 'operator': 'in', 'value': ['Paris', 'London']},
                    {'column': 'population', 'operator': 'eq', 'value': 42},
                ]
            },
        ]
    }
    assert (
        permission_conditions_to_pandas_query(c)
        == "(`country` == 'France' and (`city name` in ['Paris', 'London'] or `population` == 42))"
    )
    # Invalid and/or condition list
    with pytest.raises(ValueError):
        permission_conditions_to_pandas_query({'and': 1})
    with pytest.raises(ValueError):
        permission_conditions_to_pandas_query({'or': 1})
