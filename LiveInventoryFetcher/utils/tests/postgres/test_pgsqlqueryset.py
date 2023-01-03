
from unittest import mock

import pytest

from LiveInventoryFetcher.utils.data_access_layer.sql_db.postgres.psqlqueryset import PgSQlQuerySet
from LiveInventoryFetcher.utils.data_access_layer.sql_db.postgres.psqlresultset import PgSQlResultSet


class TestPgSQlQuerySet:
    """Test Class to run unit tests for `PgSQlQuerySet`.

    All test cases for this `PgSQlQuerySet` should go here.
    """

    @pytest.fixture(scope='function')
    @mock.patch('psycopg2.connect')
    def get_queryset_fx(self, mock_connect):
        mocked_cursor = mock_connect.connect().cursor.return_value
        queryset = PgSQlQuerySet(mocked_cursor)
        return queryset

    @pytest.fixture(scope='module')
    def query_fx(self):
        query = 'INSERT INTO TABLE(col1,col2,col3)VALUES(%s,%s,%s)'
        return query

    @pytest.fixture(scope='module')
    def query_data_fx(self):
        data = ((1), (True), ('This is a cell value'))
        return data

    def test_execute_non_query(self, get_queryset_fx, query_fx, query_data_fx):
        """Test for execute_non_query, where query return values aren't
        expected.

        None should be returned.
        """
        result = get_queryset_fx.execute_non_query(query_fx, query_data_fx)
        assert result == None

    def test_execute_query(self, get_queryset_fx, query_fx, query_data_fx):
        """Test for execute_non_query, where query return values are expected.

        Instance of `PgSQlResultSet` should be returned.
        """
        result = get_queryset_fx.execute_query(query_fx, query_data_fx)
        assert isinstance(result, PgSQlResultSet)
