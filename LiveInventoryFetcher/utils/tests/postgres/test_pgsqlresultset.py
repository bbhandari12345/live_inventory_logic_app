
from unittest import mock

import pandas as pd
import psycopg2
import pytest

from LiveInventoryFetcher.utils.data_access_layer.sql_db.postgres.psqlresultset import PgSQlResultSet


class TestPgSQlResultSet:
    """Test Class to run unit tests for `PgSQlResultSet`.

    All test cases for this `PgSQlResultSet` should go here.
    """

    def fetcherror_side_effect(self):
        raise psycopg2.ProgrammingError('no results to fetch')

    @pytest.fixture(scope='module')
    @mock.patch('psycopg2.connect')
    def returning_cursor_fx(self, mock_connect):
        mocked_cursor = mock_connect.connect().cursor.return_value
        mocked_cursor.description = [['name'], ['phone']]
        mocked_cursor.fetchall.return_value = [
            ('Smegaul', '347539593'), ('Gollum', '347539593')]
        mocked_cursor.rowcount = 2
        mocked_cursor.execute.call_args == "SELECT name,phone FROM customer;'"

        return mocked_cursor

    @pytest.fixture(scope='module')
    @mock.patch('psycopg2.connect')
    def non_returning_cursor_fx(self, mock_connect):
        mocked_cursor = mock_connect.connect().cursor.return_value
        mocked_cursor.description = None
        mocked_cursor.fetchall.side_effect = self.fetcherror_side_effect
        mocked_cursor.rowcount = -1
        mocked_cursor.execute.call_args == "DELETE customer where name='Smegaul';'"

        return mocked_cursor

    @pytest.fixture(scope='module')
    def query_result_fx(self):
        return [
            {'name': 'Smegaul', 'phone': '347539593'},
            {'name': 'Gollum', 'phone': '347539593'}
        ]

    @pytest.fixture(scope='module')
    def df_result_fx(self, query_result_fx):
        return pd.DataFrame(query_result_fx)

    def test_resultset_obj_int(self, non_returning_cursor_fx, returning_cursor_fx):
        """Tests creation of `PgSQlResultSet` object through mocked cursor."""
        resultset_non_returning = PgSQlResultSet(non_returning_cursor_fx)
        resultset_returning = PgSQlResultSet(returning_cursor_fx)

    def test_resultset_pre_process(self, non_returning_cursor_fx, returning_cursor_fx, query_result_fx):
        """Tests pre_process result."""
        resultset_non_returning = PgSQlResultSet(non_returning_cursor_fx)
        resultset_returning = PgSQlResultSet(returning_cursor_fx)

        assert resultset_returning.pre_process() == query_result_fx
        assert resultset_non_returning.pre_process() == []

    def test_resultset_rowcount_returning_query(self, returning_cursor_fx):
        """Tests rowcount for the given result returning resultset.

        A postive integer should be returned.
        """
        resultset_returning = PgSQlResultSet(returning_cursor_fx)
        assert resultset_returning.rowcount == 2

    def test_resultset_rowcount_non_returning_query(self, non_returning_cursor_fx):
        """Tests rowcount for the given result not returning resultset.

        A negative -1 integer should be returned.
        """
        resultset_non_returning = PgSQlResultSet(non_returning_cursor_fx)
        assert resultset_non_returning.rowcount != 2
        assert resultset_non_returning.rowcount == -1

    def test_resultset_length_for_returning_query(self, returning_cursor_fx):
        """Tests len() of result returning resultset.

        It must match with rowcount and query result lenght
        """

        resultset_returning = PgSQlResultSet(returning_cursor_fx)
        assert len(resultset_returning) == 2
        assert resultset_returning.rowcount == len(resultset_returning)

    def test_resultset_length_for_non_returning_query(self, non_returning_cursor_fx):
        """Tests len() for result not returning resultset.

        It must be 0.
        """
        resultset_non_returning = PgSQlResultSet(non_returning_cursor_fx)
        assert len(resultset_non_returning) == 0

    def test_fetch_data(self, non_returning_cursor_fx, returning_cursor_fx):
        """Tests for resultset fetch_data() method.

        A non result returning result set must return None A result
        returning result set must return a tuple where index 0 is
        description and index 1 is values
        """
        resultset_non_returning = PgSQlResultSet(non_returning_cursor_fx)
        resultset_returning = PgSQlResultSet(returning_cursor_fx)

        assert resultset_non_returning.fetch_data() == None
        assert resultset_returning.fetch_data() == (
            [['name'], ['phone']],
            [('Smegaul', '347539593'), ('Gollum', '347539593')]
        )

    def test_to_list(self, non_returning_cursor_fx, returning_cursor_fx, query_result_fx):
        """Tests for resultset to_list() method.

        A non result returning resultset must return None A result
        returning resultset must return a list of dict.
        """
        resultset_non_returning = PgSQlResultSet(non_returning_cursor_fx)
        resultset_returning = PgSQlResultSet(returning_cursor_fx)

        assert resultset_non_returning.to_list() == []
        assert resultset_returning.to_list() == query_result_fx

    def test_to_df(self, non_returning_cursor_fx, returning_cursor_fx, df_result_fx):
        """Tests for resultset to_df() method.

        A non result returning resultset must return an empty dataframe.
        A result returning resultset must return a dataframe of query
        result.
        """
        resultset_non_returning = PgSQlResultSet(non_returning_cursor_fx)
        resultset_returning = PgSQlResultSet(returning_cursor_fx)

        assert resultset_returning.to_df().equals(df_result_fx)
        assert resultset_non_returning.to_df().equals(pd.DataFrame([]))
