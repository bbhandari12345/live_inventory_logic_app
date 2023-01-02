
# noqa: DAR101
import pytest
import os
from psycopg2.extensions import connection, cursor

from utils.data_access_layer import PgSQLDAL
from utils.data_access_layer.sql_db.postgres.psqlqueryset import PgSQlQuerySet


class TestPgSQLDAL:
    """Test Class to run unit tests for `PgSQLDAL`.

    All test cases for this `PgSQLDAL` should go here
    """
    @pytest.fixture(scope='module')
    def get_dbparams_fx(self):
        dbparams = {
            'host': 'localhost',
                    'port': 5432,
                    'user': os.environ.get('utils_test_db_user', 'admin'),
                    'password': os.environ.get('utils_test_db_pass', 'password'),
                    'dbname': 'postgres'
        }
        yield dbparams
        del dbparams

    @pytest.fixture(scope='function')
    def get_pgsqldal_fx(self, get_dbparams_fx):
        """Fixture: Provides `PgSQLDAL` instance configured with `get_dbparams_fx` config dict.
        Calls disconnect after yeild as a teardown process.
        """
        db_engine = PgSQLDAL(get_dbparams_fx)
        yield db_engine
        db_engine.disconnect()
        del db_engine

    def test_connection_before_connecting(self, get_pgsqldal_fx):
        """Tests for connection to be None before connecting to db."""
        assert get_pgsqldal_fx.connection == None

    def test_cursor_before_connecting(self, get_pgsqldal_fx):
        """Tests for cursor to be None before connecting to db."""
        assert get_pgsqldal_fx.cursor == None

    def test_connection_after_connecting(self, get_pgsqldal_fx):
        """Tests for connecttion to be instance of psycopg2 connection after
        connecting to db."""
        get_pgsqldal_fx.connect()
        db_connection = get_pgsqldal_fx.connection

        assert isinstance(db_connection, connection)

    def test_cursor_after_connecting(self, get_pgsqldal_fx):
        """Tests for cursor to be instance of psycopg2 cursor after connecting
        to db."""
        get_pgsqldal_fx.connect()
        db_cursor = get_pgsqldal_fx.cursor

        assert isinstance(db_cursor, cursor)

    def test_cursor_status_after_diconnect(self, get_pgsqldal_fx):
        """Tests for cursor status to be false after disconnecting."""
        get_pgsqldal_fx.connect()
        get_pgsqldal_fx.disconnect()
        assert get_pgsqldal_fx.is_cursor_active == False

    def test_connection_status_after_diconnect(self, get_pgsqldal_fx):
        """Tests for connection to be false after disconnecting."""
        get_pgsqldal_fx.connect()
        get_pgsqldal_fx.disconnect()
        assert get_pgsqldal_fx.is_connection_active == False

    def test_connection_status_after_connect(self, get_pgsqldal_fx):
        """Tests for conection status to be true after connecting."""
        get_pgsqldal_fx.connect()
        assert get_pgsqldal_fx.is_connection_active == True

    def test_cursor_status_after_connect(self, get_pgsqldal_fx):
        """Tests for cursor status to be true after connecting."""
        get_pgsqldal_fx.connect()
        assert get_pgsqldal_fx.is_cursor_active == True

    def test_queryset_after_connection(self, get_pgsqldal_fx):
        """Tests for queryset to be instance of `PgSQlQuerySet`"""
        queryset = get_pgsqldal_fx.queryset
        assert isinstance(queryset, PgSQlQuerySet)

    def test_db_transaction_with_autocommit(self, get_pgsqldal_fx):
        """Tests for connection and cursor status iside and outside transaction
        to verify db transaction workflow."""
        db = get_pgsqldal_fx
        with db.transaction(auto_commit=True) as qryset:
            assert db.is_connection_active == True
            assert db.is_cursor_active == True
            assert isinstance(qryset, PgSQlQuerySet)

        assert db.is_cursor_active == False
        assert db.is_connection_active == False

    def test_rollback_before_connecting(self, get_pgsqldal_fx):
        """Tests for rollback worklow before connecting to db.

        *This is an incomplete implementation of rollback test.*
        *We are just assuming the inner driver rollback functions as intented.*

        *A better approach would be make changes in db and  perform rollback and then
        check if the changes has persisted or not.*
        """
        get_pgsqldal_fx.rollback()

    def test_rollback_after_connecting(self, get_pgsqldal_fx):
        """Tests for rollback after connecting to db."""
        get_pgsqldal_fx.connect()
        get_pgsqldal_fx.rollback()

    def test_db_connection_reconnect(self, get_pgsqldal_fx):
        """Tests for db connection and cursor status before and after
        performing reconnect when reconnect is called after connecting to
        db."""

        assert get_pgsqldal_fx.is_connection_active == False
        assert get_pgsqldal_fx.is_cursor_active == False

        get_pgsqldal_fx.connect()

        assert get_pgsqldal_fx.is_connection_active == True
        assert get_pgsqldal_fx.is_cursor_active == True

        get_pgsqldal_fx.reconnect()

        assert get_pgsqldal_fx.is_connection_active == True
        assert get_pgsqldal_fx.is_cursor_active == True

    def test_db_connection_reconnect_after_diconnecting(self, get_pgsqldal_fx):
        """Tests for db connection and cursor status before and after
        performing reconnect when reconnect is called after disconnecting to
        db.

        [EDGE CASE]
        """
        assert get_pgsqldal_fx.is_connection_active == False
        assert get_pgsqldal_fx.is_cursor_active == False

        # connecting
        get_pgsqldal_fx.connect()

        assert get_pgsqldal_fx.is_connection_active == True
        assert get_pgsqldal_fx.is_cursor_active == True

        # disconnecting
        get_pgsqldal_fx.disconnect()

        assert get_pgsqldal_fx.is_connection_active == False
        assert get_pgsqldal_fx.is_cursor_active == False

        # reconnecting
        get_pgsqldal_fx.reconnect()

        assert get_pgsqldal_fx.is_connection_active == True
        assert get_pgsqldal_fx.is_cursor_active == True
