
"""Concrete `PgSQL DAL` class for all Postgres db operations.

Connection management and transaction is handled in this class with
appropriate appropriate `QuerySet`.
"""

from contextlib import contextmanager
from typing import Any, Dict, Generator

import psycopg2

from LiveInventorySchedular.utils.data_access_layer.sql_db.dbdalbase import DBDALBase
from LiveInventorySchedular.utils.data_access_layer.sql_db.postgres.psqlqueryset import PgSQlQuerySet


class PgSQLDAL(DBDALBase):
    """Concrete `PgSQL DAL` class for all Postgres db operations. Connection
    management and transaction is handled in this class with appropriate
    appropriate `QuerySet`.

    :param dbparams: Dictionary with connection parameters required by `psycopg2.connect()`
    :type dbparams: dict, example

    ```    {
                        "host" : "database_host_name",
                        "port" : "database_port",
                        "user" : "database_user_name",
                        "password": "database_password",
                        "dbname" : "database_name",
            } ````
    """

    def __init__(self, dbparams: Dict[str, Any]) -> None:
        """Concrete `PgSQL DAL` class for all Postgres db operations.
        Connection management and transaction is handled in this class with
        appropriate appropriate `QuerySet`.

        :param dbparams: Dictionary with connection parameters required by `psycopg2.connect()`
        :type dbparams: dict, example

                ```    {
                        "host" : "database_host_name",
                        "port" : "database_port",
                        "user" : "database_user_name",
                        "password": "database_password",
                        "dbname" : "database_name",
                    } ````
        """
        super().__init__(dbparams)
        self.cursor = None
        self.connection = None

    def connect(self) -> None:
        """Connects postgresql db server with given dbparams through `psycopg2`
        driver."""
        if not self.is_connection_active:
            # Creating new connection if closed.
            self.connection = psycopg2.connect(**self.dbparams)

        if not self.is_cursor_active:
            # Creating new cursor if closed.
            self.connection.cursor()  # type: ignore

        self.connection = psycopg2.connect(**self.dbparams)  # type: ignore
        self.cursor = self.connection.cursor()  # type: ignore

    @property
    def queryset(self) -> PgSQlQuerySet:
        """Makes database connection and returns `PgSQlQuerySet` as an
        interface for all queries operations.

        :return: Returns`PgSQlQuerySet` through which queries are executed.
        :rtype: PgSQlQuerySet
        """
        self.connect()
        return PgSQlQuerySet(self.cursor)  # type: ignore

    @property
    def is_connection_active(self) -> bool:
        if self.connection:
            return not bool(self.connection.closed)
        else:
            return False

    @property
    def is_cursor_active(self) -> bool:
        if self.cursor:
            return not bool(self.cursor.closed)
        else:
            return False

    def disconnect(self) -> None:
        """Closes database cursor and connection channel."""
        if self.is_cursor_active:
            self.cursor.close()  # type: ignore

        if self.connection:
            self.connection.close()

    def reconnect(self) -> None:
        """Re-connects to `PostgreSQL` by first disconnecting and then
        connecting again.

        This should be done when connection refresh is required.
        """
        self.disconnect()
        self.connect()

    def commit(self) -> None:
        """Perfroms database commit."""
        if self.connection:
            self.connection.commit()

    def rollback(self) -> None:
        """Performs database rollback."""
        if self.connection:
            self.connection.rollback()

    @contextmanager
    def transaction(self, auto_commit: bool = True) -> Generator[PgSQlQuerySet, bool, None]:
        """DB transaction contextlib. This should be used with `with
        transaction()...` to execute queries within a transaction.

        # noqa: DAR401
        :param auto_commit: Boolean value whether to commit or not after transaction end,
                            defaults to True
        :type auto_commit: bool
        :yields: `PgSQlQuerySet`
            E.g:
                ```
                db = PgSQLDal({....})
                with db.transaction(auto_commit=False) as conn:
                    try:
                        result = conn.execute_non_query('DELETE FROM users where id = 1')
                        db.commit()
                    except:
                        db.rollback()

                ```

        You don't have to manage connection open and close while using transaction.
        """

        queryset = self.queryset
        try:
            yield queryset
        except Exception as e:
            if auto_commit:
                self.rollback()

            del queryset
            raise e
        finally:
            if auto_commit:
                self.commit()
            self.disconnect()
