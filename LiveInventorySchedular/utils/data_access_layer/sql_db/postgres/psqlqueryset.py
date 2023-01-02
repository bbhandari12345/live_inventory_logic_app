
"""Concrete `PgSQL Queryset` class for all query operations.

DB query should happens through this
"""

from typing import Optional, Sequence, Type

from psycopg2.extensions import cursor as c

from LiveInventorySchedular.utils.data_access_layer.sql_db.postgres.psqlresultset import PgSQlResultSet
from LiveInventorySchedular.utils.data_access_layer.sql_db.querysetbase import QuerySet


class PgSQlQuerySet(QuerySet):
    """Concrete `PgSQL Queryset` class for all query operations.
    :param:cursor: `psycopg2` cursor for query execution
    :type cursor: `<class 'psycopg2.extensions.cursor'>`
    """

    def __init__(self, cursor: Type[c]) -> None:
        """Constructor for `PgSQlQuerySet`
        :param:cursor: `psycopg2` cursor for query execution
        :type cursor: `<class 'psycopg2.extensions.cursor'>`
        """
        super().__init__(cursor)
        self.query = None

    def execute_non_query(self, query: str, data: Optional[Sequence] = None) -> None:
        """Method for executing result not returning query.

        :param query: Query string to execute,
        :type query: str

        :param data: Data assocaited with the query. e.g: Data for delete statement.
        :type data: tuple, optional
        """
        self.query = self.cursor.mogrify(query, data)
        self.cursor.execute(query, data)

    def execute_query(self, query: str, data: Optional[Sequence] = None) -> PgSQlResultSet:
        """Method for executing result returning query.

        :param query: Query string to execute,
        :type query: str

        :param data: Data assocaited with the query. e.g: Data for insert statement.
        :type data: tuple, optional

        :return: `PgSQlResultSet` query result.
        :rtype: PgSQlResultSet
        """
        self.query = self.cursor.mogrify(query, data)
        self.cursor.execute(query, data)
        return PgSQlResultSet(self.cursor)
