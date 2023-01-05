
"""Concrete `PgSQL ResultSet` class for all query result processing.

DB query results should be processed here.
"""

from typing import Dict, List, Type, Any, Union

import pandas as pd
import psycopg2
from psycopg2.extensions import cursor as c


from LiveInventoryDispatcher.utils.data_access_layer.sql_db.resultsetbase import ResultSet


class PgSQlResultSet(ResultSet):
    """Abstract base class for `Database ResultSet`. This class process the
    query result.

    :param cursor: Database `cursor` through which query is executed and
    results are fetched.
    :type cursor: psycopg2.cursor
    """

    def __init__(self, cursor: Type[c]) -> None:
        super().__init__(cursor)
        self._raw_data = self.raw

    def pre_process(self) -> List[Dict[str, Any]]:
        """Method to preprocess raw data fetched from cursor.

        Parses cursor raw data to dictionary.
        :return: list of dictionaries
        :rtype: list
        """
        # self._raw_data could be none if not result returning query is executed as result returning
        # this conditional is for fail safe.
        if self._raw_data is not None:
            cursor_data = self._raw_data
            columns = cursor_data[0]
            rows_data = cursor_data[1]

            result = [
                {
                    columns[index][0]:column for
                    index, column in enumerate(value)
                }
                for value in rows_data
            ]

            return result
        else:
            return []

    def to_list(self) -> List[Dict[str, Any]]:
        """Method to parse cursor data to list dictionary.

        :return: list of dictionaries processed from raw cursor data.
        :rtype: list
        """
        return self.pre_process()

    def to_df(self) -> pd.DataFrame:
        """Method to parse cursor data to `pandas.DataFrame`
        :return: cursor data parsed to pandas dataframe.
        :rtype: pandas.DataFrame
        """
        return pd.DataFrame(self.to_list())

    def fetch_data(self) -> Union[None, tuple]:
        """Private method to fetch cursor description (columns data) and cursor
        data Cursor is destroyed once accessed so this method should be *used
        only once*.

        :return: Named Tuple for column description along with list of tuples with column data.
                refer to https://www.psycopg.org/docs/cursor.html#cursor.description
                Index 0 has column name data where Index 1 has row data.

        :rtype: tuple
        """
        try:
            return self.cursor.description, self.cursor.fetchall()
        except psycopg2.ProgrammingError:
            # Programming Error is caught when cursor has no data for fetch.
            # This maybe caused when execute_query is used for non result returning query.
            return None

    @property
    def raw(self) -> Union[None, tuple]:
        """Property to get raw data from cursor. First the raw data is fetched
        from cursor and then returned.

        :return: Named Tuple for column description along with list of tuples with column data.
                refer to https://www.psycopg.org/docs/cursor.html#cursor.description

                Index 0 has column name data where Index 1 has row data.

        :rtype: tuple
        """
        return self.fetch_data()

    @property
    def rowcount(self) -> int:
        """Property to get cursor row count.

        *Note*: There could be cases where -1 is returned as a rowcount please refer to
        https://www.psycopg.org/docs/cursor.html#cursor.rowcount

        :return: cursor rowcount
        :rtype: int
        """
        return self.cursor.rowcount

    @property
    def query(self) -> str:
        """Property to get query executed in the given cursor.

        :return: executed query in the given cursor
        :rtype: str
        """

        return self.cursor.query

    def __len__(self) -> int:
        """Length of result fetched from query.

        Use of `len(PgSQlResultSet())` is encouraged instead of rowcount. rowcount is primitive
        implementation of cursor so it may differ according to db driver/db api version.

        :return: row length in int
        :rtype: int
        """

        # Index 0 has column name data where Index 1 has row data'
        if self._raw_data:
            return len(self._raw_data[1])
        else:
            return 0

    def __repr__(self):
        """Repr.

        #noqa: DAR201:
        """
        return f'<PgSQlResultSet> for {self.query}'
