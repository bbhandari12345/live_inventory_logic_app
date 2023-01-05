
"""Base Abstract class for `Database Queryset`.

DB query should happen through this class and all query related methods
goes here.

All concrete class should inherit this base class and override
it public methods for implementation.
"""

from abc import ABC, abstractmethod
from typing import Any, Optional, Sequence

from LiveInventoryDispatcher.utils.data_access_layer.sql_db.resultsetbase import ResultSet


class QuerySet(ABC):
    """Abstract Base class for Database Query operations.

    :param cursor: Database `cursor` through which query is executed and
    results are fetched.
    :type cursor: any
    """

    def __init__(self, cursor: Any) -> None:
        """Constructor for QuerySet.

        :param cursor: Database `cursor` through which query is executed and
            results are fetched.
        :type cursor: Any
        """
        super().__init__()
        self.cursor = cursor

    @abstractmethod
    def execute_non_query(self, query: str, data: Optional[Sequence] = None) -> None:
        """Abstract Method for executing result not returning query.

        :param query: Query string to execute,
        :type query: str

        :param data: Data associated with the query. e.g: Data for delete statement.
        :type data: any
        """
        pass

    @abstractmethod
    def execute_query(self, query: str, data: Optional[Sequence] = None) -> ResultSet:
        """Abstract Method for executing result returning query.

        :param query: Query string to execute,
        :type query: str

        :param data: Data associated with the query. e.g: Data for insert statement.
        :type data: any
        """
        pass
