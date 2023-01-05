"""Base Abstract class for `Database` Data Acess layer.

All concrete calss should inherit this base class and override it public
methods for implementation.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Callable

from LiveInventoryDispatcher.utils.data_access_layer.sql_db.querysetbase import QuerySet


class DBDALBase(ABC):
    """Abstract Base class for `Database` Data Acess layer.

    :param dbparams: `Database` connection parameters. Should be in dict.
    :type dbparams: dict

    :param uri: `Database` connection URI
    :type uri: str, optional
    """
    transaction: Callable

    def __init__(self, dbparams: Dict[str, Any] = None, uri: str = None) -> None:
        """Constructor for Abstract Class.

        :param dbparams: `Database` connection parameters. Should be in dict.
        :type dbparams: dict

        :param uri: `Database` connection URI,
                    optional
        :type uri: str
        """
        super().__init__()
        self.dbparams = dbparams
        self.uri = uri

    @abstractmethod
    def connect(self) -> None:
        """Abstract method to prepare and make `Database` connection."""
        pass

    @property
    @abstractmethod
    def queryset(self) -> QuerySet:
        """Abstract property to make connection, get cursor and provide.

        appropriate concrete`QuerySet` as an interface to channel an all
        queries through it.
        """
        pass

    @property
    @abstractmethod
    def is_connection_active(self) -> bool:
        """Abstract property to provide connection status.

        Either closed or open as a Boolean value.
        """
        pass

    @property
    @abstractmethod
    def is_cursor_active(self) -> bool:
        """Abstract property to provide cursor status.

        Either closed or open as a Boolean value.
        """
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Abstract method to disconnect from active `Database` connection."""
        pass

    @abstractmethod
    def reconnect(self) -> None:
        """Abstract method to first disconnect and then again connect to
        `Database`.

        This is required in scenarios when we need to refresh our
        database connection.
        """
        pass

    @abstractmethod
    def commit(self) -> None:
        """Abstract method to explicityly perform database `commit`"""
        pass

    @abstractmethod
    def rollback(self) -> None:
        """Abstract method to explicityly perform database `rollback`"""
        pass
