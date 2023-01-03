
"""Base Abstract class for `Database ResultSet`.

DB query result are processed here.

All concrete class should inherit this base class and override
it public methods for implementation.

TODO:
    1. Refractor this abstract class to iterable/generator class
    2. Introduce abstractmethods to iterate through cursor.
    3. Provide more public method to interact with cursor.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List

import pandas as pd


class ResultSet(ABC):
    """Abstract base class for `Database ResultSet`. This class process the
    query result.

    :param cursor: Database `cursor` through which query is executed and
    results are fetched.
    :type cursor: any
    """

    def __init__(self, cursor: Any) -> None:
        super().__init__()
        self.cursor = cursor

    @abstractmethod
    def pre_process(self) -> Any:
        """Abstract method to preprocess raw data fetched from cursor."""
        pass

    @abstractmethod
    def to_list(self) -> List[Dict]:
        """Abstract method to parse cursor data to list of dictinary."""
        pass

    @abstractmethod
    def to_df(self) -> pd.DataFrame:
        """Abstract method to parse cursor data to `pandas.DataFrame`"""
        pass

    @property
    @abstractmethod
    def raw(self) -> Any:
        """Abstract property to get raw data from cursor."""
        pass

    @property
    @abstractmethod
    def rowcount(self) -> int:
        """Abstract property to get cursor row count."""
        pass

    @property
    @abstractmethod
    def query(self) -> str:
        """Abstract property to get query executed in the given cursor."""
        pass

    @abstractmethod
    def __len__(self) -> int:
        """Lenght of result fetched from query."""
        pass
