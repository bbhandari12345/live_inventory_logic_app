
"""Collection of modules and subpackages dealing with database server and Data
Access Layers.

Everything that fetches and push data through database server required
in application runtime goes right inside this  package.

This init module contains DB DAL Factory Method.
"""
from typing import ClassVar
from .postgres.pgsqldal import PgSQLDAL
from .dbdalbase import DBDALBase


class DBEngineFactory:
    """Factory to get appropirate DB DAL accroding to supplied engine type.

    :param config: dict config for db connection. Connection parameters could differ
                    as per db server type.
    :param engine_type: value to denote which server to connect to. For now only
                        `DBEngineFactory.POSTGRES` is available.

    :type config: dict
    :type engine_type: int
    """

    POSTGRES: ClassVar[int] = 1

    @staticmethod
    def get_db_engine(config, engine_type: int) -> DBDALBase:
        """Factory static method to get appropirate DB DAL accroding to
        supplied engine type.

        :param config: dict config for db connection. Connection parameters could differ
                         as per db server type.
        :param engine_type: value to denote which server to connect to. For now only
                             `DBEngineFactory.POSTGRES` is available.

        :type config: dict
        :type engine_type: int

        :raises ValueError: Raised when `engine_type` isn't recognized

        :return: Concrete class object of DBDALBase
        :rtype: DBDALBase
        """
        if engine_type == DBEngineFactory.POSTGRES:
            return PgSQLDAL(config)
        else:
            raise ValueError(
                'Only `DBEngineFactory.POSTGRES` is allowed as engine_type.')
