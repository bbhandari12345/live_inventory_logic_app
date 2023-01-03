"""
ORM base class for performing MRC database actions
"""

from abc import ABC, abstractmethod
from typing import Any
from marshmallow import fields, ValidationError, EXCLUDE
import logging
from LiveInventoryFetcher.common_utils.db_utils import (insert_data, update_data, delete_rows, get_one, get_all,
                                                          upsert_data, upsert_bulk_data, get_all_netsuite_items)

# get the logger instance
logger = logging


class LIOrmBase(ABC):
    """Base Class to perform db actions for the given table.

    Along with dynamic schema validation and dump for data in and out.
    This class performs saveCRUD operations
    :params src_data: dict or list of dict. Data to be loaded on schema for
                        validation other operations.
    :type src_data: List[dict] or dict
    """

    def __init__(self, src_data=None):
        """Perform sql_db actions for the given table.

        With dynamic schema validation and dump for data in and out. This class
        performs save CRUD operations in sql_db.

        :params src_data: dict or list of dict. Data to be loaded on schema for validation
                            other operations.
        :type src_data: List[dict] or dict
        """
        self.src_data = src_data

        self.loaded_data = None
        self.dumped_data = None

        self.is_loaded = False
        self.is_dumped = False

    @property
    @classmethod
    @abstractmethod
    def __table_name__(cls):
        """Class attribute for tablename."""
        pass

    @property
    @classmethod
    @abstractmethod
    def __schema__(cls):
        """Class attribute for table schema object."""
        pass

    @classmethod
    def get_schema(cls):
        """Return schema used in class.

        :returns: Table Schema
        :rtype: marshmallow.schema
        """
        return cls.__schema__

    @classmethod
    def get_table(cls):
        """Return tablename used in class.

        :returns: tablename
        :rtype: str
        """
        return cls.__table_name__

    @classmethod
    def get_dump_only_fields(cls) -> list:
        """Return schema fields defined as dump only.

        :returns: list of dump only fields name.
        :rtype:list
        """
        dump_only_fields = []
        declared_fields = getattr(cls.get_schema(), 'declared_fields')  # noqa: B009
        for key, value in declared_fields.items():
            if not isinstance(value, fields.Method):
                if getattr(value, 'dump_only'):  # noqa: B009
                    dump_only_fields.append(key)

        return dump_only_fields

    def check_src_data(self):
        """Check if src data loaded or not.

        :raises ValueError: Raised when src_data is None.
        """
        if self.src_data is None:
            raise ValueError('src_data must not be None.')

    def load(self, unknown=EXCLUDE, **kwargs):
        """Load src data to given schema along followed by validation.

        :params kwargs: keyword arguments supplied to marshmallow schema load method.
        :type kwargs: dict
        :returns: loaded dict
        :rtype: dict
        """
        self.check_src_data()

        logger.info(f'Loading data to {self.get_table()} schema.')
        logger.debug(f'Data for loading: {self.src_data}')
        self.is_loaded = True
        try:
            if isinstance(self.src_data, list):
                logger.debug('Got a list of data. Preparing to call schema load with many=True')
                self.loaded_data = self.get_schema().load(self.src_data, unknown=unknown, many=True, **kwargs)

            if isinstance(self.src_data, dict):
                logger.debug('Got a single dict data. Preparing to call schema load with many=False')
                self.loaded_data = self.get_schema().load(self.src_data, unknown=unknown, many=False, **kwargs)
        except ValidationError as err:
            logger.error("Encountered marhsmallow validation error", exc_info=True)
            raise err

        return self.loaded_data

    def dump(self, **kwargs):
        """Dump schema data after proper schema build.

        :params kwargs: keyword arguments supplied to marshmallow schema dump method.
        :type kwargs: dict
        :returns: dynamically created schema dict list of dict values or single dict.
        :rtype: list, dict

        :raises Exception: Raised when marshmallow dump raises any exception.
        """
        self.check_src_data()

        logger.info(f'Dumping data of {self.get_table()}  schema.')
        self.is_dumped = True
        try:
            if isinstance(self.src_data, list):
                logger.debug('Got a list of data. Preparing to calll schema load with many=True')
                self.dumped_data = self.get_schema().dump(self.src_data, many=True, **kwargs)

            if isinstance(self.src_data, dict):
                logger.debug('Got a single dict data. Preparing to calll schema load with many=False')
                self.dumped_data = self.get_schema().dump(self.src_data, many=False, **kwargs)

        except Exception as e:
            logger.error('Error in data dump', exc_info=True)
            raise e
        return self.dumped_data

    # @tracelog(logger)
    def save(self):
        """Save src data to database.

        :raises NotImplementedError: Raised when src_data is a list of records as i.e.
                                    as of now bulk insert isn't implemented.

        :raises ValueError: Raised when update() is called before loading or dumping data.
        """
        self.check_src_data()
        logger.debug(f'Saving data: {self.src_data}')

        if isinstance(self.src_data, list) and len(self.src_data) > 0:
            logger.warning('Save for Multiple instances not implemented.')
            raise NotImplementedError('Save for Multiple instances not implemented yet.')

        if not self.is_loaded:
            logger.critical('Data should be loaded first to save.')
            raise ValueError('You can only save loaded data. Perform .load() before saving.')
        logger.info('Executing insert_data')

        self.loaded_data = insert_data(self.get_table(), self.src_data,
                                       include=self.get_dump_only_fields())[0]

    def update(self, row_value: Any = None, identifier: str = 'id'):
        """Update `src_data` in database.

        :params row_value: where clause filter value.
        :type row_value: Any

        :params identifier: where clause filter column name. defaults to `id`.
        :type identifier:str

        :raises NotImplementedError: Raised when src_data is a list of records as i.e.
                                    as of now bulk insert isn't implemented.

        :raises ValueError: Raised when update() is called before loading or dumping data.
        """
        self.check_src_data()
        if isinstance(self.src_data, list) and len(self.src_data) > 1:
            logger.warning('Update for Multiple instances not implemented.')
            raise NotImplementedError('Update for Multiple instances not implemented yet.')

        if self.is_loaded is False and self.is_dumped is False:
            logger.critical('Data should be loaded or dumped first to update.')
            raise ValueError('.load() or .dump() must be called before update')

        if row_value is None:
            if isinstance(self.loaded_data, list):
                row_value = self.loaded_data[0][identifier]
            else:
                row_value = self.loaded_data[identifier]

        logger.debug(f'Updating with identifier {identifier}'
                     f'with row value {row_value} '
                     f'data for {self.loaded_data} ')

        logger.info('Executing update_data.')
        update_data(self.get_table(), self.loaded_data, identifier, row_value)

    def delete(self, row_value: Any = None, identifier: str = 'id') -> None:
        """Delete row from database.

        :params row_value: where clause filter value.
        :type row_value: Any

        :params identifier: where clause filter column name. defaults to `id`..
        :type identifier:str

        :raises NotImplementedError: Raised when src_data is a list of records as i.e.
                                    as of now bulk delete isn't implemented.
        """
        if isinstance(self.src_data, list) and len(self.src_data) > 1:
            logger.warning('Update for Multiple instances not implemented.')
            raise NotImplementedError('Delete for Multiple instances not implemented yet.')

        if row_value is None:
            if isinstance(self.src_data, list):
                row_value = self.src_data[0][identifier]
            else:
                self.check_src_data()
                row_value = self.src_data[identifier]

        logger.debug(f'Deleting with identifier {identifier}'
                     f'with row value {row_value}'
                     f'data for {self.src_data} ')

        logger.info('Executing delete_rows.')
        delete_rows(self.get_table(), identifier, row_value)

    def all(self, current_page: int, per_page: int) -> dict:  # noqa: A003
        """Return all records with pagination meta info.

        :param current_page: page number to fetch result from.
        :type current_page: int

        :param per_page: total number of records to fetch in a page.
        :type per_page:int

        :returns: schema dump along with meta object with pagination info.
        :rtype: dict
        """
        current_page = abs(current_page)
        per_page = per_page

        # offset index starts from 0 but pagination starts from 1
        # when api call is made /path/?page=1 should return first page.
        # where this page = 1 should be transformed to match offset 0 i.e.
        # page 1 = offset 0 (sql)
        current_page = current_page if current_page == 0 else current_page - 1

        logger.info(f'Fetching all {self.get_table()}')
        logger.debug(f'Fetching all {self.get_table()} with offset {current_page} and limit {per_page}')

        all_data = get_all(self.get_table(), current_page, per_page)
        # all_data = get(self.get_table(), current_page, per_page)

        logger.info(f'Fetch complete for all {self.get_table()}.')
        logger.debug(f'Fetch Complete with data {all_data}')
        #: default page is 0 as per default offset .i.e. current_page = 0
        #: adding + 1 to current_page gives us UX readable page number starting from 1.
        #: adding +2 to current_page gives next page. i.e. readable page + 1.
        pseudo_page = current_page + 1
        meta = {
            'current_page': pseudo_page,
            'per_page': per_page,
            'count': len(all_data),
            'next': pseudo_page + 1,
            'prev': pseudo_page - 1 if current_page > 0 else None
        }
        logger.debug(f'Prepared meta object: {meta}')

        dumped_data = self.get_schema().dump(all_data, many=True)
        data = {'meta': meta, 'data': dumped_data}
        logger.debug(f'Dumping all {self.get_table()} with metadata: {data}')
        return data

    def one(self, row_value: Any, identifier: str = 'id') -> dict:
        """Return single record row.

        :params row_value: where clause filter value
        :type row_value: Any

        :params identifier: where clause filter column name. defaults to `id`
        :type identifier: str

        :returns: single schema dump.
        :rtype: dict
        """
        logger.info(f'Fetching single {self.get_table()}.')
        logger.debug(f'Fetching single {self.get_table()} with identifier '
                     f'"{identifier}" and value "{row_value}"')

        single_data = get_one(self.get_table(),
                              row_value=row_value,
                              row_identifier=identifier)

        if single_data:
            logger.debug(f'Dumping single {self.get_table()} data: {single_data[0]}')
            return self.get_schema().dump(single_data[0])
        else:
            logger.warning(f'No data was found for {self.get_table()} with identifier '
                           f'"{identifier}" and value "{row_value}"')
            return {}

    def getVendorCodesById(self, row_value: Any, identifier: str = 'id') -> list:
        """Return vendor codes by vendor Id.

        :params row_value: where clause filter value
        :type row_value: Any

        :params identifier: where clause filter column name. defaults to `id`
        :type identifier: str

        :returns: single schema dump.
        :rtype: dict
        """
        logger.info(f'Fetching single {self.get_table()}.')
        logger.debug(f'Fetching single {self.get_table()} with identifier '
                     f'"{identifier}" and value "{row_value}"')

        all_data = get_one(self.get_table(),
                           row_value=row_value,
                           row_identifier=identifier)

        if all_data:
            logger.debug(f'Dumping single {self.get_table()} data: {all_data}')
            dumped_data = self.get_schema().dump(all_data, many=True)
            logger.debug(f'Dumping all {self.get_table()} with metadata: {all_data}')
            return {'data': dumped_data}
        else:
            logger.warning(f'No data was found for {self.get_table()} with identifier '
                           f'"{identifier}" and value "{row_value}"')
            return {}

    def get_product_by_InternalId(self, row_value: Any, identifier: str = 'id') -> list:
        """Return vendor codes by vendor Id.

        :params row_value: where clause filter value
        :type row_value: Any

        :params identifier: where clause filter column name. defaults to `id`
        :type identifier: str

        :returns: single schema dump.
        :rtype: dict
        """
        logger.info(f'Fetching single {self.get_table()}.')
        logger.debug(f'Fetching single {self.get_table()} with identifier '
                     f'"{identifier}" and value "{row_value}"')

        all_data = get_one(self.get_table(),
                           row_value=row_value,
                           row_identifier=identifier)

        if all_data:
            logger.debug(f'Dumping single {self.get_table()} data: {all_data}')
            dumped_data = self.get_schema().dump(all_data, many=True)
            logger.debug(f'Dumping all {self.get_table()} with metadata: {all_data}')
            return {'data': dumped_data}
        else:
            logger.warning(f'No data was found for {self.get_table()} with identifier '
                           f'"{identifier}" and value "{row_value}"')
            return {}

    def upsert(self, row_value: Any = None, identifier: str = 'id', conflict_fields: str = None):
        """Upsert `src_data` in database.

        :params row_value: where clause filter value.
        :type row_value: Any

        :params identifier: where clause filter column name. defaults to `id`.
        :type identifier:str

        :params conflict_fields: fields that are primary key in table and are required for upsert on conflict
        :type conflict_fields: str

        :raises NotImplementedError: Raised when src_data is a list of records as i.e.
                                    as of now bulk insert isn't implemented.

        :raises ValueError: Raised when update() is called before loading or dumping data.
        """
        self.check_src_data()
        if isinstance(self.src_data, dict) and len(self.src_data) > 1:
            upsert_data(self.get_table(), self.src_data, returning=True)
            logger.warning('Upsert for Multiple instances not implemented.')
            # raise NotImplementedError('Update for Multiple instances not implemented yet.')

        if self.is_loaded is False and self.is_dumped is False:
            logger.critical('Data should be loaded or dumped first to update.')
            raise ValueError('.load() or .dump() must be called before update')

        if row_value is None:
            if isinstance(self.loaded_data, list):
                row_value = self.loaded_data[0][identifier]
            else:
                row_value = self.loaded_data[identifier]

        logger.info('Executing upsert_data.')
        try:
            logger.debug(f'Saving data: {self.src_data}')
            upsert_bulk_data(self.get_table(), self.src_data, returning=True, conflict_fields=conflict_fields)
            # self.loaded_data = insert_data(self.get_table(), self.src_data,
            #                                include=self.get_dump_only_fields())[0]


        except Exception as ex:
            logger.info(ex)
            logger.info(f'updating data for {self.loaded_data}')
            pass

    def allNetsuiteItems(self, current_page: int, per_page: int) -> dict:  # noqa: A003
        """Return all records with pagination meta info.

        :param current_page: page number to fetch result from.
        :type current_page: int

        :param per_page: total number of records to fetch in a page.
        :type per_page:int

        :returns: schema dump along with meta object with pagination info.
        :rtype: dict
        """
        current_page = abs(current_page)
        per_page = per_page

        # offset index starts from 0 but pagination starts from 1
        # when api call is made /path/?page=1 should return first page.
        # where this page = 1 should be transformed to match offset 0 i.e.
        # page 1 = offset 0 (sql)
        current_page = current_page if current_page == 0 else current_page - 1

        logger.info(f'Fetching all {self.get_table()}')
        logger.debug(f'Fetching all {self.get_table()} with offset {current_page} and limit {per_page}')

        all_data = get_all_netsuite_items(self.get_table(), current_page, per_page)

        logger.info(f'Fetch complete for all {self.get_table()}.')
        logger.debug(f'Fetch Complete with data {all_data}')
        #: default page is 0 as per default offset .i.e. current_page = 0
        #: adding + 1 to current_page gives us UX readable page number starting from 1.
        #: adding +2 to current_page gives next page. i.e. readable page + 1.
        pseudo_page = current_page + 1

        meta = {
            'current_page': pseudo_page,
            'per_page': per_page,
            'count': len(all_data),
            'next': pseudo_page + 1,
            'prev': pseudo_page - 1 if current_page > 0 else None
        }
        logger.debug(f'Prepared meta object: {meta}')

        dumped_data = self.get_schema().dump(all_data, many=True)
        data = {'meta': meta, 'data': all_data}
        logger.debug(f'Dumping all {self.get_table()} with metadata: {data}')
        return data
