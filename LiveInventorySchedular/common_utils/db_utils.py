""" Collections of most common db queries"""
from typing import Any, Dict, List, Tuple
from LiveInventorySchedular.config import Config
from LiveInventorySchedular.utils.data_access_layer.sql_db import DBEngineFactory
import logging

li_db = DBEngineFactory.get_db_engine(Config.DB_CONFIG, DBEngineFactory.POSTGRES)
# get the logger instance
logger = logging


def insert_data(table: str, insert_data: dict, include=None, returning: bool = True) -> dict:
    """Save given single data to li_db table.

    Prepares insert Query with placeholders from insert_data dict keys
    and inserts in a single transaction.

    :params table: table to insert into.
    :type table: str

    :params insert_data: new record value in dict where keys must map to table columns.
    :type insert_data: dict

    :params include: any extra fields to include while returning after insert. e.g:: ['id','created_date']
    :type include: list

    :params returning: Preapre query to return after insert if true else no return insert.
    :type returning: bool

    :returns: Inserted data points back from database in a dict.
    :rtype: dict

    :raises Exception: Raised when error occurs in insert transaction.
    """
    sql = generate_insert_sql(table, insert_data, include, returning)
    values = list(insert_data.values())

    try:
        with li_db.transaction(auto_commit=True) as qryset:
            resultset = qryset.execute_query(sql, values)
            logger.debug(f'Executed Non Query {qryset.query}')
            return resultset.to_list()
    except Exception as e:
        logger.error('Error in insert_data execution.', exc_info=True)
        raise e


def update_data(table: str, update_data: dict, row_identifier: str, row_value: Any) -> None:
    """Update table data.

    Prepares update query with placeholders from update_data dict keys and
    updates in a single transaction.

    :params table: table to update.
    :type table: str

    :params update_data: updated record value in dict where keys must map to table columns.
    :type update_data: dict

    :params row_value: where clause filter value.
    :type row_value: Any

    :params row_identifier:where clause filter column name.
    :type row_identifier: str

    :raises Exception: Raised when error occurs in update transaction.
    """
    sql = generate_update_sql(table, update_data, row_identifier)
    # Using dictionary as placeholders instead of tuples
    # Preparing placegolder data along with where clause.
    # Specific to python 3.9 `|` mergers two dicts
    update_data = update_data | {row_identifier: row_value}
    try:
        with li_db.transaction(auto_commit=True) as qryset:
            qryset.execute_non_query(sql, update_data)
            logger.debug(f'Executed non Query {qryset.query}')
    except Exception as e:
        logger.error('Error in update_data execution.', exc_info=True)
        raise e


def delete_rows(table: str, row_identifier: str, row_value: Any) -> None:
    """Delete single row from li_db table.

    :params table: table to delete from.
    :type table: str

    :params row_value: where clause filter value.
    :type row_value: Any

    :params row_identifier: cwhere clause filter column name.
    :type row_identifier: str

    :raises Exception: Raised when error occurs in delete transaction.
    """
    sql = generate_delete_sql(table, row_identifier)
    # Preparing placeholder data for dict values.
    delete_data = {row_identifier: row_value}
    try:
        with li_db.transaction(auto_commit=True) as qryset:
            qryset.execute_non_query(sql, delete_data)
            logger.debug(f'Executed non Query {qryset.query}')
    except Exception as e:
        logger.error('Error in delete_rows execution.', exc_info=True)
        raise e


def get_all(table: str, current_page: int, per_page: int) -> List[Dict[str, Any]]:
    """Return all records queried with given limit and offset.

    :params table: table to fetch rows.
    :type table: str

    :param current_page: page number to fetch result from (offset)
    :type current_page: int

    :param per_page: total number of records to fetch in a page. (limit)
    :type per_page:int

    :returns: list of dict
    :rtype: list

    :raises Exception: Raised when error occurs in select transaction.
    """
    sql = generate_get_all_sql(table, current_page, per_page)
    try:
        with li_db.transaction(auto_commit=True) as qryset:
            resultset = qryset.execute_query(sql)
            logger.debug(f'Executed Query {qryset.query}')
            return resultset.to_list()
    except Exception as e:
        logger.error('Error in get_all execution.', exc_info=True)
        raise e


def get_one(table: str, row_value: Any, row_identifier: str) -> dict:
    """Return single records filtered by where clause.

        :params table: table to fetch rows.
        :type table: str

        :params row_value: where clause filter value
        :type row_value: Any

        :params row_identifier: where clause filter column name
        :type row_identifier: str

        :returns: single row result in dict.
        :rtype: dict

        :raises Exception: Raised when error occurs in select transaction.
        """
    sql = generate_get_one_sql(table, row_identifier)
    try:
        with li_db.transaction(auto_commit=True) as qryset:
            resultset = qryset.execute_query(sql, (row_value,))
            logger.debug(f'Executed Query {qryset.query}')
            return resultset.to_list()
    except Exception as e:
        logger.error('Error in get_one execution.', exc_info=True)
        raise e


def generate_insert_sql(table: str, insert_data: dict, include, returning) -> str:
    """Prepare insert query with placeholders from insert_data dict keys.

        eg::
            INSERT INTO table_name (col1,col2) VALUES ();

        :params table: table to insert into.
        :type table: str

        :params insert_data: new record value in dict where keys must map to table columns.
        :type insert_data: dict

        :params include: any extra fields to include while returning after insert. e.g:: ['id','created_date']
        :type include: list

        :params returning: Preapre query to return after insert if true else no return insert.
        :type returning: bool

        :returns: Generated SQL Query string.
        :rtype: str
        """
    logger.info('Generating SQL statement for data insert.')
    placeholders = ', '.join(['%s'] * len(insert_data))
    columns = ', '.join(insert_data.keys())

    if returning:
        logger.debug('Returning param is set to True. Preparing INSERT with returning.')
        if include:
            returning_cols = ', '.join((set(include + list(insert_data.keys()))))
        else:
            returning_cols = columns

        sql = ('INSERT INTO %s ( %s ) VALUES ( %s ) '
               'RETURNING %s;' % (table, columns, placeholders, returning_cols))  # noqa: S608
    else:
        logger.debug('Returning param is set to False. Preparing INSERT without returning.')
        sql = ('INSERT INTO %s ( %s ) VALUES ( %s )' % (table, columns, placeholders))  # noqa: S608

    logger.info(f'Generated SQL statement {sql}')
    return sql


def generate_update_sql(table: str, update_data: dict, row_identifier: str) -> str:
    """Prepare update query with placeholders from update_data dict keys.

        eg::
           `UPDATE <table> SET key1=%(key1)s, key2=%(key2)s WHERE <row_identifier> =%(row_identifier)s;`

        :params table: table to update.
        :type table: str

        :params update_data: updated record value in dict where keys must map to table columns.
        :type update_data: dict

        :params row_identifier:where clause filter column name.
        :type row_identifier: str

        :returns: Generated SQL Query string.
        :rtype: str
        """
    logger.info('Generating SQL statement for data update.')
    set_placeholders = ', '.join([f'{key}=%({key})s' for key in update_data.keys()])
    where_clause = f' {row_identifier} =%({row_identifier})s'
    sql = 'UPDATE %s SET %s WHERE%s' % (table, set_placeholders, where_clause)  # noqa: S608
    logger.info(f'Generated SQL statement {sql}')
    return sql


def generate_bulk_update_sql(table: str, data: List, row_identifier: str) -> str:
    """Prepare bulk update query with placeholders from update_data list keys.
        :params table: table to update.
        :type table: str
        :params update_data: updated record value in dict where keys must map to table columns.
        :type update_data: dict
        :params row_identifier:where clause filter column name.
        :type row_identifier: str
        :returns: Generated SQL Query string.
        :rtype: str
        """
    logger.info('Generating SQL statement for bulk data update.')

    set_placeholders = ', '.join([f'{key}=c2.{key}' for key in data.keys() if key not in row_identifier.split(',')])
    select_placeholders = ', '.join([f'unnest(array{v}) as {k}' for k, v in data.items()])
    where_clause = " and ".join(f"c2.{val}=c1.{val}" for val in row_identifier.split(','))

    sql = """
        update %s as c1 
        set %s
        from (select %s) as c2
        where %s;
    """ % (table, set_placeholders, select_placeholders, where_clause)
    logger.info(f'Generated SQL statement {sql}')
    return sql


def generate_delete_sql(table: str, row_identifier: str) -> str:
    """Prepare delete SQL statement for given table with row indentifier as
        where clause.

        eg::
            `DELETE FROM <table> WHERE <row_indentifier> = %(row_indentifier)s;'`

        :params table: table to delete from.
        :type table: str

        :params row_identifier: where clause filter column name.
        :type row_identifier: str

        :returns: Generated SQL Query string.
        :rtype: str
        """
    logger.info('Generating SQL statement for data deletion.')
    sql = f'DELETE FROM {table} WHERE {row_identifier} = %({row_identifier})s;'  # noqa: S608
    logger.info(f'Generated SQL statement {sql}')
    return sql


def generate_get_one_sql(table: str, row_identifier: str) -> str:
    """Prepare single indentifier record fetch query by where clause.

        eg::
            `SELECT * FROM <table> WHERE <row_identifier> = %s;`

        :params table: table to fetch rows.
        :type table: str

        :params row_identifier: where clause filter column name
        :type row_identifier: str

        :returns: Generated SQL Query string.
        :rtype: str
        """
    logger.info('Generating SQL statement getting one data point.')
    sql = f'SELECT * FROM {table} WHERE {row_identifier} = %s;'  # noqa: S608
    logger.info(f'Generated SQL statement {sql}')
    return sql


def generate_get_all_sql(table, current_page, per_page):
    """Prepare SELECT * for given table with limit and offset for pagination.

        eg::
            `SELECT * FROM <table> OFFSET <current_page> LIMIT <per_page>;`

        :params table: table to fetch rows.
        :type table: str

        :param current_page: page number to fetch result from (offset)
        :type current_page: int

        :param per_page: total number of records to fetch in a page. (limit)
        :type per_page:int

        :returns: Generated SQL Query string.
        :rtype: str
        """
    logger.info('Generating SQL statement getting all data with limit.')
    if per_page == -1:
        logger.debug('Got -1 in per_page. Preparing query without OFFSET and LIMIT.')
        sql = f'SELECT * FROM {table};'  # noqa: S608
    else:
        sql = f'SELECT * FROM {table} OFFSET {current_page} LIMIT {per_page};'  # noqa: S608

    logger.info(f'Generated SQL statement {sql}')
    return sql


def execute_sql_query(query, params):
    """

    :param query:
    :param params:
    :return:
    """
    try:
        with li_db.transaction() as query_set:
            query_set.execute_query(query, params)
            logger.info(f'Executed Query {query_set.query}')

    except Exception as e:
        logger.error(f'Error while executing {query_set.query}', exc_info=True)
        raise e


def execute_sql_non_query(query, params=None):
    """

    :param query:
    :param params:
    :return:
    """
    try:
        with li_db.transaction(auto_commit=True) as query_set:
            query_set.execute_query(query, params)
            logger.info(f'Executed Query {query_set.query}')

    except Exception as e:
        logger.error(f'Error while executing {query_set.query}', exc_info=True)
        raise e
    raise NotImplementedError


def upsert_data(table: str, insert_data: dict, include=None, returning: bool = True) -> dict:
    """Save given single data to li_db table.

    Prepares insert Query with placeholders from insert_data dict keys
    and inserts in a single transaction.

    :params table: table to insert into.
    :type table: str

    :params insert_data: new record value in dict where keys must map to table columns.
    :type insert_data: dict

    :params include: any extra fields to include while returning after insert. e.g:: ['id','created_date']
    :type include: list

    :params returning: Preapre query to return after insert if true else no return insert.
    :type returning: bool

    :returns: Inserted data points back from database in a dict.
    :rtype: dict

    :raises Exception: Raised when error occurs in insert transaction.
    """
    sql = generate_upsert_sql(table, insert_data, include, returning)
    values = list(insert_data.values())

    try:
        with li_db.transaction(auto_commit=True) as qryset:
            qryset.execute_non_query(sql, values)
            logger.info(f'Executed Query {qryset.query}')
            # return resultset
    except Exception as e:
        logger.info(e)
        logger.error('Error in upsert_data execution.', exc_info=True)
        raise e


def upsert_bulk_data(table: str, insert_data: list, include=None, returning: bool = True,
                     conflict_fields: str = None) -> list:
    """Save given multiple data to li_db table.

    Prepares insert Query with placeholders from insert_data keys
    and inserts in a single transaction.

    :params table: table to insert into.
    :type table: str

    :params insert_data: new record value in dict where keys must map to table columns.
    :type insert_data: dict

    :params include: any extra fields to include while returning after insert. e.g:: ['id','created_date']
    :type include: list

    :params returning: Preapre query to return after insert if true else no return insert.
    :type returning: bool

    :params conflict_fields: fields that are primary key in table and are required for upsert on conflict
    :type conflict_fields: str

    :returns: Inserted data points back from database in a list.
    :rtype: list

    :raises Exception: Raised when error occurs in insert transaction.
    """

    # sql = generate_bulk_upsert_sql(table, insert_data, include, returning)

    def chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    CHUNK_SIZE = 200
    split_body = chunks(insert_data, CHUNK_SIZE)

    try:
        for items in split_body:
            sql, cols = generate_bulk_upsert_sql(table, items, include, returning, conflict_fields)

            value_list = []
            cols = cols.split(', ')

            ### For some xml vendors the keys of objects in an array are not in same order 
            ### to that of columns set in insert statement. So, sorting their orders
            for data in items:
                value = []
                for val in cols:
                    value.append(data.get(val))
                value_list.append(value)

            try:
                with li_db.transaction(auto_commit=True) as qryset:
                    # qryset.execute_non_query(sql, values)
                    tuples = tuple(tuple(x) for x in value_list)
                    qryset.execute_non_query(sql, tuples)
                    logger.info(f'Executed Query {qryset.query}')
                    # return resultset
            except Exception as e:
                logger.info(e)
                logger.error('Error in upsert_data execution.', exc_info=True)
                raise e

    except Exception as e:
        logger.error("Error during processing of items in chunked array", exc_info=True)
        logger.error(e, exc_info=True)


def generate_upsert_sql(table: str, insert_data: dict, include, returning) -> str:
    """Prepare insert query with placeholders from insert_data dict keys.

        eg::
            INSERT INTO table_name (col1,col2) VALUES ();

        :params table: table to insert into.
        :type table: str

        :params insert_data: new record value in dict where keys must map to table columns.
        :type insert_data: dict

        :params include: any extra fields to include while returning after insert. e.g:: ['id','created_date']
        :type include: list

        :params returning: Preapre query to return after insert if true else no return insert.
        :type returning: bool

        :returns: Generated SQL Query string.
        :rtype: str
        """
    logger.info('Generating SQL statement for data upsert.')
    placeholders = ', '.join(['%s'] * len(insert_data))
    columns = ', '.join(insert_data.keys())

    if returning:
        logger.debug('Returning param is set to True. Preparing INSERT with returning.')
        if include is None:
            returnset = set(list(insert_data.keys()))
            returnset.remove('vendor_code')
            returnset.remove('vendor_id')
            # returnset.remove('vendor_id')
            pre_res = [f'{keys} = EXCLUDED.' + keys for keys in returnset]
            returning_cols = ', '.join(set(pre_res))
        else:
            returning_cols = columns

        sql = ('INSERT INTO %s ( %s ) VALUES ( %s ) '
               'limit 1 on conflict (vendor_code, vendor_id) do UPDATE SET %s;' % (
               table, columns, placeholders, returning_cols))  # noqa: S608

    else:
        logger.debug('Returning param is set to False. Preparing INSERT without returning.')
        sql = ('INSERT INTO %s ( %s ) VALUES ( %s )' % (table, columns, placeholders))  # noqa: S608

    logger.info(f'Generated SQL statement {sql}')
    return sql


def generate_bulk_upsert_sql(table: str, insert_data: list, include, returning, conflict_fields: str = None) -> Tuple[
    str, str]:
    """Prepare insert query with placeholders from insert_data dict keys.

        eg::
            INSERT INTO table_name (col1,col2) VALUES ();

        :params table: table to insert into.
        :type table: str

        :params insert_data: new record value in dict where keys must map to table columns.
        :type insert_data: dict

        :params include: any extra fields to include while returning after insert. e.g:: ['id','created_date']
        :type include: list

        :params returning: Preapre query to return after insert if true else no return insert.
        :type returning: bool

        :params conflict_fields: fields that are primary key in table and are required for upsert on conflict
        :type conflict_fields: str

        :returns: Generated SQL Query string and columns.
        :rtype: Tuple[str, str]
        """
    logger.info('Generating SQL statement for data upsert.')
    placeholders = ', '.join(['%s'] * len(insert_data))
    columns = ', '.join(insert_data[0].keys())

    try:
        if returning:
            logger.debug('Returning param is set to True. Preparing INSERT with returning.')
            if include is None:
                returnset = set(list(insert_data[0].keys()))
                returnset.remove('vendor_code')
                returnset.remove('vendor_id')
                returnset.remove('internal_id')
                if table == "vendor_codes":
                    pre_res = [f'{keys} = EXCLUDED.' + keys for keys in returnset if keys != 'internal_id']
                else:
                    pre_res = [f'{keys} = EXCLUDED.' + keys for keys in returnset]
                returning_cols = ', '.join(set(pre_res))
            else:
                returning_cols = columns

            sql = ('INSERT INTO %s ( %s ) VALUES  %s  '
                   'on conflict (%s) do UPDATE SET %s;' % (
                       table, columns, placeholders, conflict_fields, returning_cols))  # noqa: S608

        else:
            logger.debug('Returning param is set to False. Preparing INSERT without returning.')
            sql = ('INSERT INTO %s ( %s ) VALUES ( %s )' % (table, columns, placeholders))  # noqa: S608

    except Exception as e:
        logger.error("Couldnot create statement for bulk upsert", exc_info=True)
        logger.error(e, exc_info=True)

    logger.info(f'Generated SQL statement {sql}')
    return sql, columns

    # logger.info('Generating SQL statement for data upsert.') if isinstance(insert_data, list): sql = ('INSERT INTO
    # public.inventory (vendor_id, vendor_code, availability_count, next_availability_date, internal_id, "cost",
    # currency, created_on, inventory_id, modified_on, availability_status) VALUES ( %s ) ' 'limit 1 on conflict (
    # vendor_code, vendor_id) do UPDATE SET Excluded.vendor_id, Excluded.vendor_code, Excluded.availability_count,
    # Excluded.next_availability_date, Excluded.internal_id, Excluded."cost", Excluded.currency, Excluded.created_on,
    # Excluded.inventory_id, Excluded.modified_on, Excluded.availability_status;')   # noqa: S608
    #
    # else:
    #     logger.debug('Returning param is set to False. Preparing INSERT without returning.')
    #     sql = ('INSERT INTO %s ( %s ) VALUES ( %s )')  # noqa: S608
    #
    # logger.info(f'Generated SQL statement {sql}')
    # return sql


def generate_get_all_internal_id_sql(table, current_page, per_page):
    """Prepare SELECT * for given table with limit and offset for pagination.

        eg::
            `SELECT * FROM <table> OFFSET <current_page> LIMIT <per_page>;`

        :params table: table to fetch rows.
        :type table: str

        :param current_page: page number to fetch result from (offset)
        :type current_page: int

        :param per_page: total number of records to fetch in a page. (limit)
        :type per_page:int

        :returns: Generated SQL Query string.
        :rtype: str
        """

    logger.info('Generating SQL statement getting all data with limit.')
    if per_page == -1:
        logger.debug('Got -1 in per_page. Preparing query without OFFSET and LIMIT.')
        sql = f'SELECT json_build_object({table}.internal_id, json_agg({table}. *)) FROM {table} group by {table}.internal_id;'  # noqa: S608
    else:
        sql = f'SELECT json_build_object({table}.internal_id, json_agg({table}. *)) FROM {table} group by {table}.internal_id OFFSET {current_page} LIMIT {per_page};'  # noqa: S608

    logger.info(f'Generated SQL statement {sql}')
    return sql


def get_all_netsuite_items(table: str, current_page: int, per_page: int) -> List[Dict[str, Any]]:
    """Return all records queried with given limit and offset.

    :params table: table to fetch rows.
    :type table: str

    :param current_page: page number to fetch result from (offset)
    :type current_page: int

    :param per_page: total number of records to fetch in a page. (limit)
    :type per_page:int

    :returns: list of dict
    :rtype: list

    :raises Exception: Raised when error occurs in select transaction.
    """
    sql = generate_get_all_internal_id_sql(table, current_page, per_page)
    # nosql = "select json_build_object(inv.internal_id, json_agg(inv.*)) from inventory inv group by inv.internal_id"

    try:
        with li_db.transaction(auto_commit=True) as qryset:
            resultset = qryset.execute_query(sql)
            logger.debug(f'Executed Query {qryset.query}')
            return_set = resultset.to_list()
            returnList = []
            for items in return_set:
                returnList.append(items.get('json_build_object'))
            return returnList
    except Exception as e:
        logger.error('Error in get_all execution.', exc_info=True)
        raise e


def generate_sql_get_disabled_vendors(**kwargs):
    """
    This function generates the sql query from given keyword arguments
    first argument: method = (GET)
    second argument: table name = "table_name1"
    third argument: [{key1: value1},{key2: value2},...]
    :param kwargs:
    :return: sql
    """
    select_key = kwargs.get('select_key')
    table_name = kwargs.get("table_name")
    identifier = kwargs.get('identifier')
    identifier_keys = kwargs.get('identifier_keys')
    identifier_type = kwargs.get('identifier_type')
    sql = 'select ' + select_key + ' from ' + table_name + ' where '
    i = 1
    for item in identifier:
        my_key = identifier_keys[i - 1]
        if identifier_type[i - 1] == 'int':
            if len(identifier) == i:
                sql = sql + my_key + ' = ' + str(item[my_key]) + " "
            else:
                sql = sql + my_key + ' = ' + str(item[my_key]) + ' and '
        elif identifier_type[i - 1] == 'str':
            if len(identifier) == i:
                sql = sql + my_key + ' = ' + " '" + str(item[my_key]) + "'"
            else:
                sql = sql + my_key + ' = ' + " '" + str(item[my_key]) + "'" + ' and '
        i += 1
    logger.info("NO sql command is generated to get a row")
    return sql
