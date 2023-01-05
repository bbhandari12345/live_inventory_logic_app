"""Collection of complex and other business logic related li_db queries."""

QUERY_FETCH_SYNC_CANDIDATE_FROM_VENDORS = (
    "select vendor_id, config_path from vendors as v "
    "where NOW() - v.last_fetch_date >= v.sync_interval and v.enabled != false;"
)

QUERY_UPDATE_LAST_MOD_VENDORS = (
    "update vendors "
    "set last_fetch_date = NOW() "
    "where vendor_id = %(vendor_id)s;"
)

QUERY_FETCH_AND_PRIORITY_FETCH_SYNC_CANDIDATE_FROM_VENDORS_VENDORS_CODE_PRODUCTS = (
    "select v.vendor_id , vc.vendor_code  from vendor_codes vc "
    "inner join products p on vc.internal_id = p.internal_id "
    "inner join vendors v on vc.vendor_id  = v.vendor_id "
    "where p.priority = true and (now()-vc.last_fetch_date >=p.sync_interval) and v.enabled != false;"
)

QUERY_BULK_UPDATE_LAST_FETCHED_VENDOR_CODES = (
    """
    update vendor_codes as c1 
    set
        last_fetch_date = NOW()
    from 
    (select 
        unnest(%(vendor_codes)s) as vendor_code
    ) as c2
    where c2.vendor_code = c1.vendor_code and c1.vendor_id = %(vendor_id)s;
    """
)

UPDATE_LAST_FETCH_DATE_OF_VENDOR_CODE_FOR_SINGLE_VENDOR = (
    """
    update public.vendor_codes
    set last_fetch_date = NOW()
    where vendor_id = %(vendor_id)s;
    """
)

QUERY_UPDATE_LAST_MOD_VENDOR_CODES = (
    "update vendor_codes "
    "set last_fetch_date = NOW() "
    "where vendor_id = %(vendor_id)s and vendor_code = %(vendor_code)s;"
)

QUERY_FETCH_VENDORS_CONFIG = (
    "select "
    "jsonb_build_object( "
    "'vendor_id',vendor_id, "
    "'config_file_path',%(config_file_path)s||vendor_id::text||'.json', "
    "'fetcher_write_path', %(fetcher_write_path)s, "
    "'item_codes',json_agg(distinct vendor_code::text), "
    "'template_values',jsonb_object_agg(key_name, value)) "
    "from vendor_configs vconfig inner join vendor_codes vc using(vendor_id) where vconfig.vendor_id = %(vendor_id)s "
    "group by vendor_id;"
)

QUERY_FETCH_VENDORS_CONFIG_PRIORITY = (
    "select "
    "jsonb_build_object( "
    "'vendor_id',vendor_id, "
    "'config_file_path',%(config_file_path)s||vendor_id::text||'.json', "
    "'fetcher_write_path', %(fetcher_write_path)s, "
    "'item_codes', %(vendor_code)s, "
    "'template_values',jsonb_object_agg(key_name, value)) "
    "from vendor_configs vconfig inner join vendor_codes vc using(vendor_id) where vconfig.vendor_id = %(vendor_id)s "
    "group by vendor_id;"
)

QUERY_GENERATE_ACCESS_TOKEN_CMD = (
    "select "
    "jsonb_build_object( "
    "'vendor_id',vendor_id, "
    "'config_file_path',%(config_file_path)s||vendor_id::text||'.json', "
    "'template_values',jsonb_object_agg(key_name, value)) "
    "from vendor_configs vconfig inner join vendors v using(vendor_id) "
    "where v.token_required = true and (v.token_generated_timestamp + v.token_life_seconds)  <= now() "
    "group by vendor_id;"
)

QUERY_FETCH_VENDORS_CODES = (
    "select vendor_code from inventory i where vendor_id = %(vendor_id)s"
)

QUERY_UPSERT_INVENTORY = (
    "INSERT INTO inventory ( vendor_id, vendor_code, availability_count, cost, currency ) VALUES ( %(vendor_id)s, "
    "%(vendor_code)s, %(availability_count)s, %(cost)s, %(currency)s)"
    "on conflict (vendor_code) do UPDATE "
    "SET vendor_id=EXCLUDED.vendor_id, currency=EXCLUDED.currency, cost=EXCLUDED.cost, "
    "availability_count=EXCLUDED.availability_count"
    # "INSERT INTO inventory ( vendor_id, vendor_code, availability_count, cost, currency) VALUES ( %(vendor_id)s,
    # %(vendor_code)s, %(availability_count)s, %(cost)s, %(currency)s ) "
)

QUERY_UPDATE_BEARER_TOKEN = (
    "update vendor_configs set value = %(auth_data)s where vendor_id = %(vendor_id)s and key_name= %(field_to_update)s"
)

QUERY_UPDATE_LAST_FETCH_ACCESS_TOKEN_VENDORS = (
    "update vendors set token_generated_timestamp = now(), token_life_seconds = %(token_life_seconds)s where "
    "vendor_id = %(vendor_id)s"
)

QUERY_SELECT_PRODUCTS_BY_VENDOR_CODES = (
    " select *  FROM products "
    "inner join vendor_codes vc "
    "on products.product_id = %(vc.product_id)s "
)

QUERY_FETCH_VENDORS_CONNECTION_TYPE = (
    "select v.connection_type from vendors v where v.vendor_id = %(vendor_id)s"
)

QUERY_CHECK_IF_VENDOR_CODE_EXISTS = (
    "select exists( "
    "select 1 from vendor_codes vc "
    "WHERE (vc.vendor_code, vc.vendor_id) IN ( "
    " (%(vendor_code)s,%(vendor_id)s) ) )"
)

QUERY_CHECK_IF_VENDOR_CODE_EXISTS_IN_VENDOR_CODES_TABLE = (
    "select vendor_code from vendor_codes vc where vendor_id  = %(vendor_id)s"
)

QUERY_UPDATE_RESPONSE_CODE_RESPONSE_TEXT_IN_VENDORS = (
    "UPDATE vendors SET response_code = %(response_code)s, response_text = %(response_text)s"
    " WHERE vendor_id = %(vendor_id)s"
)

QUERY_FETCH_VENDOR_CODE_INTERNAL_ID_MAPPING = (
    "select vendor_code, internal_id from vendor_codes where vendor_id=%s"
)

QUERY_FETCH_DATA_FROM_INVENTORY_USING_INTERNAL_ID = (
    """
    select vc.internal_id, vc.vendor_code, vc.vendor_id,
        i.availability_status, i.availability_count, i.next_availability_date, i.cost, i.currency, 
        i.modified_on AS modified, v.vendor_name,
        case 
        when (i.internal_id is null) then 'Unavailable'
        else 'OK'
        END as status
        from vendor_codes vc 
        full join inventory i  using (internal_id, vendor_id, vendor_code ) 
        full join vendors v using (vendor_id) 
        where vc.internal_id = any(%(internal_ids)s);
    """
)

QUERY_FETCH_FILTER_ALLOWED_CODES = """
            select
                vendor_id,
                jsonb_agg(vendor_code) as vendor_codes
            from
                vendor_codes vc
            inner join vendors v
            using (vendor_id)
            where
                internal_id = any (%(internal_ids)s)
                and v.allows_vc_filter = true
            group by
                vendor_id ;
                    """

QUERY_DELETE_OLD_RECORD_INVENTORY = (
    "delete from inventory i where (NOW() - case when (i.last_valid_inserted_updated is not NULL) then "
    "i.last_valid_inserted_updated else  i.created_on end >  %(invalid_record_age)s) and i.invalid = true;"
)
