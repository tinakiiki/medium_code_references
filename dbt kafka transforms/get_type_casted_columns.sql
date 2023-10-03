{% macro get_cdc_casted_columns(table_name,dtype_regex=none) %}

{% set expected_count = 2 if 'expected_count' in this.schema else 1 %}
{% set schema_regex = 'schema_regex' if 'schema_regex' in this.schema else '' %}
{% set target_database = 'target_database' if target.name == 'prod' else 'target_database_DEV' %}

{% set get_cols_query %}
    SELECT lower(casted_column) , COUNT(distinct INFORMATION_SCHEMA_TABLE) as ct
    FROM {{target_database}}.YOUR_TABLE_WITH_INFORMATION_SCHEMA
    WHERE lower(table_name) = '{{table_name|lower()}}'
    AND (CONTAINS('{{ this.identifier}}', REPLACE(lower(INFORMATION_SCHEMA_TABLE),'_information_schema'))
        OR CONTAINS(lower(INFORMATION_SCHEMA_TABLE),'{{schema_regex}}'))
    {% if dtype_regex %} AND data_type ILIKE {{ "'%"~ dtype_regex ~"%'"}} {% endif %}
    GROUP BY casted_column
    HAVING ct = {{expected_count}}
{% endset %}

{% set results = run_query(get_cols_query) %}

{% if execute %}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{{ return( results_list ) }}

{% endmacro %}
