{{
    config(
        materialized="incremental",
        unique_key="_KAFKA_INGEST_ID",
        incremental_strategy="merge",
        on_schema_change='append_new_columns')
}}

{{
    kafka_base_transform(
        source_name=source("YOUR SCHEMA SNOWFLAKE SINK", "YOUR SNOWFLAKE SINK DESTINATION TABLE"),
    )
}}
