{{
    config(
        materialized="incremental",
        unique_key="_kafka_record_metadata_key_id",
        incremental_strategy="merge",
        on_schema_change='sync_all_columns',
        )
}}

SELECT
    _kafka_record_metadata_key_id,
    _kafka_ingest_id
FROM {{ ref("base_table") }}
{% if is_incremental() %}
WHERE
    _kafka_ingested_to_raw_date
    > dateadd(day,-3,SYSDATE()::DATE)
{% endif %}
qualify
    row_number() over (
        partition by _kafka_record_metadata_key_id
        order by
            _kafka_record_content_ts_ms desc,
            _kafka_record_content_lsn desc,
            _kafka_record_metadata_offset desc,
            _kafka_ingest_id desc
    )
    = 1
    
