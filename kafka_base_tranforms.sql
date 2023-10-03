{% macro kafka_base_transform(source_name, order_clause=none, additional_columns=[],flatten_after=True) %}


{% set all_columns = extract_json_keys(
            model_name=source_name,
            json_columns=['RECORD_CONTENT:after' if flatten_after else 'RECORD_CONTENT'],
            prepend_json_column_name=False
            ).split(',')+ additional_columns %}

{% set scale_ts = 3 if 'somedb' in this.identifier else 6  %}

    SELECT DISTINCT
    COALESCE(RECORD_METADATA:key:id,RECORD_METADATA:key) AS _KAFKA_RECORD_METADATA_KEY_ID,
    {{ convert_ts(ts_scale=scale_ts, kafka_json_key_res = all_columns) }},
    TO_TIMESTAMP(RECORD_METADATA :LogAppendTime :: INT,3) AS KAFKA_RECORD_TS,
    RECORD_CONTENT:op :: VARCHAR AS _KAFKA_RECORD_CONTENT_OP,
    RECORD_CONTENT:source:ts_ms :: INTEGER AS _KAFKA_RECORD_CONTENT_TS_MS,
    ROW_NUMBER() OVER (PARTITION BY _KAFKA_RECORD_METADATA_KEY_ID ORDER BY KAFKA_RECORD_TS DESC,_KAFKA_RECORD_CONTENT_TS_MS DESC,RECORD_METADATA:PARTITION DESC,RECORD_METADATA:OFFSET DESC) AS _KAFKA_TRANSACTION_RANK
    FROM {{ source_name }}

{% endmacro %}
