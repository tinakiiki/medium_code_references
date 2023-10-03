{% macro convert_ts(ts_scale,kafka_json_key_res=none,model_name=none) %}
    {%- set table_name = this.identifier.split('__')[1]-%}
    {% set casted_ts_columns = get_cdc_casted_columns(
            table_name=table_name,
            dtype_regex='time'
            ) %}
    {% set ts_columns = [] %}

    {% for col in casted_ts_columns %}
        {%- do ts_columns.append(col.split('as ')[1]) -%}
    {% endfor %}

    -- if model then get columns and compare to ts_columns
    {% if model_name %}
    {% set model_columns = adapter.get_columns_in_relation(model_name) %}
    SELECT
        {% for col in model_columns %}
            {% if '_ms' in col.name and col.name in ts_columns %}
                to_timestamp("{{ col.name }}":: int,3) AS "{{ col.name }}"
            {% elif col.name in ts_columns %}
                to_timestamp("{{ col.name }}":: int,{{ts_scale}}) AS "{{ col.name }}"
            {% else %}
                "{{ col.name }}"
            {% endif %}
            {% if not loop.last %},{% endif %}
        {% endfor %}
    FROM
        {{ model_name }}

    {% else %}
    -- else if column list input then filter list for ts_colums
    
    {% for column in kafka_json_key_res %}

        {% set ts_scale = 3 if '_ms' in column else ts_scale %}
        
        {% set is_ts_col = [] %}

        {% for ts_col in ts_columns %}
            {% if column.lower().find(ts_col) != -1  %}
                {%- do is_ts_col.append('True') %}
            {% endif %}
        {% endfor %}
        
        {% if 'AS ' in column and 'True' in is_ts_col %}
        -- convert ts
            {{ 'to_timestamp(' ~ column | replace('AS', ':: int,'~ ts_scale ~')' ~ ' AS ' ) }}
        {% elif 'True' in is_ts_col and ' AS ' not in column %}
        -- convert ts
            {{ 'to_timestamp(' ~ column ~ ':: int,'~ ts_scale ~')' ~ ' AS ' ~ column }}
        {% else %}
            {{ column }}
        {% endif %}
        {% if not loop.last %},{% endif %}
    {% endfor %}
    {% endif %}
{% endmacro %}
