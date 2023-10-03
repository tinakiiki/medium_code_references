{% macro extract_json_keys(model_name, json_columns = [], prepend_json_column_name=False) %}
    {%- set json_keys = [] -%}

    {% for json_column in json_columns %}
        {% set json_column_query %}
                SELECT DISTINCT json_object.key AS column_name
                FROM {{model_name}},
                LATERAL FLATTEN (input => {{ json_column }}) AS json_object
        {% endset %}

        {% set results = run_query(json_column_query) %}

        {% if execute %}
            {% set results_list = results.columns[0].values() %}
        {% else %}
            {% set results_list = [] %}
        {% endif %}

        {% for column_name in results_list %}
            {% if prepend_json_column_name %}
                {% set col_name = 'STRIP_NULL_VALUE(' ~ json_column ~ ':' ~ column_name ~ ') AS ' ~  json_column | replace(":", "_") ~ '_' ~ column_name %}
            {% else %}
                {% set col_name = 'STRIP_NULL_VALUE(' ~ json_column ~ ':' ~ column_name ~ ') AS ' ~  column_name %}
            {% endif %}
            {%- do json_keys.append(col_name) -%}
        {% endfor %}
        
    {% endfor %}
    
    {{ return( json_keys | join(", ") ) }}

{% endmacro %}
