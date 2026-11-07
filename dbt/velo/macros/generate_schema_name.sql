{#- Route models with +schema: features to velo.features (not gold_features).
    Everything else keeps dbt's default behaviour, so existing models and test-audit tables don't move. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif custom_schema_name | trim in ['features'] -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
