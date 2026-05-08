-- THIS MACRO is used:
    -- if a custom schema is defined in dbt_project.yml, use it as-is without any prefix.
    -- if no custom schema is defined, fall back to the target schema.
    -- EXAMPLE: custom schema = 'cove_bronze' → result = 'cove_bronze' (clean, no prefix, as we expected); not `cove_elt_cove_bronze`.

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}