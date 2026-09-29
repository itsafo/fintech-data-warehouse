{#
    Standard dbt multi-env override: on the `prod` target, the custom schema
    (silver/gold, set per-folder in dbt_project.yml) is used as-is, so
    models land in the exact schemas Terraform created and analyst_reader
    was granted SELECT on. On any other target, it's suffixed onto the
    target schema (e.g. dev_schema_silver) so dev iteration never touches
    the prod-facing schemas.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
