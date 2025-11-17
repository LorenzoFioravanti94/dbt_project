{# 
    Macro to grant USAGE on each schema and SELECT permissions to the
    PUBLIC role for all tables and views in the specified schemas.
    Always works on target.schema suffixed with _STAGING, _TRANSFORMATION, _MARTS
#}
{% macro grant_schema_usage_and_select_permissions_to_public(database) %}

    {% set schemas = [
        target.schema ~ '_STAGING',
        target.schema ~ '_TRANSFORMATION',
        target.schema ~ '_MARTS'
    ] %}

    {% for schema in schemas %}
        grant usage on schema {{ database }}.{{ schema | upper }} to role PUBLIC;
        grant select on all tables in schema {{ database }}.{{ schema | upper }} to role PUBLIC;
        grant select on all views in schema {{ database }}.{{ schema | upper }} to role PUBLIC;
    {% endfor %}

{% endmacro %}
