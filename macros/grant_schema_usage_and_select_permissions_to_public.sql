{# 
    Macro to grant USAGE on each schema and SELECT permissions to the
    PUBLIC role for all tables and views in the specified schemas.
    
    Parameters:
        database(str): target database name
        schemas(list[str]): list of schema names within the database
#}
{% macro grant_schema_usage_and_select_permissions_to_public(database, schemas) %}
    {# Loop over each schema provided in the list #}
    {% for schema in schemas %}
        grant usage on schema {{ database }}.LFIORAVANTI_{{ schema | upper }} to role PUBLIC;
        grant select on all tables in schema {{ database }}.LFIORAVANTI_{{ schema | upper }} to role PUBLIC;
        grant select on all views in schema {{ database }}.LFIORAVANTI_{{ schema | upper }} to role PUBLIC;
    {% endfor %}
{% endmacro %}
