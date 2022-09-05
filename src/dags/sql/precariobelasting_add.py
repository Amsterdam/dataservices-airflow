from typing import Final

# Add derived columns:
# The column `gebied` is missing in WOONSCHEPEN and BEDRIJFSVAARTUIGEN
# yet defined in Amsterdam schema, so needs to be added.
ADD_GEBIED_COLUMN: Final = """
{% set schema = params.schema|default("public", true) %}
{% for tablename in params.tablenames %}
    ALTER TABLE {{ schema }}.{{ tablename }} ADD COLUMN IF NOT EXISTS gebied VARCHAR(25);

    WITH {{ tablename }}_gebied as (
    select
    ID identifier,
    /* based on the rate ('tarief') value, a area ('gebied') is identified by an
        added group number. The regexp is used to focus on the rate numbers only,
        not clutered by additional karakters like euro sign or , or -
    */
    CASE
    WHEN DENSE_RANK() \
        OVER (ORDER BY REGEXP_REPLACE(tarief_per_jaar_per_m2, '[^[:digit:]]', '', 'g')) = 1
    THEN 'Tariefgebied A'
    ELSE 'Tariefgebied B'
    END
    as gebied_type
    from {{ schema }}.{{ tablename }}
    )
    UPDATE {{ schema }}.{{ tablename }}
    SET gebied = gebied_type
    FROM {{ tablename }}_gebied
    WHERE ID = identifier;
    COMMIT;
{% endfor %}
"""

# The source contains in the field gebied the value 'gebied A' or 'gebied 1',
# this needs te be translated to 'Tariefgebied [Letter or Number]' etc.
RENAME_DATAVALUE_GEBIED: Final = """
{% set schema = params.schema|default("public", true) %}
{% for tablename in params.tablenames %}
    UPDATE {{ schema }}.{{ tablename }}
    SET gebied = REGEXP_REPLACE(gebied, 'Gebied', 'Tariefgebied');
    COMMIT;
{% endfor %}
"""

# ---------------------------------------------------------------------------------- #
# TEMPORARY: Needed for geosearch API support.
# TO DO: The creation of the DB structure must be based completely on Amsterdam schema.
# ---------------------------------------------------------------------------------- #
ADD_TITLE: Final = """
{% set schema = params.schema|default("public", true) %}
{% for tablename in params.tablenames %}
    ALTER TABLE {{ schema }}.{{ tablename }} ADD COLUMN IF NOT EXISTS title VARCHAR(100);

    WITH {{ tablename }}_gebied as (
    select
    ID identifier,
    {% if 'woonschepen' in tablename %}
    'Precariobelasting woonschepen per belastinggebied, per jaar en per m2' \
        as title_text
    {% elif 'bedrijfsvaartuigen' in tablename %}
    'Precariobelasting bedrijfsvaartuigen per belastinggebied, per jaar en per m2' \
        as title_text
    {% elif 'passagiersvaartuigen' in tablename %}
    'Precariobelasting passagiersvaartuigen per belastinggebied, per jaar en per m2' \
        as title_text
    {% elif 'terrassen' in tablename %}
    'Precariobelasting terrassen per belastinggebied, per jaar, per seizoen en per m2' \
        as title_text
    {% else %}
    {{tablename}}
    {% endif %}
    from {{ schema }}.{{ tablename }}
    )
    UPDATE {{ schema }}.{{ tablename }}
    SET title = title_text
    FROM {{ tablename }}_gebied
    WHERE ID = identifier;
    COMMIT;
{% endfor %}
"""
