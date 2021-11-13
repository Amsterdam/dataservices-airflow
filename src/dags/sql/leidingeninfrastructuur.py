from typing import Final

# REMOVING ANY INTERMEDIATE TABLES USED FOR MANTELBUIZEN, KABELSONDERGRONDS and KABELSBOVENGRONDS
SQL_REMOVE_TABLE: Final = """
    DROP TABLE IF EXISTS {{ params.tablename }} CASCADE;
"""

# CREATING MANTELBUIZEN TABLE
SQL_MANTELBUIZEN_TABLE: Final = """
    DROP TABLE IF EXISTS {{ params.tablename }} CASCADE;
    CREATE TABLE {{ params.tablename }} AS SELECT
    m.id,
    geometry,
    inwinningstype.naam as inwinningstype,
    thema.naam as thema,
    klasse.naam as klasse,
    mantelbuistype.naam as mantelbuistype,
    janee.naam as zichtbaar,
    materiaal.naam as materiaal,
    diepte,
    nauwkeurigheid.naam as nauwkeurigheid_diepte,
    hoofdcategorie.naam as hoofdcategorie,
    eigenaar.naam as eigenaar,
    jva,
    mantelbuisdiameter.naam as mantelbuisdiameter,
    lengte
    FROM   mantelbuizen m
    INNER JOIN inwinningstype ON inwinningstype.code = m.wijzeinw
    INNER JOIN hoofdcategorie ON hoofdcategorie.code = m.hoofdcat
    INNER JOIN thema ON thema.code = m.thema
    INNER JOIN klasse ON klasse.code = m.klasse
    INNER JOIN mantelbuistype ON mantelbuistype.code = m.type
    INNER JOIN janee ON janee.code = m.zichtbaar
    INNER JOIN materiaal ON materiaal.code = m.materiaal
    INNER JOIN nauwkeurigheid ON nauwkeurigheid.code = m.nauwdiep
    INNER JOIN eigenaar ON eigenaar.code = m.eigenaar
    INNER JOIN mantelbuisdiameter ON mantelbuisdiameter.code = m.diameter;
"""

# CREATING KABELSBOVEN/ONDERGRONDS TABLE
SQL_KABELSBOVEN_OR_ONDERGRONDS_TABLE: Final = """
    DROP TABLE IF EXISTS {{ params.tablename }} CASCADE;
    CREATE TABLE {{ params.tablename }} AS SELECT
    k.id,
    geometry,
    inwinningstype.naam as inwinningstype,
    thema.naam as thema,
    klasse.naam as klasse,
    type.naam as type,
    janee.naam as zichtbaar,
    bovenonder,
    diepte,
    nauwkeurigheid1.naam as nauwkeurigheid_diepte,
    hoogte,
    nauwkeurigheid2.naam as nauwkeurigheid_hoogte,
    hoofdcategorie.naam as hoofdcategorie,
    eigenaar.naam as eigenaar,
    jva,
    typeextra,
    functie.naam as functie,
    van,
    tot,
    kast,
    groep.naam as groep,
    kabeltype.naam as kabeltype,
    kabeldiameter.naam as kabeldiameter,
    voltage,
    fase.naam as fase,
    bouwtype.naam as bouwtype,
    bereikbaar,
    datum,
    lengte
    FROM  kabels k
    INNER JOIN inwinningstype ON inwinningstype.code = k.wijzeinw
    INNER JOIN hoofdcategorie ON hoofdcategorie.code = k.hoofdcat
    INNER JOIN thema ON thema.code = k.thema
    INNER JOIN klasse ON klasse.code = k.klasse
    INNER JOIN type ON type.code = k.type
    INNER JOIN janee ON janee.code = k.zichtbaar
    INNER JOIN nauwkeurigheid nauwkeurigheid1 ON nauwkeurigheid1.code = k.nauwdiep
    INNER JOIN nauwkeurigheid nauwkeurigheid2 ON nauwkeurigheid2.code = k.nauwhoog
    INNER JOIN eigenaar ON eigenaar.code = k.eigenaar
    INNER JOIN functie ON functie.code = k.functie
    INNER JOIN groep ON groep.code = k.groep::char
    INNER JOIN kabeltype ON kabeltype.code = k.typekabel
    INNER JOIN kabeldiameter ON kabeldiameter.code = k.diameter
    INNER JOIN fase ON fase.code = k.fase
    INNER JOIN bouwtype ON bouwtype.code = k.bouwtype
    where LOWER(bovenonder)='{{ params.filter }}';
"""

# CREATING KABELSBOVEN/ONDERGRONDS TABLE
SQL_PUNTEN_TABLE: Final = """
DROP TABLE IF EXISTS {{ params.tablename }} CASCADE;
CREATE TABLE {{ params.tablename }} AS SELECT
    k.id,
    k.geometry,
    inwinningstype.naam as inwinningstype,
    thema.naam as thema,
    klasse.naam as klasse,
    punttype.naam as type,
    janee.naam as zichtbaar,
    bovenonder,
    diepte,
    nauwkeurigheid1.naam as nauwkeurigheid_diepte,
    hoogte,
    nauwkeurigheid2.naam as nauwkeurigheid_hoogte,
    hoofdcategorie.naam as hoofdcategorie,
    eigenaar.naam as eigenaar,
    jva,
    gebruiker,
    datum as mutatiedatum
    FROM  punten k
    INNER JOIN inwinningstype ON inwinningstype.code = k.wijzeinw
    INNER JOIN hoofdcategorie ON hoofdcategorie.code = k.hoofdcat
    INNER JOIN thema ON thema.code = k.thema
    INNER JOIN klasse ON klasse.code = k.klasse
    INNER JOIN punttype ON punttype.code = k.type
    INNER JOIN janee ON janee.code = k.zichtbaar
    INNER JOIN nauwkeurigheid nauwkeurigheid1 ON nauwkeurigheid1.code = k.nauwdiep
    INNER JOIN nauwkeurigheid nauwkeurigheid2 ON nauwkeurigheid2.code = k.nauwhoog
    INNER JOIN eigenaar ON eigenaar.code = k.eigenaar;
"""

# CONVERTING TO GEOMETRY to 2D AND CHECKING CONTENT
SQL_GEOM_CONVERT: Final = """
    ALTER TABLE {{ params.tablename }}
    ALTER COLUMN geometry TYPE geometry USING ST_FORCE2D(geometry::geometry);
    UPDATE {{ params.tablename }} SET geometry = ST_MakeValid(geometry);
    COMMIT;
    {% if 'mantelbuizen' in params.tablename %}
    UPDATE {{ params.tablename }}
    SET GEOMETRY =  ST_CollectionExtract(geometry, 3)
    WHERE GeometryType(geometry) NOT IN ('MULTIPOLYGON');
    COMMIT;
    {% elif 'kabels' in params.tablename %}
    UPDATE {{ params.tablename }}
    SET GEOMETRY =  ST_CollectionExtract(geometry, 2)
    WHERE GeometryType(geometry) NOT IN ('MULTILINESTRING');
    COMMIT;
    {% endif %}
"""

# ALTER DATAYPES THAT OGR2OGR HAS NOT DONE CORRECTLY
SQL_ALTER_DATATYPES: Final = """
    ALTER TABLE {{ params.tablename }} ALTER COLUMN jaar_van_aanleg TYPE integer
    USING jaar_van_aanleg::integer;
    ALTER TABLE {{ params.tablename }} ALTER COLUMN diepte TYPE numeric USING diepte::numeric;
    {% if 'punten' not in params.tablename %}
    ALTER TABLE {{ params.tablename }} ALTER COLUMN lengte TYPE numeric USING lengte::numeric;
    {% endif %}
    {% if 'kabels' in params.tablename %}
    ALTER TABLE {{ params.tablename }} ALTER COLUMN hoogte TYPE numeric USING hoogte::numeric;
    ALTER TABLE {{ params.tablename }} ALTER COLUMN geometry TYPE geometry(MULTILINESTRING, 28992)
    USING ST_SetSRID(geometry, 28992);
    {% elif 'mantelbuizen' in params.tablename %}
    ALTER TABLE {{ params.tablename }} ALTER COLUMN geometry TYPE geometry(MULTIPOLYGON, 28992)
    USING ST_SetSRID(geometry, 28992);
    {% elif 'punten' in params.tablename %}
    ALTER TABLE {{ params.tablename }} ALTER COLUMN geometry TYPE geometry(POINT, 28992)
    USING ST_SetSRID(geometry, 28992);
    {% endif %}
"""
