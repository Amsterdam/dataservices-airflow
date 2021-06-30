from typing import Final

# Before creating the target table based on the schema definition
# make sure it is dropped if exists.
DROP_TABLE_IF_EXISTS: Final = """
DROP TABLE IF EXISTS {{ params.tablename }};
"""

# Check if table contains source data.
# If the source does not have data (no electric blackouts present)
# then the source column `storing_nummer` is not present.
# In the latter case, this operation will crash. Then in the DAG flow this
# will trigger a different path to execute.
CHECK_TABLE: Final = """
SELECT storing_nummer FROM {{ params.tablename }};
"""

# The source data contains epoch times.
# In order to translate it to a timestamp, the data is updated.
# The dates are stored in millseconds.
# Because Unix timestamps measures time with seconds, and not milliseconds (in Postgres too),
# the values must be devided by 1000 before converting to timestamp.
# Casting to ::int is used to prevent an error when there is null value present and the
# ogr2ogr import interpret this a char datatype.
CONVERT_DATE_TIME: Final = """
ALTER TABLE {{ params.tablename }} ALTER COLUMN storing_datum_gemeld TYPE TIMESTAMP
    WITH TIME zone using TO_TIMESTAMP(storing_datum_gemeld::int/1000);
ALTER TABLE {{ params.tablename }} ALTER COLUMN storing_datum_schatting TYPE TIMESTAMP
    WITH TIME zone using TO_TIMESTAMP(storing_datum_schatting::int/1000);
ALTER TABLE {{ params.tablename }} ALTER COLUMN storing_datum_eind TYPE TIMESTAMP
    WITH TIME zone using TO_TIMESTAMP(storing_datum_eind::int/1000);
ALTER TABLE {{ params.tablename }} ALTER COLUMN date_end TYPE TIMESTAMP
    WITH TIME zone using TO_TIMESTAMP(date_end::int/1000);
ALTER TABLE {{ params.tablename }} ALTER COLUMN storing_datum_change TYPE TIMESTAMP
    WITH TIME zone using TO_TIMESTAMP(storing_datum_change::int/1000);
ALTER TABLE {{ params.tablename }} ALTER COLUMN storing_service_update TYPE TIMESTAMP
    WITH TIME zone using TO_TIMESTAMP(storing_service_update::int/1000);
ALTER TABLE {{ params.tablename }} ALTER COLUMN log_date TYPE TIMESTAMP
    WITH TIME zone using TO_TIMESTAMP(log_date::int/1000);
COMMIT;

# Due to the nature of the data, it can happen that there are no electric
# blackouts present at the current time (polling every day every 10 minutes).
# In that case a dummy record is added indicating there is no electric blackout.
# The text NO DATA is projected as a multipolygon (requested by the user).
NO_DATA_PRESENT_INDICATOR: Final = """
INSERT INTO {{ params.tablename }} (ID, geometry)
VALUES (-1, ST_Transform(ST_GeomFromText('MULTIPOLYGON (((
        4.848844913545913 52.3728446270015,
        4.8491882362998195 52.35989958374957,
        4.854338077608413 52.35995200047924,
        4.85416641623146 52.367237320916075,
        4.863436130586929 52.35989958374955,
        4.868500141207046 52.35995200047924,
        4.868070987764663 52.37294942968427,
        4.862835315767593 52.37289702837396,
        4.863178638521499 52.36566505125643,
        4.853393940035171 52.372897028373934,
        4.848844913545913 52.3728446270015)),
        ((4.8717268646075595 52.37288945263481,
        4.8717268646075595 52.36010167236266,
        4.885288113386856 52.36010167236266,
        4.885288113386856 52.37299425521125,
        4.8717268646075595 52.37288945263481)),
        ((4.887979062849475 52.366501085785295,
        4.897248777204943 52.366501085785295,
        4.89707711582799 52.37331372125051,
        4.900853666120959 52.37331372125051,
        4.901196988874865 52.35937289637421,
        4.887979062849475 52.359477731020846,
        4.887979062849475 52.366501085785295)),
        ((4.906690152937365 52.37341852282017,
        4.910466703230334 52.37352332414115,
        4.917676481062365 52.35926806147886,
        4.913728269392443 52.35937289637421,
        4.91081002598424 52.36671072100576,
        4.907720121199084 52.36660590351987,
        4.906518491560412 52.35916322633481,
        4.90308526402135 52.35916322633481,
        4.906690152937365 52.37341852282017)),
        ((4.917565308706964 52.37377017014901,
        4.934044800894464 52.37377017014901,
        4.934044800894464 52.37146449652752,
        4.928723298208917 52.371359690321206,
        4.929066620962823 52.358885976072585,
        4.924603425162042 52.35878114002205,
        4.924775086538995 52.371359690321206,
        4.917565308706964 52.37115007716251,
        4.917565308706964 52.37377017014901)),
        ((4.9390181243731845 52.373832666869966,
        4.943652981550919 52.37393746720823,
        4.949317806990372 52.35810979967158,
        4.94536959532045 52.35821463731483,
        4.942966336043106 52.365867113500556,
        4.939361447127091 52.365971932739335,
        4.936614865095841 52.358004961779635,
        4.932323330672013 52.35790012363894,
        4.9390181243731845 52.373832666869966)))',4326), 28992));
COMMIT;
"""
