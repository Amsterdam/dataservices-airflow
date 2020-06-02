#!/usr/bin/env python
from shared.utils.check_imported_data import (
    run_sql_checks,
    all_valid_url,
    assert_count_minimum,
    assert_count_zero,
)

sql_checks = [
    ("count", "select count(*) from winkgeb_new", assert_count_minimum(75)),
    #     ('columns', """
    # select column_name from information_schema.columns where
    # table_schema = 'public' and table_name = 'vezips_new'
    #     """, lambda x: x == [('ogc_fid',), ('wkb_geometry',), ('soortcode',), ('vezip_nummer',), ('vezip_type',),
    #                          ('standplaats',), ('jaar_aanleg',), ('venstertijden',), ('toegangssysteem',),
    #                          ('beheerorganisatie',), ('bijzonderheden',)]),
    (
        "geometrie",
        """
select count(*) from winkgeb_new where
wkb_geometry is null or ST_GeometryType(wkb_geometry) not in ('ST_Polygon', 'ST_MultiPolygon')
    """,
        assert_count_zero(),
    ),
]

if __name__ == "__main__":
    run_sql_checks(sql_checks)
