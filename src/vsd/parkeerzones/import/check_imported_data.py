#!/usr/bin/env python
from shared.utils.check_imported_data import (
    assert_count_minimum,
    assert_count_zero,
    run_sql_checks,
)

sql_checks = [
    ("count", "select count(*) from parkeerzones_new", assert_count_minimum(100)),
    (
        "count_uitz",
        "select count(*) from parkeerzones_uitz_new",
        assert_count_minimum(60),
    ),
    (
        "geometrie",
        """
select count(*) from parkeerzones_new where
wkb_geometry is null or ST_GeometryType(wkb_geometry) <> 'ST_Polygon'
    """,
        assert_count_zero(),
    ),
    (
        "geometrie",
        """
    select count(*) from parkeerzones_uitz_new where
    wkb_geometry is null or ST_GeometryType(wkb_geometry) <> 'ST_Polygon'
        """,
        assert_count_zero(),
    ),
    #    ('columns', """
    # select column_name from information_schema.columns where
    # table_schema = 'public' and table_name = 'hoofdroutes_new'
    #    """, lambda x: x == [("ogc_fid",), ("wkb_geometry",), ("id",), ("name",), ("route",), ("type",)]),
]

if __name__ == "__main__":
    run_sql_checks(sql_checks)
