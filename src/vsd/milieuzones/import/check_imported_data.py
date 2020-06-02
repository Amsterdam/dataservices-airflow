#!/usr/bin/env python
from shared.utils.check_imported_data import (
    run_sql_checks,
    all_valid_url,
    assert_count_minimum,
    assert_count_zero,
)

sql_checks = [
    ("count", "select count(*) from milieuzones_new", assert_count_minimum(5)),
    (
        "columns",
        """
select column_name from information_schema.columns where
 table_schema = 'public' and table_name = 'milieuzones_new'
    """,
        lambda x: x
        == [
            ("ogc_fid",),
            ("wkb_geometry",),
            ("id",),
            ("verkeerstype",),
            ("vanafdatum",),
            ("color",),
        ],
    ),
    (
        "geometrie",
        """
select count(*) from milieuzones_new where
 wkb_geometry is null or ST_GeometryType(wkb_geometry) <> 'ST_MultiPolygon'
    """,
        assert_count_zero(),
    ),
]

if __name__ == "__main__":
    run_sql_checks(sql_checks)
