#!/usr/bin/env python
from shared.utils.check_imported_data import (
    assert_count_minimum,
    assert_count_zero,
    run_sql_checks,
)

sql_checks = [
    ("count", "select count(*) from bekendmakingen_new", assert_count_minimum(1000)),
    (
        "columns",
        """
select column_name from information_schema.columns where
table_schema = 'public' and table_name = 'bekendmakingen_new'
    """,
        lambda x: x
        == [
            ("ogc_fid",),
            ("wkb_geometry",),
            ("id",),
            ("oid_",),
            ("categorie",),
            ("onderwerp",),
            ("titel",),
            ("beschrijving",),
            ("url",),
            ("postcodehuisnummer",),
            ("plaats",),
            ("straat",),
            ("datum",),
            ("overheid",),
        ],
    ),
    (
        "geometrie",
        """
select count(*) from bekendmakingen_new where
wkb_geometry is null or ST_IsValid(wkb_geometry) = false or ST_GeometryType(wkb_geometry) <> 'ST_Point'
    """,
        assert_count_zero(),
    ),
]

if __name__ == "__main__":
    run_sql_checks(sql_checks)
