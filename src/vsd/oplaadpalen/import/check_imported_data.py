#!/usr/bin/env python
from shared.utils.check_imported_data import (
    run_sql_checks,
    assert_count_minimum,
    assert_count_zero,
)

sql_checks = [
    ("count", "select count(*) from oplaadpalen_new", assert_count_minimum(10)),
    (
        "geometrie",
        """
select count(*) from oplaadpalen_new where
wkb_geometry is null or ST_IsValid(wkb_geometry) = false or ST_GeometryType(wkb_geometry) <> 'ST_Point'
    """,
        assert_count_zero(),
    ),
]

if __name__ == "__main__":
    run_sql_checks(sql_checks)
