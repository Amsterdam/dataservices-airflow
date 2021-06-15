#!/usr/bin/env python
from shared.utils.check_imported_data import (
    assert_count_minimum,
    assert_count_zero,
    run_sql_checks,
)

sql_checks = [
    ("count", "select count(*) from bb_quotum_new", assert_count_minimum(90)),
    (
        "count",
        """
select count(column_name) from information_schema.columns where
 table_schema = 'public' and table_name = 'bb_quotum_new'
 and column_name in ('wijk', 'availability_color', 'geo')
    """,
        assert_count_minimum(3),
    ),
    (
        "geometrie",
        """
select count(*) from bb_quotum_new where
geo is null or ST_IsValid(geo) = false or ST_GeometryType(geo) <> 'ST_MultiPolygon'
    """,
        assert_count_zero(),
    ),
]

if __name__ == "__main__":
    run_sql_checks(sql_checks)
