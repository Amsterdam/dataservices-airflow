#!/usr/bin/env python
from shared.utils.check_imported_data import (
    run_sql_checks,
    assert_count_zero,
    assert_count_minimum,
)

sql_checks = [
    ("count", "select count(*) from gebieden_grootstedelijke_projecten_new", assert_count_minimum(74)),
    (
        "geometrie",
        """
select count(*) from gebieden_grootstedelijke_projecten_new where
ST_GeometryType(geometrie) <> 'ST_MultiPolygon'
    """,
        assert_count_zero(),
    ),
]

if __name__ == "__main__":
    run_sql_checks(sql_checks)
