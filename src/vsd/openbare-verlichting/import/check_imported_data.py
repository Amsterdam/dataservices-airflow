#!/usr/bin/env python
from shared.utils.check_imported_data import run_sql_checks, assert_count_minimum

sql_checks = [
    (
        "count",
        "select count(*) from openbare_verlichting_new",
        assert_count_minimum(129410),
    ),
]

if __name__ == "__main__":
    run_sql_checks(sql_checks)
