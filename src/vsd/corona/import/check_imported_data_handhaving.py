#!/usr/bin/env python
from shared.utils.check_imported_data import (
    run_sql_checks,
    assert_count_minimum,
)

sql_checks = [
    ("count", "select count(*) from corona_handhaving_new", assert_count_minimum(30)),
    (
        "columns",
        """
    select count(column_name)
    from information_schema.columns
    where table_schema = 'public'
    and table_name = 'corona_handhaving_new'
    and column_name in (
        'id',
        'organisatie',
        'type_interventie',
        'aantal',
        'week_nummer'
    )
    """,
        assert_count_minimum(5),
    ),
]

if __name__ == "__main__":
    run_sql_checks(sql_checks)
