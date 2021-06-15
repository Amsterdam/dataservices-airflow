#!/usr/bin/env python
from shared.utils.check_imported_data import assert_count_minimum, run_sql_checks

sql_checks = [
    ("count", "select count(*) from corona_gevallen_new", assert_count_minimum(300)),
    (
        "columns",
        """
    select count(column_name) from information_schema.columns where
    table_schema = 'public' and table_name = 'corona_gevallen_new'
    and column_name in ('id', 'datum', 'gemeente_naam',
            'gemeente_code', 'provincie', 'aantal_totaal',
            'aantal_ziekenhuisopnames', 'aantal_sterfgevallen')
    """,
        assert_count_minimum(8),
    ),
]

if __name__ == "__main__":
    run_sql_checks(sql_checks)
