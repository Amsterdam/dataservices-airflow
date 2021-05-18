#!/usr/bin/env python
from shared.utils.check_imported_data import (
    run_sql_checks,
)

sql_checks = [
    ("count_metro", "select count(*) from trm_metro_new", lambda x: x[0][0] > 700),
    ("count_tram", "select count(*) from trm_tram_new", lambda x: x[0][0] > 4200),
    (
        "columns_tram",
        """
select column_name from information_schema.columns where table_schema = 'public' and table_name = 'trm_tram_new'
                    """,
        lambda x: {"ogc_fid", "wkb_geometry", "volgorde"} <= set(map(lambda y: y[0], x)),
    ),
    (
        "columns_metro",
        """
select column_name from information_schema.columns where table_schema = 'public' and table_name = 'trm_metro_new'
                    """,
        lambda x: {"ogc_fid", "wkb_geometry", "kge"} <= set(map(lambda y: y[0], x)),
    ),
]


if __name__ == "__main__":
    run_sql_checks(sql_checks)
