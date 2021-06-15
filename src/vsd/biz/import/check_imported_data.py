#!/usr/bin/env python
from shared.utils.check_imported_data import (
    all_valid_url,
    assert_count_minimum,
    assert_count_zero,
    run_sql_checks,
)

sql_checks = [
    ("count", "select count(*) from biz_view_new", assert_count_minimum(48)),
    (
        "columns",
        """
select column_name from information_schema.columns where
 table_schema = 'public' and table_name = 'biz_data_new'
    """,
        lambda x: x
        == [
            ("biz_id",),
            ("naam",),
            ("biz_type",),
            ("heffingsgrondslag",),
            ("website",),
            ("heffing",),
            ("bijdrageplichtigen",),
            ("verordening",),
            ("wkb_geometry",),
        ],
    ),
    (
        "website",
        "select website from biz_view_new where website is not NULL",
        all_valid_url,
    ),
    (
        "verordening",
        "select verordening from biz_view_new where verordening is not NULL",
        all_valid_url,
    ),
    (
        "geometrie",
        """
select count(*) from biz_view_new where
geometrie is null or ST_IsValid(geometrie) = false or ST_GeometryType(geometrie) <> 'ST_Polygon'
    """,
        assert_count_zero(),
    ),
]

if __name__ == "__main__":
    run_sql_checks(sql_checks)
