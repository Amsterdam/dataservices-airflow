#!/usr/bin/env python
import logging
import random

import psycopg2
from various_small_datasets.settings import DATABASES

log = logging.getLogger(__name__)

database = DATABASES["default"]
dbname = database["NAME"]
user = database["USER"]
password = database["PASSWORD"]
host = database["HOST"]
port = database["PORT"]

all_colors = {
    "#009DEC",
    "#00A03C",
    "#BED200",
    "#FF9100",
    "#E50082",
    "#A00078",
    "#EC0000",
    "#767676",
}


def assign_colors(conn, table_name):
    """
    This script will assign colors randomly to parkeerzones in such a way that

    parkeerzones next to each other will not have the same color
    parkeerzones with the same parent will have the same color

    Because subsequest imports should not change colors, especially if the data
    does not change. This script should only be used once . Then the colors will be fixed

    Therefore the assigned  colers were saved in parkerz_zones_map_color table which should be
    used in the import for stting the colors

    :param conn:
    :param table_name:
    :return:
    """
    rows_changed = 0
    with conn.cursor() as curs:
        # curs.execute(f"UPDATE {table_name} SET color = NULL")
        curs.execute("DROP VIEW IF EXISTS parkeerzones_color")
        curs.execute(
            f"""
CREATE VIEW parkeerzones_color AS
SELECT COALESCE(parent, cast(ogc_fid as varchar(80))) AS id, parent is not null as is_group,
ST_Union(ST_SnapToGrid(wkb_geometry, 0.0001)) as wkb_geometry, color::varchar(7)
 FROM {table_name} WHERE SHOW = 'TRUE'
 GROUP BY id, color, is_group;
        """
        )

        curs.execute("SELECT id, is_group FROM parkeerzones_color WHERE color IS NULL")
        parkeerzones = curs.fetchall()

        for parkeerzone in parkeerzones:
            (id, is_group) = parkeerzone
            curs.execute(
                """
SELECT B.color FROM parkeerzones_color A JOIN parkeerzones_color B ON ST_Touches(A.wkb_geometry, B.wkb_geometry)
 WHERE A.id = %s AND B.color IS NOT NULL
            """,
                (id,),
            )
            color_lists = curs.fetchall()
            used_colors = set([col[0] for col in color_lists])
            available_colors = all_colors - used_colors
            if not available_colors:
                log.error("Unable to assign colors. Exiting")
                exit(1)
            new_color = random.choice(tuple(available_colors))
            if is_group:
                curs.execute(
                    f"UPDATE {table_name} SET color = %s WHERE parent = %s",
                    (new_color, id),
                )
            else:
                curs.execute(
                    f"UPDATE {table_name} SET color = %s WHERE ogc_fid = %s",
                    (new_color, id),
                )
            rows_changed += curs.rowcount

        curs.execute()
    conn.commit("DROP VIEW parkeerzones_color")
    return rows_changed == len(parkeerzones)


def main():
    table_name = "parkeerzones"
    try:
        dsn = f"dbname='{dbname}' user='{user}' password='{password}' host='{host}' port={port}"
        with psycopg2.connect(dsn) as conn:
            result = assign_colors(conn, table_name)
            if not result:
                exit(1)
    except psycopg2.Error as exc:
        print("Unable to connect to the database", exc)
        exit(1)

    print("All colors assigned")
    exit(0)


if __name__ == "__main__":
    main()
