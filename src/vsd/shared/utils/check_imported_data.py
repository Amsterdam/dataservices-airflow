#!/usr/bin/env python
import logging

import psycopg2
from common.db import fetch_pg_env_vars
from validator_collection import checkers

log = logging.getLogger(__name__)

pg_env_vars = fetch_pg_env_vars()
dbname = pg_env_vars["PGDATABASE"]
user = pg_env_vars["PGUSER"]
password = pg_env_vars["PGPASSWORD"]
host = pg_env_vars["PGHOST"]
port = pg_env_vars["PGPORT"]


def assert_count_zero():
    return lambda x: x[0][0] == 0


def assert_count_minimum(target):
    return lambda x: x[0][0] >= target


def all_valid_url(urls):
    return all(checkers.is_url(url[0]) for url in urls)


def do_checks(conn, sql_checks):
    with conn.cursor() as curs:
        for (name, sql, check) in sql_checks:
            print(f"check {name}")
            curs.execute(sql)
            result = curs.fetchall()
            if not check(result):
                print(f"Failed : {sql}")
                print(f"result : {result}")
                return False

    return True


def run_sql_checks(sql_checks):
    try:
        dsn = f"dbname='{dbname}' user='{user}' password='{password}' host='{host}' port={port}"
        with psycopg2.connect(dsn) as conn:
            result = do_checks(conn, sql_checks)
            if not result:
                exit(1)
    except psycopg2.Error as exc:
        print("Unable to connect to the database", exc)
        exit(1)

    print("All checks passed")
    exit(0)
