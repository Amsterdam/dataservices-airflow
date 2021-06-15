import os

import dsnparse


def fetch_objectstore_credentials(swift_conn_id="swift_default"):
    env_varname = f"AIRFLOW_CONN_{swift_conn_id.upper()}"
    from . import env

    result = {
        "VERSION": "2.0",
        "AUTHURL": env("OS_AUTH_URL"),
        "REGION_NAME": "NL",
    }
    if env_varname not in os.environ and env_varname == "AIRFLOW_CONN_SWIFT_DEFAULT":
        result.update(
            {
                "TENANT_NAME": env("OS_TENANT_NAME"),
                "TENANT_ID": env("OS_TENANT_NAME"),
                "USER": env("OS_USERNAME"),
                "PASSWORD": env("OS_PASSWORD"),
            }
        )
    else:
        airflow_conn_info = dsnparse.parse_environ(env_varname)
        result.update(
            {
                "TENANT_NAME": airflow_conn_info.host,
                "TENANT_ID": airflow_conn_info.host,
                "USER": airflow_conn_info.username,
                "PASSWORD": airflow_conn_info.password,
            }
        )
    return result
