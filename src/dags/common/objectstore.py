import dsnparse
import os


def fetch_objectstore_credentials(swift_conn_id="swift_default"):

    env_varname = f"AIRFLOW_CONN_{swift_conn_id.upper()}"
    from . import env
    if not env(env_varname) and env_varname == "AIRFLOW_CONN_SWIFT_DEFAULT":
        os.environ["AIRFLOW_CONN_SWIFT_DEFAULT"] = \
            f"s3://{env('OS_USERNAME')}:{env('OS_PASSWORD')}@{env('OS_TENANT_NAME')}"
    airflow_conn_info = dsnparse.parse_environ(env_varname)
    return {
        "VERSION": "2.0",
        "AUTHURL": env("OS_AUTH_URL"),
        "TENANT_NAME": airflow_conn_info.host,
        "TENANT_ID": airflow_conn_info.host,
        "USER": airflow_conn_info.username,
        "PASSWORD": airflow_conn_info.password,
        "REGION_NAME": "NL",
    }
