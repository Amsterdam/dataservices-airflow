import requests, json


def schema_fetch_url_file(schema_url_file):
    """Return schemadata from URL or File"""

    if not schema_url_file.startswith("http"):
        with open(schema_url_file) as f:
            schema_location = json.load(f)
    else:
        response = requests.get(schema_url_file)
        response.raise_for_status()
        schema_location = response.json()

    return schema_location
