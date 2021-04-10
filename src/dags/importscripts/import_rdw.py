import requests


def download_data(endpoint: str, output_file: str) -> None:
    """Retrieves data from source RDW

    Note:
    The RDW source has several options to retreive data (API, download).
    Since the resources needed for the RDW dataset contain a lot of data,
    going up to a couple of GB's per resource, the RDW resources are collected
    by means of a csv download.
    Getting the data by csv is by far the fastest way to retreive the data, then
    for instance getting the data by means of several API calls.

    Args:
        endpoint: the URL specification for the data service to call
        output_file: Name of file to save data

    Result:
        Stores the data in csv to a file.

    """
    with open(output_file, "w+") as f:
        try:
            request = requests.get(endpoint)
            request.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise SystemExit(e) from e
        else:
            f.truncate()
            f.write(request.text)
