from airflow.hooks.http_hook import HttpHook


def download_file(url: str, destination, data=None, headers=None, http_conn_id="http_default"):
    """Perform an HTTP download using Airflow's HttpHook"""
    if http_conn_id and "://" in url:
        raise ValueError("Use http_conn_id=None when downloading from FQDN-urls")

    http = HttpHook(method="GET", http_conn_id=http_conn_id)
    response = http.run(
        url,
        data=data,
        headers=headers,
        extra_options={"check_response": False, "stream": True, "verify": True},
    )
    with response:
        # this has to be inside with statement for stream=True
        http.check_response(response)

        # Write the stream, decode gzip on the fly.
        with open(destination, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # filter out keep-alive new chunks
                    file.write(chunk)
    return response
