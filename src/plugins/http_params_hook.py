from typing import Any, Optional

import requests
from airflow.hooks.http_hook import HttpHook


class HttpParamsHook(HttpHook):
    """Unfortunately, the default HttpHook makes it impossible
    to combine query params with a payload in the body, so
    we need a subclass with a slightly modified copy of the
    run method
    """

    def run(
        self,
        endpoint: str,
        params: Optional[dict[str, str]] = None,
        data: str = None,
        headers: Optional[dict[str, str]] = None,
        extra_options: Optional[dict[str, Any]] = None,
        **request_kwargs,
    ):
        r"""
        Performs the request

        :param endpoint: the endpoint to be called i.e. resource/v1/query?
        :type endpoint: str
        :param params: query parameters
        :type data: dict
        :param data: payload to be uploaded or request parameters
        :type data: dict
        :param headers: additional headers to be passed through as a dictionary
        :type headers: dict
        :param extra_options: additional options to be used when executing the request
            i.e. {'check_response': False} to avoid checking raising exceptions on non
            2XX or 3XX status codes
        :type extra_options: dict
        :param  \**request_kwargs: Additional kwargs to pass when creating a request.
            For example, ``run(json=obj)`` is passed as ``requests.Request(json=obj)``
        """
        extra_options = extra_options or {}

        session = self.get_conn(headers)

        if (
            self.base_url
            and not self.base_url.endswith("/")
            and endpoint
            and not endpoint.startswith("/")
        ):
            url = self.base_url + "/" + endpoint
        else:
            url = (self.base_url or "") + (endpoint or "")

        # Weird bug in Airflow stripping https when connections is configured as env var
        # re-fetch conn, we need to know the conn_type
        # conn = self.get_connection(self.http_conn_id)
        # if url.startswith("http:") and conn.conn_type == "https":
        #     url = f"https:{url[5:]}"
        req = None
        if self.method == "GET":
            # GET uses params
            req = requests.Request(
                self.method, url, params=params, headers=headers, **request_kwargs
            )
        elif self.method == "HEAD":
            # HEAD doesn't use params
            req = requests.Request(self.method, url, headers=headers, **request_kwargs)
        else:
            # Others use data
            req = requests.Request(
                self.method,
                url,
                data=data,
                params=params,
                headers=headers,
                **request_kwargs,
            )

        prepped_request = session.prepare_request(req)
        self.log.info("Sending '%s' to url: %s", self.method, url)
        return self.run_and_check(session, prepped_request, extra_options)
