import logging
from typing import Any

from clickhouse_driver import Client


class ClickhouseNativeHook:
    def __init__(
        self,
        login: str,
        password: str,
        host: str = "ch_server",
        port: int = 9000,
        secure: bool = False,
        *args,
        **kwargs,
    ):
        self.login = login
        self.password = password
        self.host = host
        self.port = port
        self.secure = secure
        self.args = args
        self.kwargs = kwargs

    def get_connection(self):
        client = Client(
            host=self.host,
            port=self.port,
            user=self.login,
            password=self.password,
            secure=self.secure,
            *self.args,
            **self.kwargs,
        )
        return client

    def execute(self, query: str, stream: bool = False, log_query: bool = True, *args, **kwargs) -> Any:
        conn = self.get_connection()
        if log_query:
            self._log_query(query=query)
        if stream is True:
            return conn.execute_iter(query=query, *args, **kwargs)
        return conn.execute(query=query, *args, **kwargs)

    @staticmethod
    def _log_query(query):
        message = "Executing query"
        logging.info(msg=f"{message}: \n{query}")
