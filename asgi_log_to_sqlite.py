import sqlite_utils
import time


class AsgiLogToSqlite:
    lookup_columns = (
        "path",
        "user_agent",
        "referer",
        "accept_language",
        "content_type",
        "query_string",
    )

    def __init__(self, app, file):
        self.app = app
        self.db = sqlite_utils.Database(file)
        self.file = file
        self.ensure_tables()

    def ensure_tables(self):
        for column in self.lookup_columns:
            table = "{}s".format(column)
            if not self.db[table].exists:
                self.db[table].create({"id": int, "name": str}, pk="id")
        if "requests" not in self.db.table_names():
            self.db["requests"].create(
                {
                    "start": float,
                    "method": str,
                    "path": int,
                    "query_string": int,
                    "user_agent": int,
                    "referer": int,
                    "accept_language": int,
                    "http_status": int,
                    "content_type": int,
                    "client_ip": str,
                    "duration": float,
                    "body_size": int,
                },
                foreign_keys=self.lookup_columns,
            )

    async def __call__(self, scope, receive, send):
        response_headers = []
        body_size = 0
        http_status = None

        async def wrapped_send(message):
            nonlocal body_size, response_headers, http_status
            if message["type"] == "http.response.start":
                response_headers = message["headers"]
                http_status = message["status"]

            if message["type"] == "http.response.body":
                body_size += len(message["body"])

            await send(message)

        start = time.time()
        await self.app(scope, receive, wrapped_send)
        end = time.time()

        path = str(scope["path"])
        query_string = None
        if scope.get("query_string"):
            query_string = "?{}".format(scope["query_string"].decode("utf8"))

        request_headers = dict(scope.get("headers") or [])

        referer = request_headers.get(b"referer", b"").decode("utf8") or None
        user_agent = request_headers.get(b"user-agent", b"").decode("utf8") or None
        accept_language = (
            request_headers.get(b"accept-language", b"").decode("utf8") or None
        )

        content_type = dict(response_headers).get(b"content-type", b"").decode("utf8")

        # Now log to that file
        with self.db.conn:
            self.db["requests"].insert(
                {
                    "start": start,
                    "method": scope["method"],
                    "path": self.db["paths"].lookup({"name": path}),
                    "query_string": self.db["query_strings"].lookup(
                        {"name": query_string}
                    )
                    if query_string
                    else None,
                    "user_agent": self.db["user_agents"].lookup({"name": user_agent})
                    if user_agent
                    else None,
                    "referer": self.db["referers"].lookup({"name": referer})
                    if referer
                    else None,
                    "accept_language": self.db["accept_languages"].lookup(
                        {"name": accept_language}
                    )
                    if accept_language
                    else None,
                    "http_status": http_status,
                    "content_type": self.db["content_types"].lookup(
                        {"name": content_type}
                    )
                    if content_type
                    else None,
                    "client_ip": scope.get("client", (None, None))[0],
                    "duration": end - start,
                    "body_size": body_size,
                },
                alter=True,
                foreign_keys=self.lookup_columns,
            )
