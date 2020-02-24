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
            if not self.db[table].exists():
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

        referer = header(request_headers, "referer")
        user_agent = header(request_headers, "user-agent")
        accept_language = header(request_headers, "accept-language")

        content_type = header(dict(response_headers), "content-type")

        # Now log to that file
        db = self.db
        with db.conn:
            db["requests"].insert(
                {
                    "start": start,
                    "method": scope["method"],
                    "path": lookup(db, "paths", path),
                    "query_string": lookup(db, "query_strings", query_string),
                    "user_agent": lookup(db, "user_agents", user_agent),
                    "referer": lookup(db, "referers", referer),
                    "accept_language": lookup(db, "accept_languages", accept_language),
                    "http_status": http_status,
                    "content_type": lookup(db, "content_types", content_type),
                    "client_ip": scope.get("client", (None, None))[0],
                    "duration": end - start,
                    "body_size": body_size,
                },
                alter=True,
                foreign_keys=self.lookup_columns,
            )


def header(d, name):
    return d.get(name.encode("utf8"), b"").decode("utf8") or None


def lookup(db, table, value):
    return db[table].lookup({"name": value}) if value else None
