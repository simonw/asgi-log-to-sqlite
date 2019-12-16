from asgiref.testing import ApplicationCommunicator
from asgi_log_to_sqlite import AsgiLogToSqlite
import sqlite_utils
import pytest


async def hello_world_app(scope, receive, send):
    assert scope["type"] == "http"
    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [[b"content-type", b"application/json"]],
        }
    )
    await send({"type": "http.response.body", "body": b'{"hello": "world"}'})


@pytest.mark.asyncio
async def test_log_to_sqlite(tmpdir):
    logfile = str(tmpdir / "log.db")
    message = {"type": "http", "http_version": "1.0", "method": "GET", "path": "/"}
    db = await get_logged_request_db(logfile, message)
    requests = list(db["requests"].rows)
    assert 1 == len(requests)
    request = requests[0]
    assert {
        "method": "GET",
        "path": 1,
        "query_string": None,
        "user_agent": None,
        "referer": None,
        "accept_language": None,
        "http_status": 200,
        "content_type": 1,
        "client_ip": None,
        "body_size": 18,
    }.items() <= request.items()
    for key in ("start", "duration"):
        assert key in request
        assert isinstance(request[key], float)
    assert [{"id": 1, "name": "/"}] == list(db["paths"].rows)
    assert [{"id": 1, "name": "application/json"}] == list(db["content_types"].rows)


@pytest.mark.asyncio
async def test_log_to_sqlite_with_more_fields(tmpdir):
    logfile = str(tmpdir / "log2.db")
    message = {
        "type": "http",
        "http_version": "1.0",
        "method": "GET",
        "path": "/foo",
        "query_string": b"bar=baz",
        "headers": [
            [b"user-agent", b"NCSA_Mosaic/2.0 (Windows 3.1)"],
            [b"referer", b"ref"],
            [b"accept-language", b"fr-CH, fr;q=0.9, en;q=0.8"],
        ],
        "client": ["127.0.0.1", 6872],
    }
    db = await get_logged_request_db(logfile, message)
    cursor = db.conn.execute(
        """
        select
            method,
            paths.name as path,
            query_strings.name as query_string,
            user_agents.name as user_agent,
            referers.name as referer,
            accept_languages.name as accept_language,
            http_status,
            content_types.name as content_type,
            client_ip
        from
            requests
            left join paths on requests.path = paths.id
            left join query_strings on requests.query_string = query_strings.id
            left join user_agents on requests.user_agent = user_agents.id
            left join referers on requests.referer = referers.id
            left join accept_languages on requests.accept_language = accept_languages.id
            left join content_types on requests.content_type = content_types.id
    """
    )
    columns = [c[0] for c in cursor.description]
    row = dict(zip(columns, cursor.fetchone()))

    assert {
        "method": "GET",
        "path": "/foo",
        "query_string": "?bar=baz",
        "user_agent": "NCSA_Mosaic/2.0 (Windows 3.1)",
        "referer": "ref",
        "accept_language": "fr-CH, fr;q=0.9, en;q=0.8",
        "http_status": 200,
        "content_type": "application/json",
        "client_ip": "127.0.0.1",
    } == row


async def get_logged_request_db(logfile, message):
    "Execute message and return DB record logging request"
    app = AsgiLogToSqlite(hello_world_app, file=logfile)
    instance = ApplicationCommunicator(app, message)
    await instance.send_input({"type": "http.request"})
    assert (await instance.receive_output(1)) == {
        "type": "http.response.start",
        "status": 200,
        "headers": [[b"content-type", b"application/json"]],
    }
    assert (await instance.receive_output(1)) == {
        "type": "http.response.body",
        "body": b'{"hello": "world"}',
    }
    return sqlite_utils.Database(logfile)
