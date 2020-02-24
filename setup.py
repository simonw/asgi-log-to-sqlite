from setuptools import setup
import os

VERSION = "0.1.1"


def get_long_description():
    with open(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "README.md"),
        encoding="utf8",
    ) as fp:
        return fp.read()


setup(
    name="asgi-log-to-sqlite",
    description="ASGI middleware for logging traffic to a SQLite database",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    author="Simon Willison",
    url="https://github.com/simonw/asgi-log-to-sqlite",
    license="Apache License, Version 2.0",
    version=VERSION,
    py_modules=["asgi_log_to_sqlite"],
    install_requires=["sqlite_utils~=2.3.1"],
    extras_require={"test": ["pytest", "pytest-asyncio", "asgiref==3.1.2"]},
)
