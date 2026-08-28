"""Debug helpers for interactive test development.

Output only appears when pytest is run with log_cli enabled, e.g.:
    pytest -o log_cli=true --log-cli-level=DEBUG path/to/test.py
"""
import logging
from pprint import pformat

log = logging.getLogger("tests.debug")


def dump(rows, title=None):
    """Pretty-log a result set from get_object() or a cursor."""
    if title:
        w1 = 80
        w2 = round((w1 - len(title) - 4)/2)
        log.debug("\n%s\n%s  %s  %s\n%s", "="*w1,"="*w2, title, "="*w2, "="*w1)
    if not rows:
        log.debug("(empty)")
        return
    for r in rows:
        log.debug("\n%s", pformat(dict(r) if hasattr(r, "keys") else r))


def dump_table(cursor, table, where=None, limit=20):
    """Quick peek at a table's contents."""
    sql = f"SELECT * FROM {table}"
    if where:
        sql += f" WHERE {where}"
    sql += f" LIMIT {limit}"
    cursor.execute(sql)
    dump(cursor.fetchall(), title=table)
