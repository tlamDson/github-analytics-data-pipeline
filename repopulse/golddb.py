"""Move the gold database to a TRANSIENT MotherDuck database (``docs/motherduck-storage.md``).

A standard MotherDuck database keeps failsafe for 7 days, a TRANSIENT one for 1 day, and the choice
is made only when the database is created. So a database created without it has to be copied.

- ``migrate``: create the target with ``CREATE DATABASE <target> (TRANSIENT)`` (an error when the
  name exists, so nothing is ever replaced), copy every table of the source, and fail unless the
  columns, the row count and min/max ``loaded_at`` of every table match. The source is only read.
  Views are not copied: their body names the source database, and ``dbt build`` recreates them.
- ``backup``: ``main_staging.stg_gharchive_events`` to a local parquet file. Refuses to run on
  GitHub Actions: the repo is public and staging holds unmasked logins and hashes.

The MotherDuck token is read by DuckDB from the environment; this module never reads or prints it.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

STAGING = "main_staging.stg_gharchive_events"
LOADED_AT = "loaded_at"
_DB_NAME = re.compile(r"[a-z][a-z0-9_]*")


class GoldDbError(ValueError):
    """Bad database name, missing staging table, or a copy that does not match its source."""


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _relation(database: str, table: str) -> str:
    schema, _, name = table.partition(".")
    return ".".join(_ident(part) for part in (database, schema, name))


# ---------------------------------------------------------------- reading


def relations(con: Any, database: str, table_type: str) -> list[str]:
    """``schema.name`` of every relation of ``database`` (BASE TABLE or VIEW), sorted."""
    rows = con.execute(
        "SELECT table_schema, table_name FROM system.information_schema.tables "
        "WHERE table_catalog = ? AND table_type = ? ORDER BY 1, 2",
        [database, table_type],
    ).fetchall()
    return [f"{schema}.{name}" for schema, name in rows]


def columns(con: Any, relation: str) -> list[tuple[str, str, str]]:
    """(name, type, nullable YES/NO) of every column."""
    return [tuple(row[:3]) for row in con.execute(f"DESCRIBE {relation}").fetchall()]


def stats(con: Any, relation: str) -> dict[str, Any]:
    """Row count and min/max ``loaded_at`` (null without that column) of ``relation``."""
    has_loaded_at = any(
        name == LOADED_AT for name, _, _ in columns(con, f"SELECT * FROM {relation}")
    )
    bounds = (
        f"min({LOADED_AT})::VARCHAR, max({LOADED_AT})::VARCHAR" if has_loaded_at else "NULL, NULL"
    )
    rows, low, high = con.execute(f"SELECT count(*), {bounds} FROM {relation}").fetchone()
    return {"rows": rows, "min_loaded_at": low, "max_loaded_at": high}


# ---------------------------------------------------------------- migrate


def create_transient(con: Any, name: str) -> None:
    """``CREATE DATABASE <name> (TRANSIENT)``; an existing database is an error, never replaced."""
    con.execute(f"CREATE DATABASE {name} (TRANSIENT)")
    row = con.execute(
        "SELECT transient FROM md_information_schema.databases WHERE name = ?", [name]
    ).fetchone()
    if row is None or row[0] is not True:
        raise GoldDbError(f"database {name} was created but is not TRANSIENT; nothing copied")


def _copy_table(con: Any, source: str, target: str, table: str) -> None:
    src, dst = _relation(source, table), _relation(target, table)
    body = ", ".join(
        f"{_ident(name)} {kind}{'' if nullable == 'YES' else ' NOT NULL'}"
        for name, kind, nullable in columns(con, src)
    )
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {_ident(target)}.{_ident(table.partition('.')[0])}")
    con.execute(f"CREATE TABLE {dst} ({body})")
    con.execute(f"INSERT INTO {dst} SELECT * FROM {src}")


def verify(con: Any, source: str, target: str) -> dict[str, dict[str, Any]]:
    """Stats of every table; raises unless both databases have the same tables and they match."""
    src_tables = relations(con, source, "BASE TABLE")
    dst_tables = relations(con, target, "BASE TABLE")
    problems = [f"{t}: only in one database" for t in sorted(set(src_tables) ^ set(dst_tables))]
    found = {}
    for table in sorted(set(src_tables) & set(dst_tables)):
        sides = [_relation(db, table) for db in (source, target)]
        if columns(con, sides[0]) != columns(con, sides[1]):
            problems.append(f"{table}: columns differ")
        want, got = (stats(con, side) for side in sides)
        if want != got:
            problems.append(f"{table}: source {want} target {got}")
        found[table] = got
    if problems:
        raise GoldDbError(f"{target} does not match {source}:\n  " + "\n  ".join(problems))
    return found


def check_names(source: str, target: str) -> None:
    for name in (source, target):
        if not _DB_NAME.fullmatch(name):
            raise GoldDbError(f"database name must match {_DB_NAME.pattern} (got {name!r})")
    if source == target:
        raise GoldDbError(f"source and target are the same database {source!r}")


def migrate(con: Any, source: str, target: str) -> dict[str, Any]:
    """Create ``target`` TRANSIENT, copy every table of ``source`` into it and verify the copy."""
    check_names(source, target)
    tables = relations(con, source, "BASE TABLE")
    if STAGING not in tables:
        raise GoldDbError(f"database {source} has no table {STAGING}; nothing created")
    create_transient(con, target)
    for table in tables:
        _copy_table(con, source, target, table)
    return {
        "source": source,
        "target": target,
        "transient": True,
        "tables": verify(con, source, target),
        "views_not_copied": relations(con, source, "VIEW"),
    }


# ---------------------------------------------------------------- backup


def backup(con: Any, out_dir: Path) -> dict[str, Any]:
    """Staging of the connected database -> ``<out_dir>/stg_gharchive_events.parquet``, checked."""
    if os.environ.get("GITHUB_ACTIONS"):
        raise GoldDbError("backup refuses to run on GitHub Actions: staging is not public data")
    path = out_dir / f"{STAGING.partition('.')[2]}.parquet"
    if path.exists():
        raise GoldDbError(f"{path} exists; choose another --out directory")
    want = stats(con, STAGING)
    out_dir.mkdir(parents=True, exist_ok=True)
    file = _literal(path.as_posix())
    con.execute(f"COPY (SELECT * FROM {STAGING}) TO {file} (FORMAT parquet)")
    got = stats(con, f"read_parquet({file})")
    if got != want:
        raise GoldDbError(f"{path} does not match {STAGING}: table {want} file {got}")
    return {"file": str(path), **got}


# ---------------------------------------------------------------- CLI


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m repopulse.golddb",
        description="Move the gold database to a TRANSIENT MotherDuck database.",
    )
    sub = p.add_subparsers(dest="command", required=True)

    m = sub.add_parser("migrate", help="copy a database into a new TRANSIENT one and verify it")
    m.add_argument("--source", required=True, help="MotherDuck database to read")
    m.add_argument("--target", required=True, help="new MotherDuck database; must not exist")

    b = sub.add_parser("backup", help="staging to a local parquet file (never on Actions)")
    b.add_argument("--database", required=True, help="DuckDB file or md:<database>")
    b.add_argument("--out", type=Path, required=True, help="directory outside the repo")
    return p


def main(argv: Sequence[str] | None = None, *, connect: Callable[[str], Any] | None = None) -> int:
    import duckdb

    args = _parser().parse_args(argv)
    connect = connect or duckdb.connect
    try:
        if args.command == "migrate":
            check_names(args.source, args.target)
            con = connect("md:")
        else:
            con = connect(args.database)
        try:
            if args.command == "migrate":
                report = migrate(con, args.source, args.target)
            else:
                report = backup(con, args.out)
        finally:
            con.close()
    except (GoldDbError, duckdb.Error) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
