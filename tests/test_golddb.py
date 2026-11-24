"""repopulse.golddb: create the gold database TRANSIENT, copy repopulse into it, back staging up.

No cloud: ``FakeMotherDuck`` is a real local DuckDB with attached files. Only the two statements
DuckDB has no local form of are translated, as written in the MotherDuck docs (read 2026-11-23):
``CREATE DATABASE <name> (TRANSIENT)`` (error when the name exists) and the ``transient`` column of
``md_information_schema.databases``. Everything else runs unchanged on DuckDB.
"""

from __future__ import annotations

import json
import re

import duckdb
import pytest

from repopulse import golddb
from repopulse.golddb import GoldDbError

STAGING = "main_staging.stg_gharchive_events"
FACTS = "main_gold.fct_events"
MART = "main_public.pub_mart_daily"


def fill_source(con, db: str) -> None:
    for schema in ("main_staging", "main_gold", "main_public"):
        con.execute(f"CREATE SCHEMA {db}.{schema}")
    con.execute(
        f"""CREATE TABLE {db}.{STAGING} (
            event_id VARCHAR, commit_author_email_hashes VARCHAR[], loaded_at TIMESTAMP)"""
    )
    con.execute(
        f"""INSERT INTO {db}.{STAGING} VALUES
            ('1', ['a', 'b'], '2026-01-05 00:00:01.000001'),
            ('2', NULL, '2026-01-06 00:00:02'),
            ('3', [], '2026-01-07 00:00:03.5')"""
    )
    con.execute(
        f"""CREATE TABLE {db}.{FACTS} (
            event_id VARCHAR NOT NULL, "select" DECIMAL(9, 2), loaded_at TIMESTAMP)"""
    )
    con.execute(f"INSERT INTO {db}.{FACTS} VALUES ('1', 1.25, '2026-01-05 00:00:01')")
    con.execute(f"CREATE TABLE {db}.{MART} (day DATE, events BIGINT)")
    con.execute(f"INSERT INTO {db}.{MART} VALUES ('2026-01-05', 3)")
    # dbt-duckdb writes the database name into the view body
    con.execute(f"CREATE VIEW {db}.main_gold.v_events AS SELECT * FROM {db}.{FACTS}")


class FakeMotherDuck:
    def __init__(self, tmp_path, *, transient=True, after_copy=None):
        self.tmp_path, self.transient, self.after_copy = tmp_path, transient, after_copy
        self.statements: list[str] = []
        self.con = duckdb.connect()
        self.con.execute("CREATE SCHEMA md_information_schema")
        self.con.execute(
            "CREATE TABLE md_information_schema.databases (name VARCHAR, transient BOOLEAN)"
        )
        self.add_database("repopulse", transient=False)
        fill_source(self.con, "repopulse")

    def add_database(self, name, *, transient):
        path = (self.tmp_path / f"{name}.duckdb").as_posix()
        self.con.execute(f"ATTACH '{path}' AS {name}")
        self.con.execute(
            "INSERT INTO md_information_schema.databases VALUES (?, ?)", [name, transient]
        )

    def execute(self, sql, params=None):
        self.statements.append(sql)
        created = re.fullmatch(r"CREATE DATABASE (\w+) \(TRANSIENT\)", sql)
        if created:
            name = created.group(1)
            if (self.tmp_path / f"{name}.duckdb").exists():
                raise duckdb.CatalogException(f"database {name} already exists")
            self.add_database(name, transient=self.transient)
            return self.con
        out = self.con.execute(sql, params) if params is not None else self.con.execute(sql)
        if self.after_copy and sql.startswith("INSERT INTO") and "stg_gharchive_events" in sql:
            self.after_copy(self.con)
        return out

    def rows(self, sql):
        return self.con.execute(sql).fetchall()

    def close(self):
        pass


def run_migrate(md, *args):
    return golddb.main(["migrate", *args], connect=lambda target: md)


# ---------------------------------------------------------------- migrate


def test_migrate_creates_a_transient_database_and_copies_every_table(tmp_path, capsys):
    md = FakeMotherDuck(tmp_path)
    assert run_migrate(md, "--source", "repopulse", "--target", "repopulse_transient") == 0
    assert "CREATE DATABASE repopulse_transient (TRANSIENT)" in md.statements
    for table in (STAGING, FACTS, MART):
        assert md.rows(f"SELECT * FROM repopulse_transient.{table} ORDER BY 1") == md.rows(
            f"SELECT * FROM repopulse.{table} ORDER BY 1"
        )
    report = json.loads(capsys.readouterr().out)
    assert report["transient"] is True
    assert report["tables"][STAGING] == {
        "rows": 3,
        "min_loaded_at": "2026-01-05 00:00:01.000001",
        "max_loaded_at": "2026-01-07 00:00:03.5",
    }
    assert report["tables"][MART] == {"rows": 1, "min_loaded_at": None, "max_loaded_at": None}


def test_migrate_keeps_column_types_and_not_null(tmp_path):
    md = FakeMotherDuck(tmp_path)
    assert run_migrate(md, "--source", "repopulse", "--target", "gold_t") == 0
    for table in (STAGING, FACTS, MART):
        assert md.rows(f"DESCRIBE gold_t.{table}") == md.rows(f"DESCRIBE repopulse.{table}")
    with pytest.raises(duckdb.ConstraintException):
        md.con.execute(f"INSERT INTO gold_t.{FACTS} VALUES (NULL, 0, NULL)")


def test_migrate_reports_views_and_does_not_copy_them(tmp_path, capsys):
    # A copied view would keep reading the source database; dbt build recreates it.
    md = FakeMotherDuck(tmp_path)
    assert run_migrate(md, "--source", "repopulse", "--target", "gold_t") == 0
    assert json.loads(capsys.readouterr().out)["views_not_copied"] == ["main_gold.v_events"]
    with pytest.raises(duckdb.CatalogException):
        md.con.execute("SELECT * FROM gold_t.main_gold.v_events")


def test_migrate_never_replaces_an_existing_database(tmp_path, capsys):
    md = FakeMotherDuck(tmp_path)
    md.add_database("gold_t", transient=True)
    md.con.execute("CREATE TABLE gold_t.keep AS SELECT 1 AS x")
    assert run_migrate(md, "--source", "repopulse", "--target", "gold_t") == 1
    assert "already exists" in capsys.readouterr().err
    assert md.rows("SELECT x FROM gold_t.keep") == [(1,)]
    assert md.rows("SELECT table_name FROM duckdb_tables() WHERE database_name = 'gold_t'") == [
        ("keep",)
    ]


def test_migrate_only_reads_the_source(tmp_path):
    md = FakeMotherDuck(tmp_path)
    assert run_migrate(md, "--source", "repopulse", "--target", "gold_t") == 0
    for sql in md.statements:
        assert not re.search(r"\b(drop|alter|delete|update|truncate|replace)\b", sql, re.I), sql
        if re.match(r"\s*(create|insert)\b", sql, re.I):
            head = sql.split(" SELECT ")[0]
            assert "gold_t" in head and "repopulse" not in head, sql


def test_migrate_refuses_a_target_that_is_not_transient_before_copying(tmp_path, capsys):
    md = FakeMotherDuck(tmp_path, transient=False)
    assert run_migrate(md, "--source", "repopulse", "--target", "gold_t") == 1
    assert "not TRANSIENT" in capsys.readouterr().err
    assert not [s for s in md.statements if s.startswith("INSERT")]


@pytest.mark.parametrize(
    "change",
    [
        "INSERT INTO repopulse.{t} VALUES ('4', NULL, '2026-01-06')",
        "UPDATE repopulse.{t} SET loaded_at = '2020-01-01' WHERE event_id = '1'",
        "UPDATE repopulse.{t} SET loaded_at = '2030-01-01' WHERE event_id = '3'",
    ],
    ids=["rows", "min", "max"],
)
def test_migrate_fails_when_a_table_differs(tmp_path, capsys, change):
    md = FakeMotherDuck(tmp_path, after_copy=lambda con: con.execute(change.format(t=STAGING)))
    assert run_migrate(md, "--source", "repopulse", "--target", "gold_t") == 1
    err = capsys.readouterr().err
    assert STAGING in err and FACTS not in err


def test_migrate_fails_when_the_source_gained_a_table(tmp_path, capsys):
    md = FakeMotherDuck(
        tmp_path, after_copy=lambda con: con.execute("CREATE TABLE repopulse.late AS SELECT 1 AS x")
    )
    assert run_migrate(md, "--source", "repopulse", "--target", "gold_t") == 1
    assert "main.late" in capsys.readouterr().err


def test_migrate_fails_on_a_source_without_staging_before_creating(tmp_path, capsys):
    md = FakeMotherDuck(tmp_path)
    md.add_database("empty_db", transient=False)
    assert run_migrate(md, "--source", "empty_db", "--target", "gold_t") == 1
    assert STAGING in capsys.readouterr().err
    assert not [s for s in md.statements if s.startswith("CREATE")]


@pytest.mark.parametrize(
    ("source", "target"),
    [
        ("repopulse", "repopulse"),
        ("repopulse", "x; DROP DATABASE repopulse"),
        ("repopulse", ""),
        ("repopulse", "Gold"),
        ("md:repopulse", "gold_t"),
    ],
)
def test_migrate_rejects_bad_names_before_connecting(source, target, capsys):
    def connect(_):
        raise AssertionError("must not connect")

    assert golddb.main(["migrate", "--source", source, "--target", target], connect=connect) == 1
    assert "error:" in capsys.readouterr().err


def test_migrate_connects_to_the_account_not_to_one_database(tmp_path):
    md, seen = FakeMotherDuck(tmp_path), []

    def connect(target):
        seen.append(target)
        return md

    golddb.main(["migrate", "--source", "repopulse", "--target", "gold_t"], connect=connect)
    assert seen == ["md:"]


# ---------------------------------------------------------------- backup


@pytest.fixture
def local_db(tmp_path, monkeypatch):
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    path = tmp_path / "src.duckdb"
    con = duckdb.connect(str(path))
    fill_source(con, "src")
    con.close()
    return path


def test_backup_writes_staging_parquet_and_checks_it(local_db, tmp_path, capsys):
    out = tmp_path / "o'brien" / "backup"  # a quote in the path must not break the SQL
    assert golddb.main(["backup", "--database", str(local_db), "--out", str(out)]) == 0
    file = out / "stg_gharchive_events.parquet"
    con = duckdb.connect(str(local_db), read_only=True)
    try:
        want = con.execute(f"SELECT * FROM {STAGING} ORDER BY 1").fetchall()
        got = con.execute("SELECT * FROM read_parquet(?) ORDER BY 1", [file.as_posix()]).fetchall()
    finally:
        con.close()
    assert got == want
    assert json.loads(capsys.readouterr().out)["rows"] == 3


def test_backup_refuses_to_overwrite(local_db, tmp_path, capsys):
    out = tmp_path / "backup"
    out.mkdir()
    (out / "stg_gharchive_events.parquet").write_bytes(b"older backup")
    assert golddb.main(["backup", "--database", str(local_db), "--out", str(out)]) == 1
    assert "exists" in capsys.readouterr().err
    assert (out / "stg_gharchive_events.parquet").read_bytes() == b"older backup"


def test_backup_refuses_to_run_on_github_actions(local_db, tmp_path, capsys, monkeypatch):
    # Public repo: staging holds unmasked logins and hashes.
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    out = tmp_path / "backup"
    assert golddb.main(["backup", "--database", str(local_db), "--out", str(out)]) == 1
    assert "GitHub Actions" in capsys.readouterr().err
    assert not out.exists()


def test_backup_of_a_database_without_staging_is_an_error(tmp_path, capsys, monkeypatch):
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    path = tmp_path / "other.duckdb"
    duckdb.connect(str(path)).close()
    out = tmp_path / "backup"
    assert golddb.main(["backup", "--database", str(path), "--out", str(out)]) == 1
    assert "error:" in capsys.readouterr().err


def test_backup_fails_when_the_parquet_differs(local_db, tmp_path, monkeypatch):
    real = golddb.stats

    def off_by_one(con, relation):
        found = real(con, relation)
        return {**found, "rows": 2} if "read_parquet" in relation else found

    monkeypatch.setattr(golddb, "stats", off_by_one)
    con = duckdb.connect(str(local_db), read_only=True)
    try:
        with pytest.raises(GoldDbError, match="does not match"):
            golddb.backup(con, tmp_path / "backup")
    finally:
        con.close()
