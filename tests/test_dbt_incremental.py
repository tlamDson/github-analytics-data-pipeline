"""Incremental dbt models (PR 7): building batch by batch == one full build on the union.

The scheduled pipeline (``.github/workflows/pipeline.yml``) gets a fresh lake on every run, so the
silver parquet handed to dbt only holds the hours of that run. ``stg_gharchive_events`` therefore
accumulates silver across runs (insert event ids not seen yet, existing copy wins) and
``dim_user``, ``pub_dim_user`` and ``fct_events`` are incremental (touched actors only); the marts
are rebuilt from the accumulated history.

Batches (built from the raw fixtures with ``silver_select_sql`` of dbt_helpers.py, no Spark):

- ``b1``: ``tests/fixtures/gharchive_small`` (deduplicated).
- ``b2``: only ``gharchive_late/2026-01-04-0.json`` (hand-written fixture): carol (1003) as
  ``carol-old`` on ``carol/dotfiles``, a repo outside the seed, *earlier* than every carol event of
  b1. Nothing new reaches ``fct_events``, yet carol's dim_user versions shift (1003-1 is now
  ``carol-old``), so her fct rows of b1 must be recomputed.
- ``b3``: ``gharchive_scd2`` + ``gharchive_parquet_only`` + ``gharchive_late/2026-01-04-1.json``
  (bob (1002) as ``bobby`` on apache/spark, earlier than bob's b1 events: same shift, seed repo).

Every step runs ``dbt build`` on one DuckDB file, then the gold models are compared with a full
build of the union in a fresh DuckDB (row by row, column by column, load marker excluded).
"""

from __future__ import annotations

import gzip
import json
import shutil
from datetime import date, datetime
from pathlib import Path

import duckdb
import pytest
import yaml

pytest.importorskip("dbt.cli.main", reason="dbt extra not installed: pip install -e '.[dbt]'")
pytest.importorskip("dbt.adapters.duckdb", reason="dbt-duckdb not installed")

from dbt_helpers import (  # noqa: E402
    GOLD_MODELS,
    GOLD_SCHEMA,
    PARQUET_ONLY_DIR,
    PROJECT,
    PUBLIC_SCHEMA,
    ci_salt,
    dedup_lines,
    run_dbt,
    silver_select_sql,
    write_profiles,
)
from dbt_helpers import q as _q  # noqa: E402
from dbt_helpers import require_ok as _require_ok  # noqa: E402

from repopulse.spark.pii import hash_pii  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures"
SMALL = FIXTURES / "gharchive_small"
SCD2 = FIXTURES / "gharchive_scd2"
LATE = FIXTURES / "gharchive_late"
STAGING = "main_staging.stg_gharchive_events"
MODELS = (STAGING, *(f"{GOLD_SCHEMA}.{m}" for m in GOLD_MODELS))
LOAD_MARKER = "loaded_at"


def _lines(path: Path) -> list[str]:
    return [ln for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]


def _first_per_id(lines: list[str]) -> list[str]:
    seen: set[str] = set()
    out = []
    for line in lines:
        event_id = json.loads(line)["id"]
        if event_id not in seen:
            seen.add(event_id)
            out.append(line)
    return out


def write_silver_parquet(base: Path, name: str, lines: list[str]) -> str:
    """Silver parquet (docs/spark-jobs.md columns) of ``lines``; empty list -> schema only."""
    raw = base / "raw" / name
    raw.mkdir(parents=True)
    rows = lines or dedup_lines((SMALL,))  # need one row to infer the JSON reader, then LIMIT 0
    with gzip.open(raw / "events.json.gz", "wt", encoding="utf-8") as fh:
        fh.write("\n".join(rows) + "\n")
    out = base / "silver" / name
    out.mkdir(parents=True)
    sql = silver_select_sql((raw / "*.json.gz").as_posix())
    if not lines:
        sql = f"SELECT * FROM ({sql}) LIMIT 0"
    con = duckdb.connect(":memory:")
    try:
        con.execute(f"COPY ({sql}) TO {_q((out / 'part-0.parquet').as_posix())} (FORMAT parquet)")
    finally:
        con.close()
    return (out / "*.parquet").as_posix()


def _sort_key(row: tuple) -> tuple:
    return tuple((v is None, round(v, 6) if isinstance(v, float) else str(v)) for v in row)


def snapshot(db: Path, *, keep_load_marker: bool = True) -> dict[str, tuple[list, list]]:
    """model -> (columns, rows sorted); optionally without the load marker column."""
    con = duckdb.connect(str(db), read_only=True)
    try:
        out = {}
        for model in MODELS:
            cols = [c[0] for c in con.execute(f"SELECT * FROM {model} LIMIT 0").description]
            if not keep_load_marker:
                cols = [c for c in cols if c != LOAD_MARKER]
            select = ", ".join(f'"{c}"' for c in cols)
            rows = con.execute(f"SELECT {select} FROM {model}").fetchall()
            out[model] = (cols, sorted(rows, key=_sort_key))
        return out
    finally:
        con.close()


def assert_same_rows(model: str, actual: tuple[list, list], expected: tuple[list, list]) -> None:
    a_cols, a_rows = actual
    e_cols, e_rows = expected
    assert a_cols == e_cols, model
    assert len(a_rows) == len(e_rows), (model, len(a_rows), len(e_rows))
    for a_row, e_row in zip(a_rows, e_rows, strict=True):
        for col, a, e in zip(a_cols, a_row, e_row, strict=True):
            if isinstance(e, float):
                assert a == pytest.approx(e, abs=1e-9), (model, col, a_row, e_row)
            else:
                assert a == e, (model, col, a_row, e_row)


# ---------------------------------------------------------------- session fixtures


@pytest.fixture(scope="session")
def batches(tmp_path_factory) -> dict[str, str]:
    base = tmp_path_factory.mktemp("dbt_incremental_batches")
    b1 = dedup_lines((SMALL,))
    b2 = _lines(LATE / "2026-01-04-0.json")
    b3 = dedup_lines((SCD2, PARQUET_ONLY_DIR)) + _lines(LATE / "2026-01-04-1.json")
    # Overlap: ids already loaded. 9000000040 comes back with another login: the copy already in
    # staging must win (silver's rule), so nothing may change.
    imposter = json.loads(_lines(SCD2 / "2026-02-12-0.json")[0])
    imposter["actor"]["login"] = "dave-imposter"
    overlap = b2 + [json.dumps(imposter, separators=(",", ":"))] + b1[:3]
    return {
        "b1": write_silver_parquet(base, "b1", b1),
        "b2": write_silver_parquet(base, "b2", b2),
        "b3": write_silver_parquet(base, "b3", b3),
        "overlap": write_silver_parquet(base, "overlap", overlap),
        "empty": write_silver_parquet(base, "empty", []),
        "union_b1_b2": write_silver_parquet(base, "union_b1_b2", _first_per_id(b1 + b2)),
        "union_all": write_silver_parquet(base, "union_all", _first_per_id(b1 + b2 + b3)),
    }


INCREMENTAL_STEPS = (  # (step, batch, extra dbt args), run in this order on one DuckDB file
    ("b1", "b1", ()),
    ("b2", "b2", ()),
    ("b3", "b3", ()),
    ("rerun_b3", "b3", ()),
    ("overlap", "overlap", ()),
    ("empty", "empty", ()),
    ("full_refresh_b3", "b3", ("--full-refresh",)),
)


@pytest.fixture(scope="session")
def incremental(batches, tmp_path_factory) -> dict:
    base = tmp_path_factory.mktemp("dbt_incremental_db")
    db = base / "incremental.duckdb"
    profiles = write_profiles(base / "profiles", db)
    procs, snaps, public = {}, {}, {}
    for step, batch, extra in INCREMENTAL_STEPS:
        proc = run_dbt(
            ["build", *extra], PROJECT, profiles, base, {"silver_gharchive_parquet": batches[batch]}
        )
        procs[step] = proc
        snaps[step] = snapshot(db) if proc.returncode == 0 else None
        public[step] = _dim_user_tables(db)[PUB_DIM_USER] if proc.returncode == 0 else None
    return {"procs": procs, "snaps": snaps, "public": public}


@pytest.fixture(scope="session")
def reference(batches, tmp_path_factory) -> dict:
    """Full builds of the unions, each in a fresh DuckDB file."""
    out = {}
    for name in ("union_b1_b2", "union_all"):
        base = tmp_path_factory.mktemp(f"dbt_reference_{name}")
        db = base / "reference.duckdb"
        proc = run_dbt(
            ["build"],
            PROJECT,
            write_profiles(base / "profiles", db),
            base,
            {"silver_gharchive_parquet": batches[name]},
        )
        _require_ok(proc)
        out[name] = snapshot(db, keep_load_marker=False)
    return out


def _snap(incremental: dict, step: str, *, keep_load_marker: bool = True) -> dict:
    _require_ok(incremental["procs"][step])
    snap = incremental["snaps"][step]
    if keep_load_marker:
        return snap
    out = {}
    for model, (cols, rows) in snap.items():
        keep = [i for i, c in enumerate(cols) if c != LOAD_MARKER]
        rows = sorted((tuple(r[i] for i in keep) for r in rows), key=_sort_key)
        out[model] = ([cols[i] for i in keep], rows)
    return out


def _column(snap: dict, model: str, col: str) -> list:
    cols, rows = snap[model]
    i = cols.index(col)
    return [r[i] for r in rows]


def _fct_user_keys(snap: dict, actor_id: int) -> dict[str, str]:
    cols, rows = snap[f"{GOLD_SCHEMA}.fct_events"]
    e, a, u = cols.index("event_id"), cols.index("actor_id"), cols.index("user_key")
    return {r[e]: r[u] for r in rows if r[a] == actor_id}


# ---------------------------------------------------------------- invariant 1: inc == full


@pytest.mark.parametrize("step", [s for s, _, _ in INCREMENTAL_STEPS])
def test_every_incremental_step_builds(incremental, step):
    _require_ok(incremental["procs"][step])


@pytest.mark.parametrize("step, ref", [("b2", "union_b1_b2"), ("b3", "union_all")])
@pytest.mark.parametrize("model", MODELS)
def test_incremental_equals_full_refresh_of_union(incremental, reference, step, ref, model):
    actual = _snap(incremental, step, keep_load_marker=False)[model]
    assert_same_rows(model, actual, reference[ref][model])


@pytest.mark.parametrize("step, ref", [("b2", "union_b1_b2"), ("b3", "union_all")])
def test_incremental_row_counts_equal_full_refresh(incremental, reference, step, ref):
    snap = _snap(incremental, step, keep_load_marker=False)
    counts = {m: len(rows) for m, (_, rows) in snap.items()}
    expected = {m: len(rows) for m, (_, rows) in reference[ref].items()}
    assert counts == expected
    assert counts[STAGING] == {"b2": 34, "b3": 42}[step]  # 33 + 1, then + 3 + 4 + 1


def test_staging_marks_each_load(incremental):
    snap = _snap(incremental, "b3")
    markers = _column(snap, STAGING, LOAD_MARKER)
    assert None not in markers
    assert len(set(markers)) == 3  # b1, b2 and b3 were each inserted by their own dbt run
    ids = _column(snap, STAGING, "event_id")
    by_id = dict(zip(ids, markers, strict=True))
    assert by_id["9000000001"] < by_id["9000000060"] < by_id["9000000061"]


# ---------------------------------------------------------------- invariant 2: idempotent


@pytest.mark.parametrize("step", ["rerun_b3", "overlap", "empty"])
def test_batch_without_new_events_changes_nothing(incremental, step):
    before = _snap(incremental, "b3")
    after = _snap(incremental, step)
    for model in MODELS:
        assert_same_rows(model, after[model], before[model])


@pytest.mark.parametrize("step", [s for s, _, _ in INCREMENTAL_STEPS])
def test_no_duplicate_event_ids(incremental, step):
    snap = _snap(incremental, step)
    for model in (STAGING, f"{GOLD_SCHEMA}.fct_events"):
        ids = _column(snap, model, "event_id")
        assert len(ids) == len(set(ids)), (step, model)


def test_existing_copy_of_an_event_wins(incremental):
    snap = _snap(incremental, "overlap")
    assert "dave-imposter" not in _column(snap, f"{GOLD_SCHEMA}.dim_user", "actor_login")
    cols, rows = snap[STAGING]
    e, login = cols.index("event_id"), cols.index("actor_login")
    assert {r[login] for r in rows if r[e] == "9000000040"} == {"dave2"}


# ---------------------------------------------------------------- invariant 3: late events


def test_out_of_seed_late_event_shifts_versions_of_existing_facts(incremental):
    # b2 holds only carol-old on carol/dotfiles: no new fact, but carol's b1 facts now point at
    # version 2 (carol), since version 1 is carol-old.
    keys = _fct_user_keys(_snap(incremental, "b2"), 1003)
    assert keys and set(keys.values()) == {"1003-2"}, keys


def test_seed_repo_late_event_shifts_versions_of_existing_facts(incremental):
    keys = _fct_user_keys(_snap(incremental, "b3"), 1002)
    assert keys.pop("9000000061") == "1002-1"  # bobby, the late event itself
    assert keys and set(keys.values()) == {"1002-2"}, keys
    assert set(_fct_user_keys(_snap(incremental, "b3"), 1003).values()) == {"1003-2"}


# ---------------------------------------------------------------- invariant 4: --full-refresh


def test_full_refresh_keeps_accumulated_staging(incremental, reference):
    snap = _snap(incremental, "full_refresh_b3", keep_load_marker=False)
    assert len(snap[STAGING][1]) == len(reference["union_all"][STAGING][1]) == 42
    for model in MODELS:
        assert_same_rows(model, snap[model], reference["union_all"][model])


# ---------------------------------------------------------------- invariant 5: UTC


def _profiles_yaml() -> dict:
    return yaml.safe_load((PROJECT / "profiles.yml").read_text(encoding="utf-8"))


@pytest.mark.parametrize("target", ["ci", "motherduck"])
def test_duckdb_targets_pin_session_time_zone_to_utc(target):
    output = _profiles_yaml()["repopulse"]["outputs"][target]
    assert output["type"] == "duckdb"
    assert output.get("settings", {}).get("TimeZone") == "UTC"


def test_motherduck_database_comes_from_env_with_default():
    path = _profiles_yaml()["repopulse"]["outputs"]["motherduck"]["path"]
    assert "env_var('MOTHERDUCK_DATABASE', 'repopulse')" in path
    assert path.startswith("md:")


def test_timestamptz_near_midnight_keeps_utc_date_and_week(tmp_path):
    """Spark's UTC timestamp may reach DuckDB as TIMESTAMPTZ: event time must not follow the
    session time zone. The session here is deliberately America/New_York."""
    line = json.dumps(
        {
            "id": "9000000070",
            "type": "WatchEvent",
            "actor": {"id": 1100, "login": "tzuser"},
            "repo": {"id": 2001, "name": "apache/spark"},
            "payload": {"action": "started"},
            "created_at": "2026-01-05T02:00:00Z",  # Monday 02:00 UTC = Sunday 21:00 in New York
        }
    )
    raw = tmp_path / "raw"
    raw.mkdir()
    with gzip.open(raw / "events.json.gz", "wt", encoding="utf-8") as fh:
        fh.write(line + "\n")
    silver = tmp_path / "silver"
    silver.mkdir()
    con = duckdb.connect(":memory:")
    try:
        con.execute("SET TimeZone = 'UTC'")
        sql = silver_select_sql((raw / "*.json.gz").as_posix())
        con.execute(
            f"COPY (SELECT * REPLACE (CAST(created_at AS TIMESTAMPTZ) AS created_at) FROM ({sql})) "
            f"TO {_q((silver / 'part-0.parquet').as_posix())} (FORMAT parquet)"
        )
        kind = con.execute(
            f"SELECT typeof(created_at) FROM read_parquet({_q((silver / '*.parquet').as_posix())})"
        ).fetchone()[0]
    finally:
        con.close()
    assert kind == "TIMESTAMP WITH TIME ZONE"

    db = tmp_path / "tz.duckdb"
    profiles = tmp_path / "profiles"
    profiles.mkdir()
    (profiles / "profiles.yml").write_text(
        "repopulse:\n"
        "  target: ci\n"
        "  outputs:\n"
        "    ci:\n"
        "      type: duckdb\n"
        f'      path: "{db.as_posix()}"\n'
        "      threads: 2\n"
        "      settings:\n"
        "        TimeZone: America/New_York\n",
        encoding="utf-8",
    )
    proc = run_dbt(
        # cautious: skip tests that also touch marts (not built here)
        ["build", "--select", "+fct_events", "--indirect-selection", "cautious"],
        PROJECT,
        profiles,
        tmp_path,
        {"silver_gharchive_parquet": (silver / "*.parquet").as_posix()},
    )
    _require_ok(proc)
    con = duckdb.connect(str(db), read_only=True)
    try:
        row = con.execute(
            f"SELECT created_at, event_date, event_week FROM {GOLD_SCHEMA}.fct_events "
            "WHERE event_id = '9000000070'"
        ).fetchone()
        staged = con.execute(
            f"SELECT created_at FROM {STAGING} WHERE event_id = '9000000070'"
        ).fetchone()
    finally:
        con.close()
    assert staged == (datetime(2026, 1, 5, 2),)
    assert row == (datetime(2026, 1, 5, 2), date(2026, 1, 5), date(2026, 1, 5))


# ---------------------------------------------------------------- invariant 6: dim_user writes
#
# A run writes dim_user (gold and its masked public copy) in proportion to its new rows (ticket
# T-0043). How a written row is seen: before a run is_bot of every gold row and is_current of every
# public row are flipped, so a row the run leaves alone stays flipped and a row the run writes has
# the right value again. The two columns differ on purpose: gold is_current stays right, so a public
# row copied again from an untouched gold row shows up as written.

DIM_USER = f"{GOLD_SCHEMA}.dim_user"
PUB_DIM_USER = f"{PUBLIC_SCHEMA}.dim_user"
BOT_RULE = (
    "coalesce(right(lower(actor_login), 5) = '[bot]' OR right(lower(actor_login), 4) = '-bot', "
    "false)"
)


def _run_dim_user(base: Path, db: Path, batch: str, *args: str, select: str = "+pub_dim_user"):
    proc = run_dbt(
        ["run", "--select", *select.split(), *args],
        PROJECT,
        write_profiles(base / "profiles", db),
        base,
        {"silver_gharchive_parquet": batch},
    )
    _require_ok(proc)


def _execute(db: Path, *statements: str) -> None:
    con = duckdb.connect(str(db))
    try:
        for sql in statements:
            con.execute(sql)
    finally:
        con.close()


def _rows(db: Path, sql: str) -> list[tuple]:
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def _flip_marks(db: Path) -> None:
    _execute(
        db,
        f"UPDATE {DIM_USER} SET is_bot = NOT is_bot",
        f"UPDATE {PUB_DIM_USER} SET is_current = NOT is_current",
    )


def _written(db: Path) -> dict[str, set[str]]:
    """table -> actors with a row written since ``_flip_marks`` (masked ids for public)."""
    salt = ci_salt()
    is_current = {
        hash_pii(key, salt): current
        for key, current in _rows(db, f"SELECT user_key, is_current FROM {DIM_USER}")
    }
    gold = _rows(db, f"SELECT DISTINCT actor_id FROM {DIM_USER} WHERE is_bot = {BOT_RULE}")
    public = _rows(db, f"SELECT user_key, actor_id, is_current FROM {PUB_DIM_USER}")
    return {
        DIM_USER: {str(actor) for (actor,) in gold},
        PUB_DIM_USER: {actor for key, actor, current in public if is_current[key] == current},
    }


def _expected_written(actor_ids) -> dict[str, set[str]]:
    salt = ci_salt()
    ids = {str(a) for a in actor_ids}
    return {DIM_USER: ids, PUB_DIM_USER: {hash_pii(a, salt) for a in ids}}


def _versions(db: Path, actor_id: int) -> list[tuple]:
    return _rows(
        db,
        f"SELECT user_key, actor_login, is_current FROM {DIM_USER} "
        f"WHERE actor_id = {actor_id} ORDER BY version_number",
    )


def _dim_user_tables(db: Path) -> dict[str, tuple[list, list]]:
    """table -> (columns, sorted rows), load marker included."""
    con = duckdb.connect(str(db), read_only=True)
    try:
        out = {}
        for table in (DIM_USER, PUB_DIM_USER):
            cur = con.execute(f"SELECT * FROM {table}")
            out[table] = ([c[0] for c in cur.description], sorted(cur.fetchall(), key=_sort_key))
        return out
    finally:
        con.close()


def _assert_equals_full_rebuild(base: Path, db: Path, batch: str) -> None:
    """Both tables == the same db after --full-refresh (staging keeps its history and markers)."""
    copy = base / "full_rebuild.duckdb"
    shutil.copy(db, copy)
    _run_dim_user(base, copy, batch, "--full-refresh")
    actual, expected = _dim_user_tables(db), _dim_user_tables(copy)
    for table in (DIM_USER, PUB_DIM_USER):
        assert LOAD_MARKER in expected[table][0], table
        assert_same_rows(table, actual[table], expected[table])


@pytest.fixture
def dim_user_db(batches, tmp_path) -> Path:
    db = tmp_path / "dim_user.duckdb"
    _run_dim_user(tmp_path, db, batches["b1"])
    return db


def test_late_event_writes_only_its_actor(dim_user_db, batches, tmp_path):
    _flip_marks(dim_user_db)
    _run_dim_user(tmp_path, dim_user_db, batches["b2"])  # carol-old, before every carol event
    assert _versions(dim_user_db, 1003) == [
        ("1003-1", "carol-old", False),
        ("1003-2", "carol", True),
    ]
    assert _written(dim_user_db) == _expected_written({1003})


def test_login_change_writes_only_the_actors_of_the_batch(dim_user_db, batches, tmp_path):
    _flip_marks(dim_user_db)
    _run_dim_user(tmp_path, dim_user_db, batches["b3"])  # bob was bobby, dave is now dave2
    assert _versions(dim_user_db, 1002)[0] == ("1002-1", "bobby", False)
    assert [v[1:] for v in _versions(dim_user_db, 1004)] == [("dave", False), ("dave2", True)]
    newest = f"loaded_at = (SELECT max(loaded_at) FROM {STAGING})"
    touched = {
        a for (a,) in _rows(dim_user_db, f"SELECT DISTINCT actor_id FROM {STAGING} WHERE {newest}")
    }
    everyone = {a for (a,) in _rows(dim_user_db, f"SELECT DISTINCT actor_id FROM {STAGING}")}
    assert {1002, 1004} <= touched < everyone
    assert _written(dim_user_db) == _expected_written(touched)


@pytest.mark.parametrize("batch", ["b1", "empty"])
def test_rerun_without_new_events_writes_no_dim_user_row(dim_user_db, batches, tmp_path, batch):
    _flip_marks(dim_user_db)
    _run_dim_user(tmp_path, dim_user_db, batches[batch])
    assert _written(dim_user_db) == _expected_written(set())


def test_late_event_and_login_change_equal_a_full_rebuild(dim_user_db, batches, tmp_path):
    for batch in ("b2", "b3", "b3", "overlap"):
        _run_dim_user(tmp_path, dim_user_db, batches[batch])
        _assert_equals_full_rebuild(tmp_path, dim_user_db, batches[batch])


def test_steps_skipped_by_a_failed_run_are_caught_up(dim_user_db, batches, tmp_path):
    """Run 2 stops after staging, run 3 after gold dim_user: run 4 still writes their actors."""
    _run_dim_user(tmp_path, dim_user_db, batches["b2"], select="stg_gharchive_events")
    _run_dim_user(tmp_path, dim_user_db, batches["b3"], select="stg_gharchive_events dim_user")
    _run_dim_user(tmp_path, dim_user_db, batches["empty"])
    _assert_equals_full_rebuild(tmp_path, dim_user_db, batches["empty"])


def test_tables_built_before_the_load_marker_are_upgraded_by_a_normal_run(
    dim_user_db, batches, tmp_path
):
    """The deployed tables have no loaded_at and no scheduled run passes --full-refresh."""
    _execute(
        dim_user_db,
        *(f"ALTER TABLE {t} DROP COLUMN IF EXISTS loaded_at" for t in (DIM_USER, PUB_DIM_USER)),
    )
    _run_dim_user(tmp_path, dim_user_db, batches["b2"])
    _assert_equals_full_rebuild(tmp_path, dim_user_db, batches["b2"])


@pytest.mark.parametrize("step, ref", [("b2", "union_b1_b2"), ("b3", "union_all")])
def test_public_dim_user_masks_the_gold_rows_of_a_full_build(incremental, reference, step, ref):
    salt = ci_salt()
    masked = {"user_key", "actor_id", "actor_login"}
    gold_cols, gold_rows = reference[ref][DIM_USER]
    expected = [
        tuple(
            hash_pii(str(v), salt) if c in masked and v is not None else v
            for c, v in zip(gold_cols, row, strict=True)
        )
        for row in gold_rows
    ]
    cols, rows = incremental["public"][step]
    keep = [i for i, c in enumerate(cols) if c != LOAD_MARKER]
    assert [cols[i] for i in keep] == gold_cols
    actual = [tuple(r[i] for i in keep) for r in rows]
    assert sorted(actual, key=_sort_key) == sorted(expected, key=_sort_key)
