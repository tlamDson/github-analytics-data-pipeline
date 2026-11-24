# MotherDuck storage: moving gold to a TRANSIENT database

The hourly `dbt build` rewrites the gold tables, and MotherDuck keeps the replaced data in failsafe
before it is freed: 7 days for a standard database, 1 day for a TRANSIENT one. TRANSIENT can only be
chosen when a database is created ([CREATE DATABASE](https://motherduck.com/docs/sql-reference/motherduck-sql-reference/create-database/),
[storage lifecycle](https://motherduck.com/docs/concepts/storage-lifecycle/)), and `repopulse` was created
without it. So the data has to move to a new database.

This document contains no measurements. The sizes seen and the estimate to check afterwards are in ticket T-0042.

**Nothing below has been run.** The migration, the switch and `DROP DATABASE` each wait for Lam's OK.

## The setting

The repository **variable** `MOTHERDUCK_DATABASE` (not a secret) names the gold database. `pipeline.yml`,
`pages.yml` and `data-quality.yml` all read it. Unset or empty means `repopulse`, so nothing changes until it is set.
Pull requests never see it: the PR smoke step keeps `repopulse_smoke`.

It takes effect once the workflow files with the setting are on `main` (after a release). Set it before that and
`pages.yml` and `data-quality.yml` on `main` ignore it.

## The commands

| Command | Where | What it does |
|---|---|---|
| `python -m repopulse.golddb backup --database md:repopulse --out <dir>` | Lam's machine | `main_staging.stg_gharchive_events` to `<dir>/stg_gharchive_events.parquet`, then compares row count and min/max `loaded_at`. Never overwrites a file. Refuses to run on GitHub Actions |
| `python -m repopulse.golddb migrate --source <db> --target <db>` | `.github/workflows/motherduck-transient.yml` | `CREATE DATABASE <target> (TRANSIENT)`, checks that MotherDuck reports it as transient, copies every table, then fails unless columns, row count and min/max `loaded_at` of every table match |

- An existing target is an error: nothing is ever replaced. The source is only read, never dropped or altered.
- `NOT NULL` constraints are kept, and `loaded_at` is copied as is, so the incremental models continue where they stopped.
- Views are listed in the output but not copied: their body names the source database. The first `dbt build` on the
  new database creates them again.
- The workflow shares the concurrency group of the scheduled pipeline, so the copy never runs while `dbt build` writes.
  It uploads nothing: the repo is public and staging holds unmasked logins and hashes.

## Steps (PowerShell, in the repo)

1. **Back staging up to Lam's machine.** It is the only place the history lives. The directory must be outside the
   repo. A separate environment keeps the DuckDB version MotherDuck supports away from the installed one. DuckDB opens
   the browser to log in to MotherDuck when `MOTHERDUCK_TOKEN` is not set.

   ```powershell
   python -m venv D:\repopulse-backup\venv
   D:\repopulse-backup\venv\Scripts\python -m pip install "duckdb==1.5.6"
   D:\repopulse-backup\venv\Scripts\python -m repopulse.golddb backup --database md:repopulse --out D:\repopulse-backup\before-transient
   ```

2. **Stop the hourly writer**, and wait until no run is in progress:

   ```powershell
   gh workflow disable pipeline.yml
   gh run list --workflow pipeline.yml --limit 3
   ```

3. **Copy** (Lam's OK):

   ```powershell
   gh workflow run motherduck-transient.yml --ref main -f source=repopulse -f target=repopulse_transient
   gh run list --workflow motherduck-transient.yml --limit 1
   ```

   Green means every table matched; the log has the counts. Red means the new database must not be used. The source
   is untouched either way. To retry, pick another `target` name, because a partly filled target is refused too.

4. **Switch** (Lam's OK), only after step 3 is green:

   ```powershell
   gh variable set MOTHERDUCK_DATABASE --body repopulse_transient
   gh workflow enable pipeline.yml
   gh workflow run pipeline.yml --ref main
   ```

   Each run loads the last 3 hours. If the writer was stopped for longer, load the missing hours too, at most 6 at a
   time: `gh workflow run pipeline.yml --ref main -f date=<YYYY-MM-DD> -f hour=<last hour> -f hours=6`. Loading an hour
   twice is harmless.

   Then check that the pipeline, Pages and Data quality runs are green.

5. **Check the storage** in MotherDuck a few days later, against the estimate in T-0042 (the view refreshes every
   1 to 6 hours):

   ```sql
   SELECT database_name, transient, active_bytes, failsafe_bytes
   FROM MD_INFORMATION_SCHEMA.STORAGE_INFO;
   ```

6. **Drop the old database** (Lam's OK), by hand in MotherDuck: `DROP DATABASE repopulse;`. Its failsafe bytes only
   drain 7 days after that. After the drop, do not run `probe.yml`: its MotherDuck check runs
   `CREATE DATABASE IF NOT EXISTS repopulse` and would leave a new, empty standard database behind.

## Rollback

**Before the drop**, `repopulse` is still there, unchanged:

```powershell
gh variable set MOTHERDUCK_DATABASE --body repopulse
gh workflow enable pipeline.yml
```

Hours loaded only into the new database are missing from `repopulse`: load them again with
`gh workflow run pipeline.yml` and `date`/`hour`/`hours` as in step 4.

**After the drop**, the history is in the new database and in the parquet backup of step 1. To rebuild from the
backup, in a new **empty** database (`<db>`), from Lam's machine:

```sql
CREATE SCHEMA <db>.main_staging;
CREATE TABLE <db>.main_staging.stg_gharchive_events AS
SELECT * FROM read_parquet('D:/repopulse-backup/before-transient/stg_gharchive_events.parquet');
```

Then set `MOTHERDUCK_DATABASE` to `<db>` and run the pipeline: `dbt build` rebuilds gold from staging. The hours after
the backup have to be loaded again as above. Keep the backup off GitHub.

## Tests

`tests/test_golddb.py` (a local DuckDB stands in for MotherDuck; only `CREATE DATABASE ... (TRANSIENT)` and the
`transient` column are simulated) and `tests/test_motherduck_transient_workflow.py`.
