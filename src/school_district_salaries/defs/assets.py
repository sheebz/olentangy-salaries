"""Medallion pipeline for Olentangy Local School District salary data.

bronze -> raw parquet, everything VARCHAR, no interpretation
silver -> typed + exploded into a queryable DuckDB file
gold   -> modeled table + Kaggle-ready publishing bundle
"""

from pathlib import Path

import dagster as dg
import duckdb

ROOT = Path(__file__).resolve().parents[3]  # defs/ -> package -> src/ -> project root
DATA = ROOT / "data"
SOURCE_CSV = DATA / "source" / "olentangy_salaries.csv"
BRONZE_PARQUET = DATA / "bronze" / "salaries_raw.parquet"
DUCKDB_PATH = DATA / "silver" / "salaries.duckdb"
GOLD_DIR = DATA / "gold"
# kept separate from gold: whatever lands here is exactly what gets uploaded, so an
# undeclared file can't sneak into the Kaggle upload and cost usability points.
KAGGLE_DIR = DATA / "kaggle"

# ponytail: plain duckdb.connect, no DuckDBResource. Swap in the resource when the
# db path needs to differ per environment; today it never does.
#
# ponytail: no feature engineering yet — no job families, pay bands, ranks or percentiles.
# Every layer here only cleans, types and reshapes what the district actually published.


def _preview(con, sql: str) -> dg.MetadataValue:
    """DuckDB already renders an ASCII table; a code fence is the whole formatter."""
    return dg.MetadataValue.md(f"```\n{con.sql(sql)}\n```")


@dg.asset(group_name="bronze", kinds={"duckdb", "parquet"})
def salaries_raw() -> dg.MaterializeResult:
    """Source CSV landed as Parquet with zero cleaning: every column stays VARCHAR."""
    BRONZE_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect() as con:
        con.execute(
            f"COPY (SELECT * FROM read_csv('{SOURCE_CSV}', all_varchar=true, header=true)) "
            f"TO '{BRONZE_PARQUET}' (FORMAT parquet)"
        )
        n = con.sql(f"SELECT count(*) FROM '{BRONZE_PARQUET}'").fetchone()[0]

    return dg.MaterializeResult(
        metadata={
            "rows": n,
            "path": str(BRONZE_PARQUET),
            "bytes": BRONZE_PARQUET.stat().st_size,
            "source": str(SOURCE_CSV),
        }
    )


@dg.asset(group_name="silver", kinds={"duckdb"}, deps=[salaries_raw])
def salaries_typed() -> dg.MaterializeResult:
    """Typed `salaries` table plus one-row-per-role `salary_roles`, in a DuckDB file.

    Gross pay becomes DECIMAL; the semicolon-delimited Position field is unnested so
    the ~1.6k employees holding multiple roles can be queried at role grain.
    """
    # ponytail: no names anywhere in this pipeline. They were only ever an ordering
    # tiebreaker, and they are stripped from the source before it reaches the repo.
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(DUCKDB_PATH) as con:
        # Ordering ties are broken by `positions` alone. 400 rows share a
        # (gross_pay, positions) key, but those rows are identical in every published
        # column, so which id each one gets cannot change the exported bytes.
        con.execute(f"""
            CREATE OR REPLACE TABLE salaries AS
            SELECT
              row_number() OVER (ORDER BY gross_pay DESC, positions) AS employee_id,
              positions, gross_pay
            FROM (
              SELECT
                trim("Position") AS positions,
                CAST(replace(replace(trim("Gross"), '$', ''), ',', '') AS DECIMAL(12, 2)) AS gross_pay
              FROM '{BRONZE_PARQUET}'
            )
        """)
        # The district encodes two things in the role string itself: a trailing 3-digit
        # contract-day count (SECRETARY 260) and a "-0" variant (TEACHER-0). Both are
        # split out mechanically; neither is given a meaning it hasn't earned.
        con.execute("""
            CREATE OR REPLACE TABLE salary_roles AS
            SELECT
              employee_id, role_seq, role,
              trim(regexp_replace(regexp_replace(role, ' [0-9]{3}$', ''), '-0$', '')) AS role_base,
              TRY_CAST(regexp_extract(role, ' ([0-9]{3})$', 1) AS INTEGER) AS contract_days,
              CASE WHEN role LIKE '%-0' THEN '-0' END AS role_suffix
            FROM (
              SELECT s.employee_id, t.ordinal AS role_seq, trim(t.r) AS role
              FROM salaries s,
                   unnest(str_split(s.positions, ';')) WITH ORDINALITY AS t(r, ordinal)
            )
        """)
        employees, roles = con.sql(
            "SELECT (SELECT count(*) FROM salaries), (SELECT count(*) FROM salary_roles)"
        ).fetchone()
        preview = _preview(
            con,
            "SELECT role, count(*) AS n FROM salary_roles GROUP BY 1 ORDER BY 2 DESC LIMIT 15",
        )

    return dg.MaterializeResult(
        metadata={
            "employees": employees,
            "role_rows": roles,
            "duckdb_path": str(DUCKDB_PATH),
            "query_hint": f"duckdb {DUCKDB_PATH} -c 'SELECT * FROM salaries LIMIT 5'",
            "top_roles": preview,
        }
    )


@dg.asset(group_name="gold", kinds={"duckdb", "parquet"}, deps=[salaries_typed])
def salaries_modeled() -> dg.MaterializeResult:
    """Publish-shaped `salaries_gold`: stably ordered, with contract_days rolled up.

    Employee names are stripped before the data enters this repo, so no layer here
    has ever held them — see the source-CSV note in the README.
    """
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(DUCKDB_PATH) as con:
        # contract_days rolls up safely: no employee holds two different day counts
        # (asserted in kaggle._check), so max() picks the single non-null value.
        con.execute("""
            CREATE OR REPLACE TABLE salaries_gold AS
            SELECT s.employee_id, s.positions, d.contract_days, s.gross_pay
            FROM salaries s
            LEFT JOIN (
              SELECT employee_id, max(contract_days) AS contract_days
              FROM salary_roles GROUP BY 1
            ) d USING (employee_id)
            ORDER BY s.employee_id
        """)
        con.execute(
            f"COPY salaries_gold TO '{GOLD_DIR / 'olentangy_salaries.parquet'}' (FORMAT parquet)"
        )
        rows, total, median = con.sql(
            "SELECT count(*), sum(gross_pay), median(gross_pay) FROM salaries_gold"
        ).fetchone()
        preview = _preview(con, "SELECT * FROM salaries_gold LIMIT 10")

    return dg.MaterializeResult(
        metadata={
            "rows": rows,
            "total_gross_pay": float(total),
            "median_gross_pay": float(median),
            "dagster/row_count": rows,
            "preview": preview,
        }
    )
