"""Kaggle publishing bundle: upload files, dataset-metadata.json and the dataset card.

Kaggle's usability score rewards a subtitle, tags, a license, per-file descriptions and
per-column descriptions. All of those live in dataset-metadata.json, so it is generated
from the gold table rather than hand-maintained.
"""

import json

import dagster as dg
import duckdb

from .assets import DUCKDB_PATH, GOLD_DIR, KAGGLE_DIR, SOURCE_CSV, salaries_modeled

KAGGLE_ID = "robschieber/olentangy-school-district-salaries-2025"
# `kaggle datasets metadata --update` re-applies this value, so it is the source of
# truth for visibility — flipping it in the web UI alone would be undone by the next
# publish. Public since 2026-09-19.
PRIVATE = False
PERIOD = "calendar year 2025"
SOURCE_NAME = "The Columbus Dispatch public payroll database"
PUBLISHED = "2026-06-29"
SOURCE_URL = (
    "https://www.dispatch.com/story/news/local/2026/06/29/"
    "public-payroll-how-central-ohio-government-agencies-spend-your-money/90691984007/"
)

COLUMN_DOCS = {
    "employee_id": "Surrogate key assigned by this pipeline, ordered by gross pay descending. Not a district identifier and not stable across dataset versions.",
    "positions": "Semicolon-delimited position string exactly as published. An employee may hold several.",
    "contract_days": "Contracted days per year, parsed from the trailing number in the role string (e.g. SECRETARY 260). Null for the ~96% of employees whose roles carry no day count.",
    "gross_pay": "Total gross pay in USD for calendar year 2025, parsed from the published dollar string. Includes overtime, stipends and 'other' pay such as holiday, vacation and accrued time.",
}

ROLE_COLUMN_DOCS = {
    "employee_id": "Foreign key to olentangy_salaries.csv.",
    "role_seq": "Position of this role within the employee's raw position string, starting at 1.",
    "role": "A single role held by the employee, split out of the raw position string, verbatim.",
    "role_base": "The role with its trailing contract-day count and '-0' suffix removed, so SECRETARY 183/226/260 group together.",
    "contract_days": "Contracted days per year parsed from the role string; null when the role carries no day count.",
    "role_suffix": "'-0' when the published role carried that suffix, otherwise null. The district publishes no key for it; see the dataset description.",
}

DESCRIPTION = """\
# Olentangy Local School District Employee Salaries (2025)

Gross pay for every employee of Olentangy Local School District (Delaware County, Ohio) \
— one of the largest and fastest-growing public school districts in Ohio — for \
**calendar year 2025**. Ohio public employee compensation is public record; this dataset \
takes the district's slice of {source_name} and cleans it into something you can actually \
analyze.

## What makes this dataset interesting

The raw listing packs multiple jobs into one free-text field: {multi_pct:.0f}% of employees \
hold more than one position — teachers who also coach, bus drivers who also work supplemental \
routes. Rather than make you parse that string, this dataset ships both grains:

- **`olentangy_salaries.csv`** — one row per employee ({employees:,} rows).
- **`olentangy_salary_roles.csv`** — one row per employee-role ({role_rows:,} rows across \
{distinct_roles} distinct roles), joinable on `employee_id`.

The role string also hides a **contract length**. Roles ending in a three-digit number are \
contracted days per year, and the pay tracks it closely — secretaries run \
$224/day at 183 days, $235 at 226, $270 at 260. That number is pulled out into \
`contract_days`, and `role_base` strips it so `SECRETARY 183/226/260` group as one role.

Past that, the data is left as published — no job families, pay bands or rankings imposed \
on it. Those are one-liners you can compute to your own taste, and the grouping of the \
{distinct_roles} roles is a genuinely open question.

## At a glance

| | |
|---|---|
| Employees | {employees:,} |
| Total gross payroll | ${total:,.0f} |
| Median gross pay | ${median:,.0f} |
| Highest gross pay | ${max_pay:,.0f} |
| Employees holding 2+ positions | {multi:,} |

### Most common roles

Grouped by `role_base`, so contract-day variants collapse together and `-0` lines are excluded.

{role_table}

## Ideas to get started

- Group the {distinct_roles} roles into job families of your own and see how payroll splits \
between instruction, administration, transportation and food service.
- What does the pay distribution look like within a single role — and how heavy is the tail?
- Does holding a supplemental role (coaching, tutoring) measurably change total compensation?
- Compare against neighboring Ohio districts, which publish payroll in the same format.

## Caveats, honestly

- **Gross pay is not salary.** Per the source's own database note, pay classified as "other" \
*"can include holiday or vacation time, accrued time as well as miscellaneous pay or \
stipends, among other payments a public employee received in 2025."* Those are rolled into \
`gross_pay` here, so a part-year substitute and a full-time teacher are not directly \
comparable — and `gross_pay` will exceed contracted salary for many employees.
- **The upstream compilation used AI tooling.** The source's editor's note states: *"The \
Dispatch used AI tools to help organize and clean data for this salary database."* Treat \
individual records as journalism-grade rather than authoritative; for anything consequential, \
verify against the district's own records.
- **No FTE, tenure, degree or hours** are published, so per-hour or per-FTE normalization is \
not possible from this file alone.
- **Some role codes are undocumented.** Labels such as CMF, CLAS-NON, LT-PLCMT, UNIV-PLA and \
ESC-CERT/CLAS are district payroll codes with no published key; they are passed through \
verbatim rather than guessed at. One record carries the role `99`, which looks like a \
placeholder.
- **Watch the `-0` role suffix.** {dash0_employees} employees carry exactly one role line \
ending in `-0` (`TEACHER-0`, `CUSTODIAN-0`). Gross pay on these is far \
lower — employees holding a `TEACHER-0` line have a median total of \
${dash0_median:,.0f} against ${teacher_median:,.0f} for `TEACHER`. The district publishes no \
key for the suffix, and most of these employees ({dash0_multi}) hold other roles too, so it \
reads as a secondary or partial-period pay line rather than a marker on the person. Decide \
deliberately whether to include them before computing any "average teacher salary" — they \
will drag it down.
- **`contract_days` is sparse.** Only {with_days:,} of {employees:,} employees have a day \
count published in their role string; it is null for everyone else, not zero.
- **Employee names have been removed**, though the district publishes them as public record. \
`employee_id` is a surrogate key created here, not a district identifier. Note that this is \
*not* full anonymization: an employee holding a unique role — `ADMIN; SUPERINTENDENT`, say — \
is still identifiable from role and pay alone. Please use this dataset to analyze \
institutions, not to target individuals.
- **No date column exists in the file itself.** The {period} framing comes from the source \
database note, not from a field you can check per row.

## Provenance and collection methodology

**How the data was collected.** The Columbus Dispatch obtained these payrolls through a \
**public records request**, as part of an ongoing series analyzing salaries across public, \
taxpayer-funded agencies in the greater Columbus area — school districts, local governments, \
colleges and more. Their database, published 2026-06-29, lets you search any of those \
institutions; this dataset is the Olentangy Local School District slice of it.

Source article: {source_url}

Credit: **{source_name}**. The underlying payroll figures are Ohio public record, but the \
records request and compilation are their work.

**What happened after that.** The data runs through a reproducible Dagster bronze/silver/gold \
pipeline — raw Parquet, a typed DuckDB layer, and this export. The only changes made here are \
parsing dollar strings to numbers, splitting the semicolon-delimited position field into a \
role table, pulling `contract_days` out of the role string, and dropping employee names. No \
values were imputed, corrected or dropped. Column definitions ship in the file metadata.

**Update frequency.** The Dispatch series is ongoing, so expect roughly annual refreshes as \
new payroll years are released.
"""


@dg.asset(group_name="gold", kinds={"kaggle", "csv"}, deps=[salaries_modeled])
def kaggle_bundle() -> dg.MaterializeResult:
    """Upload-ready Kaggle bundle: two CSVs, dataset-metadata.json and a dataset card."""
    KAGGLE_DIR.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(DUCKDB_PATH) as con:
        for table, name in [
            ("salaries_gold", "olentangy_salaries"),
            ("salary_roles", "olentangy_salary_roles"),
        ]:
            con.execute(f"COPY {table} TO '{KAGGLE_DIR / f'{name}.csv'}' (HEADER, DELIMITER ',')")

        employees, total, median, max_pay = con.sql(
            "SELECT count(*), sum(gross_pay), median(gross_pay), max(gross_pay) FROM salaries_gold"
        ).fetchone()
        role_rows, distinct_roles = con.sql(
            "SELECT count(*), count(DISTINCT role) FROM salary_roles"
        ).fetchone()
        multi = con.sql(
            "SELECT count(*) FROM (SELECT employee_id FROM salary_roles GROUP BY 1 HAVING count(*) > 1)"
        ).fetchone()[0]
        top_roles = con.sql("""
            SELECT r.role_base, count(*) AS employees, round(median(g.gross_pay)) AS median_pay
            FROM salary_roles r JOIN salaries_gold g USING (employee_id)
            WHERE r.role_suffix IS NULL
            GROUP BY 1 ORDER BY employees DESC LIMIT 15
        """).fetchall()
        with_days = con.sql(
            "SELECT count(*) FROM salaries_gold WHERE contract_days IS NOT NULL"
        ).fetchone()[0]
        dash0_roles, dash0_employees = con.sql(
            "SELECT count(*), count(DISTINCT employee_id) FROM salary_roles WHERE role_suffix = '-0'"
        ).fetchone()
        dash0_multi = con.sql("""
            SELECT count(*) FROM (
              SELECT employee_id FROM salary_roles GROUP BY 1
              HAVING count(*) FILTER (role_suffix = '-0') > 0 AND count(*) > 1)
        """).fetchone()[0]
        dash0_median, teacher_median = con.sql("""
            SELECT
              (SELECT median(g.gross_pay) FROM salary_roles r JOIN salaries_gold g
                 USING (employee_id) WHERE r.role = 'TEACHER-0'),
              (SELECT median(g.gross_pay) FROM salary_roles r JOIN salaries_gold g
                 USING (employee_id) WHERE r.role = 'TEACHER')
        """).fetchone()
        gold_cols = [c[0] for c in con.sql("DESCRIBE salaries_gold").fetchall()]
        role_cols = [c[0] for c in con.sql("DESCRIBE salary_roles").fetchall()]

    role_table = "\n".join(
        ["| Role | Employees | Median gross pay |", "|---|---|---|"]
        + [f"| {role} | {n:,} | ${med:,.0f} |" for role, n, med in top_roles]
    )
    description = DESCRIPTION.format(
        employees=employees, role_rows=role_rows, distinct_roles=distinct_roles, total=total,
        median=median, max_pay=max_pay, multi=multi, multi_pct=100 * multi / employees,
        role_table=role_table, period=PERIOD, with_days=with_days, dash0_roles=dash0_roles,
        dash0_employees=dash0_employees, dash0_multi=dash0_multi, dash0_median=dash0_median,
        teacher_median=teacher_median, source_name=SOURCE_NAME, source_url=SOURCE_URL,
    )

    def fields(cols, docs):
        return [{"name": c, "description": docs.get(c, "")} for c in cols]

    metadata = {
        "title": "Olentangy School District Employee Salaries (2025)",
        "id": KAGGLE_ID,
        "subtitle": f"2025 gross pay for {employees:,} employees of a large Ohio public school district",
        "description": description,
        "isPrivate": PRIVATE,
        # canonical server-side name: the short "CC-BY-4.0" form is accepted by
        # `datasets version` but rejected by `datasets metadata --update`
        "licenses": [{"name": "Attribution 4.0 International (CC BY 4.0)"}],
        # These three only apply via `kaggle datasets metadata --update`, never via
        # `datasets version` — see the publish-metadata make target. The frequency must be
        # lowercase; "Annually" is rejected.
        "expectedUpdateFrequency": "annually",
        "userSpecifiedSources": (
            f"Obtained by The Columbus Dispatch through a public records request and published "
            f"{PUBLISHED} in their searchable database of central Ohio public payrolls, part of "
            f"an ongoing series covering school districts, local governments and colleges. This "
            f"dataset is the Olentangy Local School District subset, reprocessed through a "
            f"Dagster bronze/silver/gold pipeline that types the pay figures, splits the "
            f"multi-role position field and removes employee names. {SOURCE_URL}"
        ),
        # Kaggle caps the tag count and only accepts its own tag slugs — an over-long or
        # invented list fails the whole create with "max category limit"
        "keywords": ["education", "government", "employment", "economics"],
        "resources": [
            {
                "path": "olentangy_salaries.csv",
                "description": f"One row per employee ({employees:,}): surrogate id, raw position string, contract days where published, and 2025 gross pay in USD.",
                "schema": {"fields": fields(gold_cols, COLUMN_DOCS)},
            },
            {
                "path": "olentangy_salary_roles.csv",
                "description": f"One row per employee-role ({role_rows:,}); joins to olentangy_salaries.csv on employee_id.",
                "schema": {"fields": fields(role_cols, ROLE_COLUMN_DOCS)},
            },
        ],
    }
    (KAGGLE_DIR / "dataset-metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    # the card goes to gold, NOT the upload dir: `kaggle datasets create` uploads every
    # file it finds except dataset-metadata.json, and a stray README would ship as an
    # undescribed data file and cost usability points.
    (GOLD_DIR / "README.md").write_text(description)

    missing = [c for c in gold_cols + role_cols if c not in COLUMN_DOCS and c not in ROLE_COLUMN_DOCS]
    files = sorted(p.name for p in KAGGLE_DIR.iterdir() if p.is_file())
    return dg.MaterializeResult(
        metadata={
            "kaggle_id": KAGGLE_ID,
            "files": dg.MetadataValue.md("\n".join(f"- `{f}`" for f in files)),
            "undocumented_columns": missing or "none",
            "upload_command": f"kaggle datasets create -p {KAGGLE_DIR} -u --dir-mode zip",
            "dataset_card": dg.MetadataValue.md(description),
        }
    )


def _check() -> None:
    """Smallest thing that fails if the modeling logic breaks."""
    assert SOURCE_CSV.exists(), f"missing source csv: {SOURCE_CSV}"
    with duckdb.connect(DUCKDB_PATH) as con:
        bad = con.sql("SELECT count(*) FROM salaries_gold WHERE gross_pay IS NULL").fetchone()[0]
        assert bad == 0, f"{bad} rows failed dollar-string parsing"
        n, roles = con.sql(
            "SELECT (SELECT count(*) FROM salaries_gold), (SELECT count(*) FROM salary_roles)"
        ).fetchone()
        assert n == 4238, f"expected 4238 employees, got {n}"
        assert roles > n, "role table should have more rows than employee table"
        # every employee has exactly one role_seq=1 row, so the gold join cannot fan out
        dupes = con.sql(
            "SELECT count(*) FROM (SELECT employee_id FROM salary_roles WHERE role_seq = 1 "
            "GROUP BY 1 HAVING count(*) > 1)"
        ).fetchone()[0]
        assert dupes == 0, f"{dupes} employees have a duplicated primary role"

        # gold rolls contract_days up with max(), which is only safe while no employee
        # holds two different day counts
        conflicts = con.sql("""
            SELECT count(*) FROM (
              SELECT employee_id FROM salary_roles WHERE contract_days IS NOT NULL
              GROUP BY 1 HAVING count(DISTINCT contract_days) > 1)
        """).fetchone()[0]
        assert conflicts == 0, f"{conflicts} employees hold conflicting contract_days"
        blank = con.sql("SELECT count(*) FROM salary_roles WHERE role_base = ''").fetchone()[0]
        assert blank == 0, f"{blank} roles parsed down to an empty role_base"
        odd = con.sql(
            "SELECT count(*) FROM salary_roles WHERE contract_days NOT BETWEEN 150 AND 300"
        ).fetchone()[0]
        assert odd == 0, f"{odd} roles have an implausible contract_days"
        # the card states each -0 employee carries exactly one such line
        many = con.sql("""
            SELECT count(*) FROM (
              SELECT employee_id FROM salary_roles WHERE role_suffix = '-0'
              GROUP BY 1 HAVING count(*) > 1)
        """).fetchone()[0]
        assert many == 0, f"{many} employees carry more than one '-0' line; card wording is now wrong"

    # the publishing boundary: no name column may reach an exported file
    meta = json.loads((KAGGLE_DIR / "dataset-metadata.json").read_text())
    for r in meta["resources"]:
        undocumented = [f["name"] for f in r["schema"]["fields"] if not f["description"]]
        assert not undocumented, f"{r['path']} has undocumented columns: {undocumented}"
    names = {"first_name", "last_name", "full_name", "first", "last", "name", "employee_name"}

    # The committed source is name-free by design; this repo is public, so catch a
    # re-added name column at ingestion rather than one layer before publication.
    src_header = {c.strip().lower() for c in SOURCE_CSV.read_text().split("\n", 1)[0].split(",")}
    assert not src_header & names, (
        f"{SOURCE_CSV.name} contains name columns {sorted(src_header & names)} — strip them "
        f"before committing; see the source-CSV note in the README"
    )

    for export in KAGGLE_DIR.glob("*.csv"):
        header = {c.strip().lower() for c in export.read_text().split("\n", 1)[0].split(",")}
        leaked = header & names
        assert not leaked, f"{export.name} leaks name columns: {leaked}"

    # everything here is uploaded except dataset-metadata.json, so nothing may sit in
    # this directory that isn't a declared resource
    declared = {r["path"] for r in meta["resources"]} | {"dataset-metadata.json"}
    stray = {p.name for p in KAGGLE_DIR.iterdir() if p.is_file()} - declared
    assert not stray, f"undeclared files would be uploaded to Kaggle: {sorted(stray)}"
    print("ok")


if __name__ == "__main__":
    _check()
