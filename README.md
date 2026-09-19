# Olentangy School District Salaries

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.22849877.svg)](https://doi.org/10.5281/zenodo.22849877)

A Dagster pipeline that turns Olentangy Local School District's published payroll into an
analysis-ready Kaggle dataset, plus the EDA notebook that goes with it.

**Dataset:** [robschieber/olentangy-school-district-salaries-2025](https://www.kaggle.com/datasets/robschieber/olentangy-school-district-salaries-2025)
· **Source:** [The Columbus Dispatch central Ohio payroll database](https://www.dispatch.com/story/news/local/2026/06/29/public-payroll-how-central-ohio-government-agencies-spend-your-money/90691984007/)
· **License:** CC BY 4.0

## Why it exists

4,238 employees, calendar year 2025. The published data is four columns of free text, and two
things in it are annoying enough to be worth a pipeline:

- **Gross pay is a string** (`"$291,734.99"`).
- **The position field packs multiple jobs into one delimited cell.** 39% of employees hold
  more than one — teachers who also coach, drivers with supplemental routes — across 88
  distinct roles. Some roles also hide a contract length (`SECRETARY 260` = 260 days/year).

The pipeline types the money, splits the roles into their own table, pulls out `contract_days`,
and drops employee names. It does not invent job families, pay bands or rankings; those are
one-liners for whoever downloads it, and the role taxonomy is a genuinely open question.

## Layers

| Asset | Layer | Output |
|---|---|---|
| `salaries_raw` | bronze | `data/bronze/salaries_raw.parquet` — all VARCHAR, zero cleaning |
| `salaries_typed` | silver | `data/silver/salaries.duckdb` — `salaries` + `salary_roles` |
| `salaries_modeled` | gold | `salaries_gold` table + Parquet, names dropped |
| `kaggle_bundle` | gold | `data/kaggle/` — CSVs + `dataset-metadata.json`, and the card |

Bronze is a faithful landing of the committed source. Silver is the queryable DuckDB layer.
Gold is the publishing shape.

### A note on the source CSV

`data/source/olentangy_salaries.csv` has employee **names removed**. The district publishes
them and they are Ohio public record, but the published dataset deliberately omits them, and
committing them here would undo that permanently in git history and in a DOI-archived
snapshot.

Nothing analytical is lost: all 4,238 rows carry distinct names, so names group nothing —
each person appears exactly once, and the multi-role signal lives entirely in the
semicolon-delimited position field, which the pipeline unnests. A hashed name would be no
better: it would be unique per row like `employee_id` already is, while being re-identifiable
by hashing a known employee roster.

`employee_id` is a surrogate key assigned by ordering on `(gross_pay DESC, positions)`. It is
**not stable across dataset versions** — do not use it to join across releases.

## Usage

```bash
make install                     # sync the venv from uv.lock
make build                       # materialize bronze -> silver -> gold -> kaggle
make test                        # assert the built data is sane and carries no names
make dev                         # Dagster UI (DAGSTER_PORT=3001)
make notebook                    # JupyterLab against the silver DuckDB
```

Query the silver layer directly:

```bash
duckdb data/silver/salaries.duckdb -c "SELECT role_base, count(*) FROM salary_roles GROUP BY 1 ORDER BY 2 DESC"
```

### Publishing

```bash
make publish M="what changed"    # data + metadata
make publish-cover               # cover image
```

`kaggle datasets version` uploads data but **silently discards** file descriptions, column
descriptors, provenance and update frequency — only `datasets metadata --update` applies
those, which is why `publish` runs both. See `publish-metadata` in the Makefile.

## Notebooks

- [`notebooks/eda.ipynb`](notebooks/eda.ipynb) — the public EDA, six charts with commentary.
  Loads from `/kaggle/input`, a local checkout, or kagglehub, in that order.
- [`notebooks/explore.ipynb`](notebooks/explore.ipynb) — scratch queries against silver.

Findings worth knowing before you model anything:

- **Pay is bimodal**, so the $49,149 median describes almost nobody — it sits in the valley
  between 1,435 part-time staff under $25k and a full-time band near $100k.
- **Holding more positions predicts *lower* pay** ($52,727 at one, $20,316 at three). Multi-role
  employees are substitutes and aides piecing together part-time work, not teachers stacking
  stipends. `n_roles` is a confound for employment category.
- **Contract days only predicts pay in interaction with role.** Counsellors earn $630/day on
  fewer days than a secretary at $270/day.

## Caveats

`gross_pay` includes overtime, stipends, holiday and accrued time — it is not contracted salary.
There is no FTE, tenure or degree field. Several role codes (`CMF`, `CLAS-NON`, `LT-PLCMT`,
`UNIV-PLA`) have no published key and are passed through verbatim rather than guessed at. The
file carries no date column; the 2025 framing comes from the source's database note. The
Dispatch disclosed using AI tooling to clean the upstream database.

Names are removed, but this is not full anonymization — an employee in a unique role
(`ADMIN; SUPERINTENDENT`) remains identifiable from role and pay. Please analyze institutions,
not individuals.

## Citing

> Schieber, R. (2026). *Olentangy School District Employee Salaries (2025)*.
> Zenodo. https://doi.org/10.5281/zenodo.22849877

The DOI above resolves to a specific release. Zenodo also issues a **concept DOI** that always
resolves to the newest version — prefer that one when citing the dataset generally rather than
a fixed snapshot; it's shown on the Zenodo record under "Cite all versions".

Machine-readable metadata lives in [`CITATION.cff`](CITATION.cff) (GitHub's "Cite this
repository") and [`.zenodo.json`](.zenodo.json) (the Zenodo record for each release).

Please also credit **The Columbus Dispatch** as the source of the payroll data — the figures
are Ohio public record, but the records request and compilation are theirs.
