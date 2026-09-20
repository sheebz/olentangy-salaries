# Olentangy salaries pipeline. `make` on its own lists targets.

# port 3000 is often taken on this machine, so default the UI off it
DAGSTER_PORT ?= 3001
JUPYTER_PORT ?= 8888
DATA := data
COVER := assets/dataset-cover-image.jpg

KAGGLE_USER := robschieber
KAGGLE_ID := $(KAGGLE_USER)/olentangy-school-district-salaries-2025

.DEFAULT_GOAL := help
.PHONY: help install dev build check test notebook publish publish-metadata publish-cover publish-kernel clean distclean

help: ## List available targets
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | sort | \
		awk -F':.*?## ' '{printf "  \033[36m%-11s\033[0m %s\n", $$1, $$2}'

install: ## Sync the virtualenv from uv.lock
	uv sync

dev: ## Serve the Dagster UI (DAGSTER_PORT=3001)
	uv run dg dev -p $(DAGSTER_PORT)

build: ## Materialize the full bronze -> silver -> gold -> kaggle graph
	uv run dg launch --assets '*'

check: ## Validate that all definitions load
	uv run dg check defs

test: ## Assert the built data is sane and carries no names
	uv run python -m school_district_salaries.defs.kaggle

notebook: ## Serve JupyterLab against the silver DuckDB (JUPYTER_PORT=8888)
	uv run jupyter lab --port $(JUPYTER_PORT) --notebook-dir notebooks

publish: test ## Push data + metadata to Kaggle (M="version notes")
	@test -n '$(M)' || (echo 'usage: make publish M="what changed"' && exit 1)
	.venv/bin/kaggle datasets version -p data/kaggle -m '$(M)'
	$(MAKE) publish-metadata
	@echo 'https://www.kaggle.com/datasets/$(KAGGLE_ID)'

publish-metadata: ## Apply file/column descriptions, provenance and update frequency
	@# `datasets version` silently ignores all of these; only --update applies them
	.venv/bin/kaggle datasets metadata --update $(KAGGLE_ID) -p data/kaggle

publish-kernel: ## Push notebooks/eda.ipynb as the public Kaggle kernel
	@# re-executes it locally first so the published copy carries current outputs
	.venv/bin/jupyter nbconvert --to notebook --execute --inplace notebooks/eda.ipynb
	.venv/bin/kaggle kernels push -p notebooks
	@echo 'https://www.kaggle.com/code/$(KAGGLE_USER)/olentangy-salaries-eda'

publish-cover: ## Upload assets/dataset-cover-image.jpg as the dataset cover
	@test -f $(COVER) || (echo 'missing $(COVER)' && exit 1)
	@# staged in its own dir: anything sitting in data/kaggle/ ships as a DATA file
	rm -rf $(DATA)/.cover && mkdir -p $(DATA)/.cover
	cp $(DATA)/kaggle/dataset-metadata.json $(COVER) $(DATA)/.cover/
	.venv/bin/kaggle datasets metadata --update $(KAGGLE_ID) -p $(DATA)/.cover
	rm -rf $(DATA)/.cover

clean: ## Delete derived data, keeping the source CSV
	rm -rf $(DATA)/bronze $(DATA)/silver $(DATA)/gold $(DATA)/kaggle

distclean: clean ## Also delete the virtualenv and Dagster run history
	rm -rf .venv $(DATA)/dagster_home
