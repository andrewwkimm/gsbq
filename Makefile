help:
	cat Makefile

################################################################################

build:
	poetry install
	make reformat
	make lint
	make type_check
	make test

lint:
	poetry run flake8 tests

reformat:
	poetry run black tests

setup:
	pre-commit install --install-hooks
	poetry install

test:
	poetry run pytest -x --cov

type_check:
	poetry run mypy tests --ignore-missing-import

################################################################################

.PHONY: \
	build \
	help \
	lint \
	reformat \
	setup \
	test \
	type_check
