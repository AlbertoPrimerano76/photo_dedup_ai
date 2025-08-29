.PHONY: help install lint type test cov fmt all

help:
	@echo "make install  - install dev deps"
	@echo "make lint     - ruff (lint + fix) & black check"
	@echo "make type     - mypy type-check"
	@echo "make test     - pytest"
	@echo "make cov      - coverage report"
	@echo "make fmt      - black + ruff format"
	@echo "make all      - lint, type, test"

install:
	pip install -r requirements-dev.txt

lint:
	ruff check --fix .
	black --check .

type:
	mypy src tests

test:
	pytest

cov:
	coverage run -m pytest
	coverage report -m

fmt:
	black .
	ruff check --fix .

all: lint type test
