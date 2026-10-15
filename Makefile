.PHONY: up down logs lint test test-unit

up:  ## build and start the whole stack
	docker compose up -d --build --wait

down:  ## stop the stack and remove its data
	docker compose down -v

logs:
	docker compose logs -f api outbox-relay consumer

lint:
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy

test:  ## needs Docker (testcontainers)
	uv run pytest

test-unit:
	uv run pytest tests/unit
