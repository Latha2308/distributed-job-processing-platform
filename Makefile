.PHONY: help install dev-setup test lint format run-api run-worker docker-up docker-down clean

help:
	@echo "Available commands:"
	@echo "  make install      - Install dependencies"
	@echo "  make dev-setup    - Set up development environment"
	@echo "  make test         - Run tests"
	@echo "  make lint         - Run linters"
	@echo "  make format       - Format code"
	@echo "  make run-api      - Run API server"
	@echo "  make run-worker   - Run worker"
	@echo "  make docker-up    - Start Docker services"
	@echo "  make docker-down  - Stop Docker services"
	@echo "  make clean        - Clean temporary files"

install:
	pip install -r requirements.txt

dev-setup:
	cp .env.example .env
	@echo "Please update .env file with your configuration"

test:
	pytest --cov=app --cov-report=html --cov-report=term

lint:
	black --check app tests
	flake8 app tests
	mypy app

format:
	black app tests
	isort app tests

run-api:
	uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

run-worker:
	python -m app.worker.worker

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

docker-logs:
	docker-compose logs -f

migrate:
	alembic upgrade head

migration:
	alembic revision --autogenerate -m "$(message)"

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf .pytest_cache
	rm -rf htmlcov
	rm -rf .coverage
