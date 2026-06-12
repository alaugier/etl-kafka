.PHONY: up down logs build docker-app prep producer consumer pipeline detector test

# Infrastructure only
up:
	docker compose up -d

down:
	docker compose down -v

logs:
	docker compose logs -f

# Docker full stack (infra + app)
build:
	docker compose build

docker-app:
	docker compose --profile app up -d

# Local dev — requires PYTHONPATH
prep:
	PYTHONPATH=src python src/data_prep.py --input $(INPUT) --output data/sample/sample_data.jsonl

producer:
	PYTHONPATH=src python src/producer.py

consumer:
	PYTHONPATH=src python src/consumer.py

pipeline:
	PYTHONPATH=src python src/pipeline.py

detector:
	PYTHONPATH=src python src/cart_abandonment_detector.py

# Tests
test:
	PYTHONPATH=src pytest tests/ -v --cov=src --cov-report=term-missing

test-unit:
	PYTHONPATH=src pytest tests/ -v -k "not integration"
