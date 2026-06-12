# ETL Kafka — Pipeline e-Commerce en temps réel

Pipeline ETL streaming complet : ingestion CSV → Kafka → transformation → PostgreSQL + Elasticsearch + détection d'abandon de panier.

## Prérequis

- Docker Desktop ≥ 6 Go RAM, 20 Go disque
- Python 3.11+
- Dataset Kaggle : [eCommerce Behavior Data](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)

## Setup rapide

```bash
# 1. Cloner et configurer
cp .env.example .env

# 2. Installer les dépendances Python
pip install -r requirements.txt

# 3. Démarrer l'infrastructure
docker compose up -d
# → Kafka:9092 | Kafdrop:9000 | PostgreSQL:5432 | Elasticsearch:9200
```

## Utilisation

### 1. Préparer les données (Issue #1)

```bash
python src/data_prep.py --input /chemin/vers/2019-Oct.csv \
                        --output data/sample/sample_data.jsonl \
                        --sample-size 1000000
```

### 2. Producer — CSV → ecommerce-raw (Issue #4)

```bash
PYTHONPATH=src python src/producer.py
# ou : make producer
```

Débit cible : **1 000 msg/s**. Vérifier via Kafdrop : [http://localhost:9000](http://localhost:9000)

### 3. Consumer — Transformation ETL (Issue #5)

```bash
PYTHONPATH=src python src/consumer.py
# ou : make consumer
```

Transformations : nettoyage nulls, parsing `category_code`, normalisation timestamps UTC.

### 4. Pipeline complet — Dual sink (Issue #8)

```bash
PYTHONPATH=src python src/pipeline.py
# ou : make pipeline
```

Débit cible : **5 000 msg/s**. Écrit dans PostgreSQL et Elasticsearch simultanément.

### 5. Détecteur d'abandon de panier (Issue #9)

```bash
PYTHONPATH=src python src/cart_abandonment_detector.py
# ou : make detector
```

Fenêtre glissante 30 min. Alerte publiée sur `ecommerce-alerts` si taux > 80%.

## Avec Docker (stack complète)

```bash
# Construire les images Python
make build

# Démarrer infra + app
make docker-app
```

Les services Python (`producer`, `consumer`, `pipeline`, `cart-detector`) sont définis dans `compose.override.yml` avec le profil `app`.

## Tests

```bash
make test
# → pytest tests/ -v --cov=src --cov-report=term-missing
```

Couverture cible : **> 80%**. Tests 100% unitaires (Kafka, PostgreSQL, Elasticsearch mockés).

## Architecture

```
CSV → data_prep.py → sample_data.jsonl
                              │
                         producer.py
                              │
                    Topic: ecommerce-raw
                              │
                         consumer.py  (transform_event)
                              │
                    Topic: ecommerce-clean
                       ┌──────┴──────┐
                  pipeline.py    cart_abandonment_detector.py
                  ┌────┴────┐         │
              PostgreSQL  Elasticsearch  Topic: ecommerce-alerts
```

Voir [docs/architecture.md](docs/architecture.md) pour le détail complet.

## Structure du projet

```
etl_kafka/
├── compose.yml              # Infrastructure Docker (Kafka, PostgreSQL, Elasticsearch)
├── compose.override.yml     # Services Python (producer, consumer, pipeline, detector)
├── Dockerfile
├── Makefile
├── requirements.txt
├── .env.example
├── schemas/                 # Schémas Avro (fastavro, sans Schema Registry)
│   ├── event_raw.avsc
│   ├── event_clean.avsc
│   └── cart_abandonment_alert.avsc
├── src/
│   ├── data_prep.py
│   ├── producer.py
│   ├── consumer.py
│   ├── pipeline.py
│   ├── cart_abandonment_detector.py
│   └── modules/
│       ├── avro_serializer.py
│       ├── avro_deserializer.py
│       ├── postgres_sink.py
│       └── elasticsearch_sink.py
├── tests/
│   ├── conftest.py
│   ├── test_consumer.py
│   ├── test_producer.py
│   ├── test_pipeline.py
│   ├── test_cart_abandonment.py
│   └── test_sinks.py
└── docs/
    ├── architecture.md
    └── kafka_cheatsheet.md
```

## Topics Kafka

| Topic               | Partitions | Rétention |
|---------------------|-----------|-----------|
| `ecommerce-raw`     | 6         | 24h       |
| `ecommerce-clean`   | 6         | 48h       |
| `ecommerce-alerts`  | 1         | 7j        |

## Critères de validation

- [ ] Producer : 1 000 msg/s vérifiés via Kafdrop
- [ ] Pipeline : 5 000 msg/s
- [ ] Crash-test : redémarrage sans perte ni doublon (exactly-once PostgreSQL)
- [ ] Détecteur : alertes visibles sur `ecommerce-alerts` (abandon > 80%)
- [ ] Tests : couverture > 80%
