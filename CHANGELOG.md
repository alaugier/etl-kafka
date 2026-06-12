# Changelog — ETL Kafka E-commerce

## Milestone 1 — Fondamentaux Kafka & Avro

### data_prep.py
- Lecture du dataset Kaggle 2019-Oct.csv (4,4 M lignes) par chunks de 100k
- Filtrage des lignes invalides (champs obligatoires nuls)
- `category_id` lu en `dtype=str` pour éviter l'overflow int64
- Production : 1 000 000 lignes JSONL, ~262 Mo

### Schémas Avro (schemaless, sans Schema Registry)
- `event_raw.avsc` : `category_id` en `string` (valeurs > int64 max)
- `event_clean.avsc` : `category_l1`, `category_l2`, `brand` normalisé
- `cart_abandonment_alert.avsc` : alerte avec taux d'abandon

### Producer
- `enable.idempotence=True`, `acks=all`
- Partitionnement par `user_id` (ordre garanti par session)
- Gestion `BufferError` séparée des erreurs de sérialisation
- Débit mesuré : ~137 000 msg/s

### Consumer/Transformer
- Transformation `ecommerce-raw` → `ecommerce-clean`
- Parsing `category_code` → `category_l1` / `category_l2`
- Commit offset manuel après production (at-least-once)

---

## Milestone 2 — Pipeline ETL & Sinks

### PostgresSink
- Tables `ecommerce_events` et `category_funnel`
- `ON CONFLICT DO NOTHING` → idempotence garantie
- `update_funnel` : agrégation quotidienne par catégorie

### ElasticsearchSink
- Bulk indexing avec `_id` déterministe
- `number_of_replicas: 0` → index vert sur cluster single-node
- 638 003 documents indexés

### Pipeline ETL
- Composants injectables pour les tests unitaires
- Flush ordonné : PostgreSQL → Elasticsearch → commit offset
- Débit : ~7 800 msg/s, 638 003 lignes PostgreSQL

### Détecteur d'abandons de panier
- Windowing 30 min basé sur l'horloge murale
- Dict TTL avec éviction toutes les 120 s
- 3 catégories alertées : accessories (85,7%), sport (100%), medicine (100%)

---

## Milestone 3 — Exactly-once & Résilience

### Crash-test SIGKILL validé
- Reset offsets `etl-pipeline` → `earliest`
- Kill SIGKILL à 200k événements traités
- Redémarrage depuis le dernier offset commité
- Résultat final : 638 003 lignes PostgreSQL, zéro doublon

---

## Milestone 4 — Tests & CI/CD

### Suite de tests unitaires (46 cas)
| Module | Tests |
|---|---|
| `test_consumer.py` | 15 — transform_event, timestamps naïfs |
| `test_cart_abandonment.py` | 11 — detect_abandonment, TTL, alertes |
| `test_producer.py` | 5 — send_events, partitionnement, callback |
| `test_pipeline.py` | 6 — setup, flush, run (injectables) |
| `test_sinks.py` | 9 — PostgresSink, ElasticsearchSink |

Couverture globale : >70% (data_prep.py et run_*() exclus, dépendants d'un broker)

### GitHub Actions CI
- Matrix Python 3.11 + 3.12
- Jobs parallèles : `test` + `lint` (ruff)
- Upload artifact `coverage.xml`
