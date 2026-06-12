# Architecture ETL Kafka — e-Commerce

## Vue d'ensemble

```
┌─────────────────────────────────────────────────────────────────┐
│                        SOURCE LAYER                             │
│  2019-Oct.csv / 2019-Nov.csv  ──→  data_prep.py  ──→  .jsonl   │
└───────────────────────────────┬─────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                       PRODUCER LAYER                            │
│  producer.py                                                    │
│  • Lecture JSONL par chunks (10 000 lignes)                     │
│  • Sérialisation Avro (schemaless, fastavro)                    │
│  • Partitionnement par user_id                                  │
│  • Idempotence : enable.idempotence=true                        │
│  • Topic : ecommerce-raw  (6 partitions, rétention 24h)         │
└───────────────────────────────┬─────────────────────────────────┘
                                │ Topic: ecommerce-raw
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                      TRANSFORM LAYER                            │
│  consumer.py                                                    │
│  • Désérialisation Avro → dict                                  │
│  • Validation champs requis                                     │
│  • brand NULL → 'unknown'                                       │
│  • category_code → category_l1 / category_l2                   │
│  • Normalisation event_time → ISO 8601 UTC                      │
│  • Commit offset manuel (après publication downstream)          │
│  • Topic : ecommerce-clean  (6 partitions, rétention 48h)       │
└────────────────────┬───────────────────────┬────────────────────┘
                     │ Topic: ecommerce-clean  │
           ┌─────────┘                         └──────────┐
           ▼                                              ▼
┌──────────────────────┐              ┌────────────────────────────────┐
│    pipeline.py       │              │  cart_abandonment_detector.py  │
│    SINK DUAL         │              │  • Windowing 30min (dict+TTL)  │
│                      │              │  • Détection cart sans purchase │
│  • PostgreSQL 16     │              │  • Alerte si abandon > 80%     │
│    ecommerce_events  │              │  • Topic: ecommerce-alerts     │
│    category_funnel   │              │    (1 partition, rétention 7j) │
│                      │              └────────────────────────────────┘
│  • Elasticsearch     │
│    ecommerce-events  │
│    (full-text +      │
│     dashboard)       │
└──────────────────────┘
```

## Topics Kafka

| Topic               | Partitions | Rétention | Producteur   | Consommateurs          |
|---------------------|-----------|-----------|--------------|------------------------|
| `ecommerce-raw`     | 6         | 24h       | producer.py  | consumer.py            |
| `ecommerce-clean`   | 6         | 48h       | consumer.py  | pipeline.py, detector  |
| `ecommerce-alerts`  | 1         | 7j        | detector.py  | (monitoring, dashboards)|

## Schémas Avro

Sérialisation **schemaless** (sans Schema Registry) via `fastavro` :
- Le schéma est chargé depuis le fichier `.avsc` à l'initialisation
- `fastavro.schemaless_writer` / `fastavro.schemaless_reader` pour compacité maximale

| Fichier                       | Enregistrement        | Usage                  |
|-------------------------------|-----------------------|------------------------|
| `schemas/event_raw.avsc`      | `EventRaw`            | Producer → raw topic   |
| `schemas/event_clean.avsc`    | `EventClean`          | Consumer → clean topic |
| `schemas/cart_abandonment_alert.avsc` | `CartAbandonmentAlert` | Detector → alerts |

## Garanties de livraison

- **Producer** : idempotent (`enable.idempotence=true`, `acks=all`)
- **Consumer** : at-least-once — offset committé **après** publication vers clean
- **Pipeline** : at-least-once — offset committé **après** confirmation des deux sinks
- **Déduplication** : `ON CONFLICT DO NOTHING` en PostgreSQL et `_id` Elasticsearch

## Modèle de données PostgreSQL

```sql
-- Événements bruts nettoyés
ecommerce_events (
    event_time, event_type, product_id, category_id,
    category_l1, category_l2, brand, price, user_id, user_session
    PRIMARY KEY (user_session, event_time, product_id, event_type)
)

-- Funnel de conversion agrégé par catégorie/jour
category_funnel (
    category_l1, event_date,
    view_count, cart_count, purchase_count
    PRIMARY KEY (category_l1, event_date)
)
```

## Infrastructure Docker

| Service         | Image                              | Port  |
|-----------------|------------------------------------|-------|
| Kafka (KRaft)   | confluentinc/cp-kafka:7.6.1        | 9092  |
| Kafdrop         | obsidiandynamics/kafdrop:4.0.2     | 9000  |
| PostgreSQL 16   | postgres:16-alpine                 | 5432  |
| Elasticsearch   | elasticsearch:8.14.3               | 9200  |
