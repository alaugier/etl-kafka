# Présentation — Pipeline ETL Kafka e-Commerce

**TP Data Engineering — 12h30**  
Stack : Apache Kafka · Avro · PostgreSQL · Elasticsearch · Docker · Python 3.12

---

## 1. Contexte et objectif

Construire un pipeline de streaming ETL complet à partir de données réelles de comportement e-commerce (Kaggle, octobre 2019 — 4,4 millions d'événements).

```
CSV → [Producer] → ecommerce-raw → [Consumer] → ecommerce-clean
                                                        │
                                          ┌─────────────┴─────────────┐
                                     [Pipeline]              [Détecteur]
                                    ┌────┴────┐                   │
                                PostgreSQL  Elasticsearch   ecommerce-alerts
```

---

## 2. Milestone 1 — Fondamentaux Kafka & Avro

### Préparation des données (`data_prep.py`)

- Lecture du CSV par chunks de 100 000 lignes pour limiter la RAM
- `category_id` forcé en `dtype=str` : les valeurs comme `2103807459595387724` dépassent la précision de `float64` (53 bits de mantisse), elles seraient arrondies si lues comme nombres
- Résultat : 1 000 000 lignes JSONL, 262 Mo

### Sérialisation Avro schemaless

Choix **fastavro sans Schema Registry** :

| | Schemaless (notre choix) | Schema Registry |
|---|---|---|
| Infrastructure | Aucune | Service HTTP Confluent |
| Header par message | 0 byte | 5 bytes (magic + schema ID) |
| Évolution de schéma | Non | Oui (versions, compatibilité) |
| Usage | TP, pipeline fermé | Production, multi-équipes |

Les schémas `.avsc` sont versionnés dans le dépôt et partagés entre producer et consumer.

### Producer (`producer.py`)

- `enable.idempotence=True` + `acks=all` : chaque message est confirmé par le leader et tous les ISR
- Partitionnement par `user_id` → tous les événements d'un même utilisateur vont sur la même partition → ordre garanti par session
- Retry sur `BufferError` (queue interne pleine) séparé des erreurs de sérialisation
- **Débit mesuré : ~137 000 msg/s**

### Consumer/Transformer (`consumer.py`)

- Parsing `category_code` → `category_l1` / `category_l2` (`"electronics.smartphone"` → `l1=electronics`, `l2=smartphone`)
- `brand=None` → `'unknown'` (normalisation)
- Offset commité **après** production sur `ecommerce-clean` → garantie at-least-once
- Topic `ecommerce-clean` créé avec 6 partitions et rétention 48h au démarrage

---

## 3. Milestone 2 — Pipeline ETL & Sinks

### Sink PostgreSQL (`postgres_sink.py`)

Deux tables :

**`ecommerce_events`** — clé primaire `(user_session, event_time, product_id, event_type)`
```sql
INSERT ... ON CONFLICT (user_session, event_time, product_id, event_type) DO NOTHING
```
→ idempotence garantie : rejouer les mêmes messages ne crée pas de doublons.

**`category_funnel`** — clé primaire `(category_l1, event_date)`
```sql
ON CONFLICT DO UPDATE SET view_count = view_count + EXCLUDED.view_count, ...
```
→ compteurs de conversion incrémentaux par catégorie et par jour.

Résultat après traitement complet : **638 003 lignes**.

### Sink Elasticsearch (`elasticsearch_sink.py`)

- `_id` déterministe : `{user_session}_{event_time}_{product_id}_{event_type}`  
  → la même donnée indexée deux fois écrase la première sans doublon
- `number_of_replicas: 0` → index vert sur cluster single-node (pas de nœud pour héberger les réplicas)
- Indexation en bulk : 638 003 documents

### Pipeline ETL (`pipeline.py`)

Séquence de flush garantissant l'at-least-once :

```
1. postgres.write_events(batch)
2. postgres.update_funnel(batch)
3. elasticsearch.write_events(batch)
4. consumer.commit(offset)   ← commité EN DERNIER
```

Si le processus est tué entre 3 et 4, le batch est rejoué au redémarrage. L'idempotence des sinks absorbe les doublons.

**Débit mesuré : ~7 800 msg/s**

### Détecteur d'abandons de panier (`cart_abandonment_detector.py`)

- Fenêtre glissante de 30 minutes basée sur l'**horloge murale** (et non les timestamps Kafka, qui sont historiques — 2019)
- Dict TTL en mémoire, éviction toutes les 120 s
- Taux d'abandon = `cart_count / (cart_count + purchase_count)`
- Alerte publiée sur `ecommerce-alerts` si taux ≥ 80%

Résultats sur le dataset :
| Catégorie | Taux d'abandon |
|---|---|
| accessories | 85,7% |
| sport | 100% |
| medicine | 100% |

---

## 4. Milestone 3 — Exactly-once & Résilience

### Protocole du crash-test

```bash
# 1. Reset offsets au début du topic
kafka-consumer-groups --reset-offsets --group etl-pipeline \
  --topic ecommerce-clean --to-earliest --execute

# 2. Truncate PostgreSQL
TRUNCATE ecommerce_events, category_funnel;

# 3. Lancer le pipeline, killer à ~200k événements
python src/pipeline.py &
kill -9 $PID   # SIGKILL — pas de cleanup possible

# 4. Redémarrer depuis le dernier offset commité
python src/pipeline.py
```

### Résultat

| Étape | Lignes PostgreSQL | Offset commité |
|---|---|---|
| Après crash | 296 862 | ~299 799 |
| Après restart | **638 003** | 638 287 |

Le gap entre l'offset commité et les lignes en base démontre l'**at-least-once** : des messages ont été retraités. L'absence de doublons confirme l'idempotence (`ON CONFLICT DO NOTHING`).

### Résilience PostgreSQL (fix post-TP)

**Bug découvert :** deux instances pipeline simultanées ont provoqué un deadlock sur `category_funnel`. Sans `rollback()`, la connexion psycopg2 restait en état `aborted` et bloquait tous les batchs suivants.

**Fix :**
```python
try:
    execute_values(cur, sql, rows)
    conn.commit()
except Exception:
    conn.rollback()   # remet la connexion dans un état propre
    raise
```

---

## 5. Milestone 4 — Tests & CI/CD

### Suite de tests (55 cas)

| Fichier | Tests | Couverture cible |
|---|---|---|
| `test_avro.py` | 7 | Round-trip sérialisation/désérialisation, préservation `category_id` |
| `test_consumer.py` | 15 | `transform_event` — validation, parsing, timestamps naïfs |
| `test_cart_abandonment.py` | 11 | `detect_abandonment`, TTL, alertes par catégorie |
| `test_producer.py` | 5 | `send_events`, partitionnement par `user_id`, delivery callback |
| `test_pipeline.py` | 6 | `ETLPipeline` — setup, flush, run (composants injectables) |
| `test_sinks.py` | 11 | `PostgresSink` (dont rollback), `ElasticsearchSink` |

**Couverture globale : 88%**

Modules exclus du calcul (dépendants d'un broker Kafka réel) :
- `data_prep.py` — script CLI one-shot
- `run_consumer()`, `run_detector()`, `run_pipeline()` — boucles Kafka

### GitHub Actions CI

```yaml
on: [push, pull_request]   # branches main et dev

jobs:
  test:   # matrix Python 3.11 + 3.12
    - pytest tests/ --cov=src --cov-fail-under=70
  lint:
    - ruff check src/ tests/ --select E,F,W --ignore E501
```

### Docker Compose

```bash
docker compose up -d                    # infra seule (Kafka, Kafdrop, PG, ES)
docker compose --profile app up -d     # infra + tous les services Python
```

---

## 6. Points techniques à retenir

| Décision | Justification |
|---|---|
| Avro schemaless | Pas de dépendance à Confluent Platform, schéma stable dans le TP |
| `category_id` en `string` | Valeurs > précision `float64` pandas → arrondi silencieux |
| 6 partitions | Scalabilité horizontale, parallélisme de consommation |
| Offset après sink | Garantie at-least-once + idempotence côté sink = exactly-once effectif |
| `number_of_replicas=0` | Cluster single-node : réplicas non placés → index resterait `yellow` |
| Groupes de consommateurs séparés | Évite les rebalancing croisés entre consumer, pipeline et détecteur |
| `rollback()` sur erreur DB | Sans ça, une erreur ponctuelle bloque toute la connexion psycopg2 |
