# Kafka Cheatsheet

## Topics

```bash
# Lister les topics
docker exec tp-kafka kafka-topics --bootstrap-server localhost:9092 --list

# Créer un topic manuellement
docker exec tp-kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic ecommerce-raw --partitions 6 --replication-factor 1

# Décrire un topic (partitions, offsets, leaders)
docker exec tp-kafka kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic ecommerce-raw

# Supprimer un topic
docker exec tp-kafka kafka-topics --bootstrap-server localhost:9092 \
  --delete --topic ecommerce-raw
```

## Lecture (console consumer)

```bash
# Lire depuis le début
docker exec tp-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ecommerce-raw \
  --from-beginning \
  --max-messages 10

# Lire un topic avec clé visible
docker exec tp-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ecommerce-raw \
  --property print.key=true \
  --from-beginning
```

## Consumer groups

```bash
# Lister les groupes
docker exec tp-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 --list

# Décrire le lag d'un groupe
docker exec tp-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group etl-pipeline \
  --describe

# Réinitialiser les offsets (pour rejouer depuis le début)
docker exec tp-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group etl-pipeline \
  --topic ecommerce-clean \
  --reset-offsets --to-earliest \
  --execute
```

## Monitoring des offsets

```bash
# Voir les offsets courants par partition
docker exec tp-kafka kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 \
  --topic ecommerce-raw
```

## Performance

```bash
# Test de débit producteur
docker exec tp-kafka kafka-producer-perf-test \
  --topic ecommerce-raw \
  --num-records 100000 \
  --record-size 256 \
  --throughput -1 \
  --producer-props bootstrap.servers=localhost:9092

# Test de débit consommateur
docker exec tp-kafka kafka-consumer-perf-test \
  --bootstrap-server localhost:9092 \
  --topic ecommerce-raw \
  --messages 100000 \
  --group perf-test
```

## KRaft (sans ZooKeeper)

Ce setup utilise Kafka en mode **KRaft** (Kafka Raft Metadata) — pas de ZooKeeper.

```bash
# Vérifier le statut du broker
docker exec tp-kafka kafka-broker-api-versions \
  --bootstrap-server localhost:9092

# Metadata du cluster
docker exec tp-kafka kafka-metadata-quorum \
  --bootstrap-server localhost:9092 \
  --command-id describe
```

## Kafdrop

Interface web disponible sur [http://localhost:9000](http://localhost:9000)

- Vue des topics et partitions
- Parcours des messages
- Consumer groups et lag
- Création/suppression de topics

## Commandes utiles Docker Compose

```bash
# Démarrer l'infrastructure
docker compose up -d

# Vérifier la santé des services
docker compose ps

# Logs Kafka
docker compose logs -f kafka

# Accéder au shell Kafka
docker exec -it tp-kafka bash

# Arrêter et supprimer les volumes (reset complet)
docker compose down -v
```

## Variables clés confluent-kafka (Python)

| Variable                                | Valeur recommandée | Effet                             |
|-----------------------------------------|--------------------|-----------------------------------|
| `enable.idempotence`                    | `True`             | Producer idempotent               |
| `acks`                                  | `all`              | Toutes les replicas acquittent    |
| `max.in.flight.requests.per.connection` | `5`                | Requis avec idempotence           |
| `enable.auto.commit`                    | `False`            | Commit manuel côté consumer       |
| `auto.offset.reset`                     | `earliest`         | Rejouer depuis le début           |
| `linger.ms`                             | `5`                | Batching micro pour le débit      |
| `compression.type`                      | `gzip`             | Réduction taille des messages     |
