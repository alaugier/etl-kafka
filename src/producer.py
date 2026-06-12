"""Kafka producer: reads events from JSONL and publishes to ecommerce-raw."""
import json
import logging
import os
import sys
import time
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

load_dotenv()

# Allow running from project root: python src/producer.py
sys.path.insert(0, str(Path(__file__).parent))
from modules.avro_serializer import AvroSerializer  # noqa: E402

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
_TOPIC_RAW  = os.getenv("KAFKA_TOPIC_RAW", "ecommerce-raw")
_SCHEMA_PATH = os.getenv("SCHEMA_RAW_PATH", "schemas/event_raw.avsc")

_PRODUCER_CONFIG = {
    "bootstrap.servers": _BOOTSTRAP,
    "enable.idempotence": True,
    "acks": "all",
    "retries": 5,
    "max.in.flight.requests.per.connection": 5,
    "linger.ms": 5,
    "batch.size": 65536,
    "compression.type": "gzip",
    # Augmenté pour absorber les bursts sans "Queue full"
    "queue.buffering.max.messages": 1_000_000,
    "queue.buffering.max.kbytes": 512_000,
}


def _ensure_topic(topic: str, num_partitions: int = 6, retention_ms: int = 86_400_000) -> None:
    admin = AdminClient({"bootstrap.servers": _BOOTSTRAP})
    meta  = admin.list_topics(timeout=10)
    if topic in meta.topics:
        return
    new_topic = NewTopic(
        topic,
        num_partitions=num_partitions,
        replication_factor=1,
        config={"retention.ms": str(retention_ms)},
    )
    for t, f in admin.create_topics([new_topic]).items():
        try:
            f.result()
            logger.info("Topic created: %s (%d partitions)", t, num_partitions)
        except Exception as e:
            logger.warning("Topic creation skipped (%s): %s", t, e)


def _delivery_callback(err, msg) -> None:
    if err:
        logger.error("Delivery failed [%s]: %s", msg.topic(), err)


def send_events(filepath: str, topic: str, chunk_size: int = 10_000) -> None:
    """Lit un fichier JSONL par chunks et publie chaque événement sur Kafka.

    Args:
        filepath: Chemin vers le fichier JSONL source (produit par data_prep.py).
        topic: Nom du topic Kafka cible.
        chunk_size: Nombre de messages accumulés avant chaque poll.

    Raises:
        KafkaException: Si la connexion au broker échoue.
        FileNotFoundError: Si le fichier JSONL est introuvable.
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"Input file not found: {filepath}")

    _ensure_topic(topic)
    serializer = AvroSerializer(_SCHEMA_PATH)
    producer   = Producer(_PRODUCER_CONFIG)

    sent  = 0
    start = time.monotonic()
    logger.info("Producer started: %s → topic[%s]", filepath, topic)

    with open(filepath) as f:
        batch: list[dict] = []
        for line in f:
            line = line.strip()
            if not line:
                continue
            batch.append(json.loads(line))
            if len(batch) >= chunk_size:
                _publish_batch(producer, topic, batch, serializer)
                sent += len(batch)
                batch = []
                _log_rate(sent, start)
        if batch:
            _publish_batch(producer, topic, batch, serializer)
            sent += len(batch)

    producer.flush()
    elapsed = time.monotonic() - start
    logger.info("Producer done: %d messages in %.1fs (%.0f msg/s)", sent, elapsed, sent / max(elapsed, 0.001))


def _publish_batch(producer: Producer, topic: str, batch: list, serializer: AvroSerializer) -> None:
    for event in batch:
        key = str(event.get("user_id", "")).encode()
        try:
            value = serializer.serialize(event)
        except Exception as e:
            logger.error("Serialization error — skipping event: %s", e)
            continue

        # Retry sur BufferError (queue interne pleine) : on draine puis on réessaie
        while True:
            try:
                producer.produce(topic, key=key, value=value, callback=_delivery_callback)
                break
            except BufferError:
                producer.poll(0.1)
    producer.poll(0)


def _log_rate(sent: int, start: float) -> None:
    elapsed = time.monotonic() - start
    rate    = sent / max(elapsed, 0.001)
    if sent % 100_000 == 0 or rate > 0:
        logger.info("Sent %d messages (%.0f msg/s)", sent, rate)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Kafka producer — ecommerce events")
    parser.add_argument("--input",      default=os.getenv("DATA_PATH", "data/sample/sample_data.jsonl"))
    parser.add_argument("--topic",      default=_TOPIC_RAW)
    parser.add_argument("--chunk-size", type=int, default=int(os.getenv("CHUNK_SIZE", "10000")))
    args = parser.parse_args()

    send_events(args.input, args.topic, args.chunk_size)
