"""Kafka consumer: reads ecommerce-raw, transforms events, publishes to ecommerce-clean."""
import logging
import os
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, str(Path(__file__).parent))
from modules.avro_deserializer import AvroDeserializer  # noqa: E402
from modules.avro_serializer import AvroSerializer  # noqa: E402

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

_BOOTSTRAP        = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
_TOPIC_RAW        = os.getenv("KAFKA_TOPIC_RAW",   "ecommerce-raw")
_TOPIC_CLEAN      = os.getenv("KAFKA_TOPIC_CLEAN",  "ecommerce-clean")
_GROUP_ID         = os.getenv("KAFKA_CONSUMER_GROUP_CONSUMER", "etl-consumer")
_SCHEMA_RAW_PATH  = os.getenv("SCHEMA_RAW_PATH",   "schemas/event_raw.avsc")
_SCHEMA_CLEAN_PATH = os.getenv("SCHEMA_CLEAN_PATH", "schemas/event_clean.avsc")

_VALID_EVENT_TYPES = {"view", "cart", "remove_from_cart", "purchase"}
_REQUIRED_FIELDS   = {"event_time", "event_type", "product_id", "user_id", "user_session"}

_running = True


def _handle_signal(sig, frame) -> None:
    global _running
    logger.info("Shutdown signal received")
    _running = False


def transform_event(raw: dict) -> dict | None:
    """Valide et nettoie un événement brut Kafka.

    Opérations :
    - Validation des champs obligatoires
    - Nettoyage des valeurs nulles (brand → 'unknown')
    - Parsing category_code → category_l1, category_l2
    - Normalisation event_time en ISO 8601 UTC

    Args:
        raw: Dictionnaire représentant un événement brut.

    Returns:
        Événement nettoyé, ou None si l'événement est invalide.
    """
    # Validate required non-null fields
    missing = {f for f in _REQUIRED_FIELDS if not raw.get(f)}
    if missing:
        logger.debug("Dropping event — missing: %s", missing)
        return None

    if raw.get("event_type") not in _VALID_EVENT_TYPES:
        return None

    # Normalize timestamp to ISO 8601 UTC
    try:
        raw_time = str(raw["event_time"]).replace(" UTC", "+00:00")
        dt = datetime.fromisoformat(raw_time)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        event_time_iso = dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return None

    # Parse category hierarchy: "electronics.smartphone" → l1=electronics, l2=smartphone
    category_code = raw.get("category_code") or ""
    parts = [p for p in category_code.split(".") if p] if category_code else []
    category_l1 = parts[0] if len(parts) > 0 else "unknown"
    category_l2 = parts[1] if len(parts) > 1 else "unknown"

    return {
        "event_time":   event_time_iso,
        "event_type":   str(raw["event_type"]),
        "product_id":   int(raw["product_id"]),
        "category_id":  str(raw.get("category_id") or "0"),
        "category_l1":  category_l1,
        "category_l2":  category_l2,
        "brand":        str(raw.get("brand") or "unknown"),
        "price":        float(raw.get("price") or 0.0),
        "user_id":      int(raw["user_id"]),
        "user_session": str(raw["user_session"]),
    }


def _ensure_topic(topic: str, num_partitions: int = 6, retention_ms: int = 172_800_000) -> None:
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


def run_consumer() -> None:
    """Run the ETL consumer: ecommerce-raw → transform → ecommerce-clean."""
    signal.signal(signal.SIGINT,  _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    _ensure_topic(_TOPIC_CLEAN, num_partitions=6, retention_ms=172_800_000)  # 48h

    deserializer = AvroDeserializer(_SCHEMA_RAW_PATH)
    serializer   = AvroSerializer(_SCHEMA_CLEAN_PATH)

    consumer = Consumer({
        "bootstrap.servers": _BOOTSTRAP,
        "group.id":          _GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    producer = Producer({
        "bootstrap.servers":            _BOOTSTRAP,
        "enable.idempotence":           True,
        "acks":                         "all",
        "linger.ms":                    5,
        "queue.buffering.max.messages": 500_000,
    })

    consumer.subscribe([_TOPIC_RAW])
    logger.info("Consumer running: %s → %s  (group=%s)", _TOPIC_RAW, _TOPIC_CLEAN, _GROUP_ID)

    processed = dropped = 0
    try:
        while _running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("Consumer error: %s", msg.error())
                continue

            try:
                raw   = deserializer.deserialize(msg.value())
                clean = transform_event(raw)
                if clean is None:
                    dropped += 1
                    # Commit offset even for dropped events to avoid re-processing
                    consumer.commit(message=msg, asynchronous=False)
                    continue

                value = serializer.serialize(clean)
                key   = str(clean["user_id"]).encode()
                while True:
                    try:
                        producer.produce(_TOPIC_CLEAN, key=key, value=value)
                        break
                    except BufferError:
                        producer.poll(0.1)
                producer.poll(0)
                # Offset committed only after successful publish to downstream topic
                consumer.commit(message=msg, asynchronous=False)
                processed += 1
                if processed % 10_000 == 0:
                    logger.info("Processed %d | Dropped %d", processed, dropped)

            except Exception as e:
                logger.error("Processing error: %s", e, exc_info=True)
    finally:
        producer.flush()
        consumer.close()
        logger.info("Consumer stopped — processed=%d dropped=%d", processed, dropped)


if __name__ == "__main__":
    run_consumer()
