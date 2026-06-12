"""Cart abandonment detector: windowing 30min with TTL dict, alerts on ecommerce-alerts."""
import logging
import os
import signal
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

from confluent_kafka import Consumer, Producer, KafkaError
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, str(Path(__file__).parent))
from modules.avro_deserializer import AvroDeserializer
from modules.avro_serializer import AvroSerializer

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

_BOOTSTRAP          = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
_TOPIC_CLEAN        = os.getenv("KAFKA_TOPIC_CLEAN",  "ecommerce-clean")
_TOPIC_ALERTS       = os.getenv("KAFKA_TOPIC_ALERTS", "ecommerce-alerts")
_GROUP_ID           = os.getenv("KAFKA_CONSUMER_GROUP_DETECTOR", "cart-abandonment")
_SCHEMA_CLEAN_PATH  = os.getenv("SCHEMA_CLEAN_PATH",  "schemas/event_clean.avsc")
_SCHEMA_ALERT_PATH  = os.getenv("SCHEMA_ALERT_PATH",  "schemas/cart_abandonment_alert.avsc")
_ABANDON_THRESHOLD  = float(os.getenv("ABANDONMENT_THRESHOLD", "0.8"))

_EVICTION_INTERVAL_S = 120   # seconds between TTL sweeps
_ALERT_INTERVAL_S    = 60    # seconds between category-level alert checks

_running = True


def _handle_signal(sig, frame) -> None:
    global _running
    _running = False


def detect_abandonment(session_state: dict, event: dict, window_minutes: int = 30) -> bool:
    """Détecte un abandon de panier dans une fenêtre temporelle glissante.

    Args:
        session_state: État courant de la session (clé = user_session).
        event: Événement Kafka nettoyé.
        window_minutes: Durée de la fenêtre en minutes.

    Returns:
        True si un abandon est détecté, False sinon.
    """
    session_id = event.get("user_session")
    if not session_id:
        return False

    now    = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=window_minutes)

    if session_id not in session_state:
        session_state[session_id] = {
            "cart_events":  [],
            "has_purchase": False,
            "category_l1":  event.get("category_l1", "unknown"),
            "user_id":      event.get("user_id", 0),
            "last_seen":    now,
        }

    state = session_state[session_id]
    state["last_seen"] = now

    if event.get("event_type") == "cart":
        state["cart_events"].append(now)
    elif event.get("event_type") == "purchase":
        state["has_purchase"] = True

    # Slide the window: drop events older than cutoff
    state["cart_events"] = [t for t in state["cart_events"] if t >= cutoff]

    return bool(state["cart_events"]) and not state["has_purchase"]


def _evict_expired_sessions(session_state: dict, window_minutes: int) -> None:
    """Remove sessions that haven't been seen for 2× the window duration."""
    cutoff  = datetime.now(timezone.utc) - timedelta(minutes=window_minutes * 2)
    expired = [k for k, v in session_state.items() if v["last_seen"] < cutoff]
    for k in expired:
        del session_state[k]
    if expired:
        logger.debug("Evicted %d expired sessions", len(expired))


def _compute_category_alerts(session_state: dict, threshold: float) -> list[dict]:
    """Aggregate abandonment rates per category and return alerts above threshold.

    Args:
        session_state: Current in-memory session state.
        threshold: Minimum abandonment rate to trigger an alert.

    Returns:
        List of alert dicts ready for Avro serialization.
    """
    cat_counts: dict = defaultdict(lambda: {"cart": 0, "purchase": 0})
    now = datetime.now(timezone.utc)

    for state in session_state.values():
        if not state["cart_events"]:
            continue
        cat = state.get("category_l1", "unknown")
        cat_counts[cat]["cart"] += 1
        if state["has_purchase"]:
            cat_counts[cat]["purchase"] += 1

    alerts = []
    for cat, counts in cat_counts.items():
        total     = counts["cart"]
        purchased = counts["purchase"]
        if total == 0:
            continue
        rate = 1.0 - (purchased / total)
        if rate >= threshold:
            alerts.append({
                "alert_time":       now.isoformat(),
                "user_id":          0,
                "user_session":     f"category_alert_{cat}",
                "category_l1":      cat,
                "abandonment_rate": round(float(rate), 4),
                "cart_count":       total,
                "purchase_count":   purchased,
            })
    return alerts


def run_detector() -> None:
    """Run the cart abandonment detector loop."""
    signal.signal(signal.SIGINT,  _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    deserializer = AvroDeserializer(_SCHEMA_CLEAN_PATH)
    serializer   = AvroSerializer(_SCHEMA_ALERT_PATH)

    consumer = Consumer({
        "bootstrap.servers": _BOOTSTRAP,
        "group.id":          _GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    producer = Producer({
        "bootstrap.servers":  _BOOTSTRAP,
        "enable.idempotence": True,
        "acks": "all",
    })

    consumer.subscribe([_TOPIC_CLEAN])
    session_state: dict = {}
    last_eviction   = time.monotonic()
    last_alert_tick = time.monotonic()

    logger.info("Cart abandonment detector started (threshold=%.0f%%)", _ABANDON_THRESHOLD * 100)

    while _running:
        now_mono = time.monotonic()

        # Periodic TTL sweep
        if now_mono - last_eviction > _EVICTION_INTERVAL_S:
            _evict_expired_sessions(session_state, 30)
            last_eviction = now_mono

        # Periodic category-level alert
        if now_mono - last_alert_tick > _ALERT_INTERVAL_S:
            for alert in _compute_category_alerts(session_state, _ABANDON_THRESHOLD):
                try:
                    producer.produce(_TOPIC_ALERTS, value=serializer.serialize(alert))
                    logger.info(
                        "ALERT: %s abandonment=%.1f%% (cart=%d, purchase=%d)",
                        alert["category_l1"],
                        alert["abandonment_rate"] * 100,
                        alert["cart_count"],
                        alert["purchase_count"],
                    )
                except Exception as e:
                    logger.error("Alert publish error: %s", e)
            producer.poll(0)
            last_alert_tick = now_mono

        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                logger.error("Consumer error: %s", msg.error())
            continue

        try:
            event = deserializer.deserialize(msg.value())
            detect_abandonment(session_state, event)
        except Exception as e:
            logger.error("Event processing error: %s", e)

    producer.flush()
    consumer.close()
    logger.info("Detector stopped — tracked sessions: %d", len(session_state))


if __name__ == "__main__":
    run_detector()
