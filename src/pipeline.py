"""Pipeline ETL: consumes ecommerce-clean, writes to PostgreSQL + Elasticsearch."""
import logging
import os
import signal
import sys
import time
from pathlib import Path
from typing import Optional

from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, str(Path(__file__).parent))
from modules.avro_deserializer import AvroDeserializer
from modules.postgres_sink import PostgresSink
from modules.elasticsearch_sink import ElasticsearchSink

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

_BOOTSTRAP         = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
_TOPIC_CLEAN       = os.getenv("KAFKA_TOPIC_CLEAN", "ecommerce-clean")
_GROUP_ID          = os.getenv("KAFKA_CONSUMER_GROUP_PIPELINE", "etl-pipeline")
_SCHEMA_CLEAN_PATH = os.getenv("SCHEMA_CLEAN_PATH", "schemas/event_clean.avsc")
_BATCH_SIZE        = int(os.getenv("PIPELINE_BATCH_SIZE", "500"))

_running = True


def _handle_signal(sig, frame) -> None:
    global _running
    _running = False
    logger.info("Shutdown signal received")


class ETLPipeline:
    """Orchestrates reading from Kafka and writing to dual sinks (PostgreSQL + Elasticsearch).

    Each component (consumer, postgres_sink, es_sink) is injectable for testability.
    """

    def __init__(
        self,
        consumer: Optional[Consumer] = None,
        postgres_sink: Optional[PostgresSink] = None,
        es_sink: Optional[ElasticsearchSink] = None,
        deserializer: Optional[AvroDeserializer] = None,
    ):
        self._consumer = consumer or Consumer({
            "bootstrap.servers": _BOOTSTRAP,
            "group.id":          _GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        self._pg           = postgres_sink or PostgresSink()
        self._es           = es_sink       or ElasticsearchSink()
        self._deserializer = deserializer  or AvroDeserializer(_SCHEMA_CLEAN_PATH)

    def setup(self) -> None:
        """Open all sink connections and subscribe to the clean topic."""
        self._pg.connect()
        self._es.setup_index()
        self._consumer.subscribe([_TOPIC_CLEAN])
        logger.info("Pipeline ready — consuming %s", _TOPIC_CLEAN)

    def close(self) -> None:
        self._consumer.close()
        self._pg.close()

    def run(self, batch_size: int = _BATCH_SIZE) -> None:
        """Run the pipeline loop until a shutdown signal.

        Offsets are committed only after both sinks have confirmed the write,
        guaranteeing at-least-once delivery.

        Args:
            batch_size: Events to buffer before flushing to sinks.
        """
        batch:   list[dict] = []
        offsets: list       = []
        processed = 0
        start     = time.monotonic()

        while _running:
            msg = self._consumer.poll(timeout=1.0)

            if msg is None:
                # Flush whatever is buffered on idle
                if batch:
                    self._flush(batch, offsets)
                    processed += len(batch)
                    batch, offsets = [], []
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("Kafka error: %s", msg.error())
                continue

            try:
                event = self._deserializer.deserialize(msg.value())
                batch.append(event)
                offsets.append(msg)
            except Exception as e:
                logger.error("Deserialization error: %s", e)
                continue

            if len(batch) >= batch_size:
                self._flush(batch, offsets)
                processed += len(batch)
                batch, offsets = [], []
                elapsed = time.monotonic() - start
                logger.info("Pipeline: %d events (%.0f msg/s)", processed, processed / max(elapsed, 0.001))

        if batch:
            self._flush(batch, offsets)

    def _flush(self, batch: list[dict], offsets: list) -> None:
        """Write batch to both sinks, then commit offsets atomically."""
        try:
            self._pg.write_events(batch)
            self._pg.update_funnel(batch)
            self._es.write_events(batch)
            if offsets:
                # Commit the highest offset in the batch
                self._consumer.commit(message=offsets[-1], asynchronous=False)
        except Exception as e:
            logger.error("Flush error — batch not committed: %s", e, exc_info=True)


def run_pipeline() -> None:
    """Entry point: build pipeline with defaults and run."""
    signal.signal(signal.SIGINT,  _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    pipeline = ETLPipeline()
    pipeline.setup()
    try:
        pipeline.run()
    finally:
        pipeline.close()
        logger.info("Pipeline stopped")


if __name__ == "__main__":
    run_pipeline()
