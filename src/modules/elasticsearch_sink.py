import logging
import os

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

logger = logging.getLogger(__name__)

_INDEX_MAPPINGS = {
    "properties": {
        "event_time":   {"type": "date"},
        "event_type":   {"type": "keyword"},
        "product_id":   {"type": "long"},
        "category_id":  {"type": "keyword"},
        "category_l1":  {"type": "keyword"},
        "category_l2":  {"type": "keyword"},
        "brand":        {"type": "keyword"},
        "price":        {"type": "double"},
        "user_id":      {"type": "long"},
        "user_session": {"type": "keyword"},
    }
}

# number_of_replicas=0 évite le statut "yellow" sur un cluster single-node (pas de nœud
# supplémentaire disponible pour héberger les replicas)
_INDEX_SETTINGS = {"number_of_shards": 1, "number_of_replicas": 0}


class ElasticsearchSink:
    """Writes cleaned events to Elasticsearch using bulk indexing."""

    def __init__(self, host: str | None = None, port: int | None = None, index: str | None = None):
        host  = host  or os.getenv("ES_HOST", "localhost")
        port  = port  or int(os.getenv("ES_PORT", "9200"))
        self._index  = index or os.getenv("ES_INDEX", "ecommerce-events")
        self._client = Elasticsearch(f"http://{host}:{port}")

    def setup_index(self) -> None:
        """Create the index with explicit mapping if it does not already exist."""
        if not self._client.indices.exists(index=self._index):
            self._client.indices.create(
                index=self._index,
                mappings=_INDEX_MAPPINGS,
                settings=_INDEX_SETTINGS,
            )
            logger.info("Created Elasticsearch index: %s", self._index)

    def write_events(self, events: list[dict]) -> int:
        """Bulk index events into Elasticsearch (idempotent via _id).

        Args:
            events: List of cleaned event dicts.

        Returns:
            Number of documents successfully indexed.
        """
        if not events:
            return 0
        actions = [
            {
                "_index": self._index,
                "_id":    f"{e['user_session']}_{e['event_time']}_{e['product_id']}_{e['event_type']}",
                "_source": e,
            }
            for e in events
        ]
        success, errors = bulk(self._client, actions, raise_on_error=False)
        if errors:
            logger.warning("Elasticsearch bulk errors (%d): %s", len(errors), errors[:2])
        logger.debug("Indexed %d events to Elasticsearch", success)
        return success
