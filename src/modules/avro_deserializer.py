import io
import json
from pathlib import Path

import fastavro


class AvroDeserializer:
    """Deserializes Avro bytes to Python dicts (schemaless, no Schema Registry)."""

    def __init__(self, schema_path: str):
        """Args:
            schema_path: Path to the .avsc schema file.
        """
        with open(schema_path) as f:
            self._schema = fastavro.parse_schema(json.load(f))

    def deserialize(self, data: bytes) -> dict:
        """Deserialize Avro bytes to a record dict.

        Args:
            data: Avro-encoded bytes (schemaless format).

        Returns:
            Deserialized dictionary.
        """
        return fastavro.schemaless_reader(io.BytesIO(data), self._schema)
