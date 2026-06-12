import io
import json

import fastavro


class AvroSerializer:
    """Serializes Python dicts to Avro bytes (schemaless, no Schema Registry)."""

    def __init__(self, schema_path: str):
        """Args:
            schema_path: Path to the .avsc schema file.
        """
        with open(schema_path) as f:
            self._schema = fastavro.parse_schema(json.load(f))

    def serialize(self, record: dict) -> bytes:
        """Serialize a record dict to Avro bytes.

        Args:
            record: Dictionary matching the schema.

        Returns:
            Avro-encoded bytes (schemaless format).
        """
        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, self._schema, record)
        return buf.getvalue()
