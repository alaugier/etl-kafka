"""Round-trip tests for AvroSerializer / AvroDeserializer using real .avsc schemas."""
from pathlib import Path

from modules.avro_serializer import AvroSerializer
from modules.avro_deserializer import AvroDeserializer

SCHEMAS_DIR = Path(__file__).parent.parent / "schemas"


def _raw_event(**overrides) -> dict:
    base = {
        "event_time":    "2019-10-01 00:00:00 UTC",
        "event_type":    "view",
        "product_id":    44600062,
        "category_id":   "2103807459595953877",
        "category_code": "electronics.smartphone",
        "brand":         "samsung",
        "price":         135.72,
        "user_id":       541312140,
        "user_session":  "72d76fde-8bb3-4e00-8c23-a032dfed738c",
    }
    base.update(overrides)
    return base


def _clean_event(**overrides) -> dict:
    base = {
        "event_time":   "2019-10-01T00:00:00+00:00",
        "event_type":   "view",
        "product_id":   44600062,
        "category_id":  "2103807459595953877",
        "category_l1":  "electronics",
        "category_l2":  "smartphone",
        "brand":        "samsung",
        "price":        135.72,
        "user_id":      541312140,
        "user_session": "72d76fde-8bb3-4e00-8c23-a032dfed738c",
    }
    base.update(overrides)
    return base


class TestAvroRawRoundTrip:
    def setup_method(self):
        schema = str(SCHEMAS_DIR / "event_raw.avsc")
        self.ser = AvroSerializer(schema)
        self.deser = AvroDeserializer(schema)

    def test_serialize_returns_bytes(self):
        result = self.ser.serialize(_raw_event())
        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_round_trip_preserves_all_fields(self):
        record = _raw_event()
        assert self.deser.deserialize(self.ser.serialize(record)) == record

    def test_round_trip_with_null_brand(self):
        record = _raw_event(brand=None)
        assert self.deser.deserialize(self.ser.serialize(record))["brand"] is None

    def test_round_trip_with_null_category_code(self):
        record = _raw_event(category_code=None)
        assert self.deser.deserialize(self.ser.serialize(record))["category_code"] is None

    def test_category_id_preserved_as_string(self):
        large_id = "2103807459595387724"
        record = _raw_event(category_id=large_id)
        result = self.deser.deserialize(self.ser.serialize(record))
        assert result["category_id"] == large_id


class TestAvroCleanRoundTrip:
    def setup_method(self):
        schema = str(SCHEMAS_DIR / "event_clean.avsc")
        self.ser = AvroSerializer(schema)
        self.deser = AvroDeserializer(schema)

    def test_round_trip_preserves_all_fields(self):
        record = _clean_event()
        assert self.deser.deserialize(self.ser.serialize(record)) == record

    def test_round_trip_all_event_types(self):
        for et in ("view", "cart", "remove_from_cart", "purchase"):
            record = _clean_event(event_type=et)
            result = self.deser.deserialize(self.ser.serialize(record))
            assert result["event_type"] == et
