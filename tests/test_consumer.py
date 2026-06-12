"""Unit tests for consumer.transform_event (pure function, no Kafka dependency)."""
from consumer import transform_event


def _base_event(**overrides) -> dict:
    event = {
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
    event.update(overrides)
    return event


class TestTransformEventValid:
    def test_returns_dict_for_valid_event(self):
        result = transform_event(_base_event())
        assert result is not None
        assert isinstance(result, dict)

    def test_category_code_parsed_correctly(self):
        result = transform_event(_base_event(category_code="electronics.smartphone"))
        assert result["category_l1"] == "electronics"
        assert result["category_l2"] == "smartphone"

    def test_category_code_single_level(self):
        result = transform_event(_base_event(category_code="electronics"))
        assert result["category_l1"] == "electronics"
        assert result["category_l2"] == "unknown"

    def test_category_code_null_becomes_unknown(self):
        result = transform_event(_base_event(category_code=None))
        assert result["category_l1"] == "unknown"
        assert result["category_l2"] == "unknown"

    def test_brand_null_becomes_unknown(self):
        result = transform_event(_base_event(brand=None))
        assert result["brand"] == "unknown"

    def test_timestamp_normalized_to_iso_utc(self):
        result = transform_event(_base_event(event_time="2019-10-01 10:30:00 UTC"))
        assert result["event_time"].endswith("+00:00")

    def test_all_valid_event_types_accepted(self):
        for et in ("view", "cart", "remove_from_cart", "purchase"):
            result = transform_event(_base_event(event_type=et))
            assert result is not None, f"Should accept event_type={et}"

    def test_output_fields_complete(self):
        result = transform_event(_base_event())
        expected = {
            "event_time", "event_type", "product_id", "category_id",
            "category_l1", "category_l2", "brand", "price", "user_id", "user_session",
        }
        assert set(result.keys()) == expected


class TestTransformEventInvalid:
    def test_missing_event_time_returns_none(self):
        assert transform_event(_base_event(event_time=None)) is None

    def test_missing_user_id_returns_none(self):
        assert transform_event(_base_event(user_id=None)) is None

    def test_missing_product_id_returns_none(self):
        assert transform_event(_base_event(product_id=None)) is None

    def test_missing_user_session_returns_none(self):
        assert transform_event(_base_event(user_session=None)) is None

    def test_invalid_event_type_returns_none(self):
        assert transform_event(_base_event(event_type="click")) is None

    def test_empty_event_type_returns_none(self):
        assert transform_event(_base_event(event_type="")) is None

    def test_malformed_timestamp_returns_none(self):
        assert transform_event(_base_event(event_time="not-a-date")) is None
