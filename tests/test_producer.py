"""Unit tests for producer.send_events (Kafka mocked)."""
import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest


class TestSendEvents:
    def _make_jsonl(self, rows: list[dict], path: Path) -> None:
        with open(path, "w") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")

    def _valid_event(self, user_id: int = 1) -> dict:
        return {
            "event_time":    "2019-10-01 00:00:00 UTC",
            "event_type":    "view",
            "product_id":    44600062,
            "category_id":   "2103807459595953877",
            "category_code": "electronics.smartphone",
            "brand":         "samsung",
            "price":         135.72,
            "user_id":       user_id,
            "user_session":  f"session-{user_id}",
        }

    @patch("producer.AvroSerializer")
    @patch("producer.Producer")
    @patch("producer._ensure_topic")
    def test_send_events_publishes_all_messages(self, mock_ensure, MockProducer, MockSerializer):
        mock_producer   = MagicMock()
        MockProducer.return_value = mock_producer
        mock_serializer = MagicMock()
        mock_serializer.serialize.return_value = b"avro-bytes"
        MockSerializer.return_value = mock_serializer

        events = [self._valid_event(i) for i in range(5)]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            for e in events:
                f.write(json.dumps(e) + "\n")
            tmp_path = f.name

        try:
            from producer import send_events
            send_events(tmp_path, "ecommerce-raw", chunk_size=3)
        finally:
            os.unlink(tmp_path)

        assert mock_producer.produce.call_count == 5
        mock_producer.flush.assert_called_once()

    @patch("producer._ensure_topic")
    def test_send_events_raises_file_not_found(self, mock_ensure):
        from producer import send_events
        with pytest.raises(FileNotFoundError):
            send_events("/nonexistent/path.jsonl", "ecommerce-raw")

    @patch("producer.AvroSerializer")
    @patch("producer.Producer")
    @patch("producer._ensure_topic")
    def test_partitioned_by_user_id(self, mock_ensure, MockProducer, MockSerializer):
        mock_producer = MagicMock()
        MockProducer.return_value = mock_producer
        mock_serializer = MagicMock()
        mock_serializer.serialize.return_value = b"bytes"
        MockSerializer.return_value = mock_serializer

        event = self._valid_event(user_id=42)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            f.write(json.dumps(event) + "\n")
            tmp_path = f.name

        try:
            from producer import send_events
            send_events(tmp_path, "ecommerce-raw")
        finally:
            os.unlink(tmp_path)

        # Key must be the user_id encoded as bytes
        _, kwargs = mock_producer.produce.call_args
        assert kwargs["key"] == b"42"


class TestDeliveryCallback:
    def test_no_error_is_silent(self):
        from producer import _delivery_callback
        msg = MagicMock()
        _delivery_callback(None, msg)  # should not raise

    def test_error_is_logged(self, caplog):
        import logging
        from producer import _delivery_callback
        msg = MagicMock()
        msg.topic.return_value = "ecommerce-raw"
        with caplog.at_level(logging.ERROR):
            _delivery_callback("some-error", msg)
        assert "Delivery failed" in caplog.text
