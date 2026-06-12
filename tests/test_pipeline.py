"""Unit tests for pipeline.ETLPipeline (all sinks and Kafka mocked)."""
from unittest.mock import MagicMock

from pipeline import ETLPipeline


def _make_pipeline() -> tuple[ETLPipeline, MagicMock, MagicMock, MagicMock, MagicMock]:
    mock_consumer     = MagicMock()
    mock_pg           = MagicMock()
    mock_es           = MagicMock()
    mock_deserializer = MagicMock()
    pipeline = ETLPipeline(
        consumer=mock_consumer,
        postgres_sink=mock_pg,
        es_sink=mock_es,
        deserializer=mock_deserializer,
    )
    return pipeline, mock_consumer, mock_pg, mock_es, mock_deserializer


def _clean_event(**overrides) -> dict:
    base = {
        "event_time":   "2019-10-01T00:00:00+00:00",
        "event_type":   "view",
        "product_id":   1,
        "category_id":  "123",
        "category_l1":  "electronics",
        "category_l2":  "smartphone",
        "brand":        "samsung",
        "price":        99.9,
        "user_id":      1,
        "user_session": "sess-1",
    }
    base.update(overrides)
    return base


class TestETLPipelineSetup:
    def test_setup_opens_pg_and_es(self):
        pipeline, consumer, pg, es, _ = _make_pipeline()
        pipeline.setup()
        pg.connect.assert_called_once()
        es.setup_index.assert_called_once()
        consumer.subscribe.assert_called_once()

    def test_close_shuts_down_consumer_and_pg(self):
        pipeline, consumer, pg, es, _ = _make_pipeline()
        pipeline.close()
        consumer.close.assert_called_once()
        pg.close.assert_called_once()


class TestETLPipelineFlush:
    def test_flush_writes_to_both_sinks_and_commits(self):
        pipeline, consumer, pg, es, deser = _make_pipeline()
        events  = [_clean_event()]
        offsets = [MagicMock()]
        pipeline._flush(events, offsets)
        pg.write_events.assert_called_once_with(events)
        pg.update_funnel.assert_called_once_with(events)
        es.write_events.assert_called_once_with(events)
        consumer.commit.assert_called_once_with(message=offsets[-1], asynchronous=False)

    def test_flush_empty_batch_skips_commit(self):
        pipeline, consumer, pg, es, _ = _make_pipeline()
        pipeline._flush([], [])
        consumer.commit.assert_not_called()

    def test_flush_error_does_not_raise(self):
        pipeline, consumer, pg, es, _ = _make_pipeline()
        pg.write_events.side_effect = Exception("DB down")
        # Should log error but not propagate
        pipeline._flush([_clean_event()], [MagicMock()])


class TestETLPipelineRun:
    def test_run_processes_messages_in_batches(self):
        import pipeline as pipeline_module
        pipeline_module._running = True

        p, consumer, pg, es, deser = _make_pipeline()
        event = _clean_event()
        deser.deserialize.return_value = event

        msg1 = MagicMock()
        msg1.error.return_value = None
        msg1.value.return_value = b"avro"

        # Sequence: 1 message then None (idle flush) then stop loop
        call_count = [0]
        def poll_side_effect(timeout):
            call_count[0] += 1
            if call_count[0] == 1:
                return msg1
            pipeline_module._running = False
            return None

        consumer.poll.side_effect = poll_side_effect

        p.run(batch_size=100)

        # Idle flush should have been triggered
        pg.write_events.assert_called()
