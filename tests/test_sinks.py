"""Unit tests for PostgresSink and ElasticsearchSink (infrastructure mocked)."""
from unittest.mock import MagicMock, patch


def _clean_event(**overrides) -> dict:
    base = {
        "event_time":   "2019-10-01T00:00:00+00:00",
        "event_type":   "view",
        "product_id":   44600062,
        "category_id":  "2103807",
        "category_l1":  "electronics",
        "category_l2":  "smartphone",
        "brand":        "samsung",
        "price":        135.72,
        "user_id":      541312140,
        "user_session": "session-abc",
    }
    base.update(overrides)
    return base


class TestPostgresSink:
    @patch("modules.postgres_sink.psycopg2.connect")
    def test_connect_creates_schema(self, mock_connect):
        mock_conn   = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.cursor.return_value.__exit__  = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        from modules.postgres_sink import PostgresSink
        sink = PostgresSink(host="h", port=5432, dbname="db", user="u", password="p")
        sink.connect()

        mock_cursor.execute.assert_called_once()
        mock_conn.commit.assert_called()

    @patch("modules.postgres_sink.execute_values")
    @patch("modules.postgres_sink.psycopg2.connect")
    def test_write_events_upserts_rows(self, mock_connect, mock_execute_values):
        mock_conn   = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.cursor.return_value.__exit__  = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        from modules.postgres_sink import PostgresSink
        sink = PostgresSink()
        sink._conn = mock_conn

        events = [_clean_event(), _clean_event(event_type="cart")]
        result = sink.write_events(events)

        assert result == 2
        mock_execute_values.assert_called_once()
        mock_conn.commit.assert_called()

    @patch("modules.postgres_sink.psycopg2.connect")
    def test_write_events_empty_returns_zero(self, mock_connect):
        from modules.postgres_sink import PostgresSink
        sink = PostgresSink()
        sink._conn = MagicMock()
        assert sink.write_events([]) == 0

    @patch("modules.postgres_sink.execute_values")
    @patch("modules.postgres_sink.psycopg2.connect")
    def test_update_funnel_aggregates_by_category_date(self, mock_connect, mock_execute_values):
        from modules.postgres_sink import PostgresSink
        mock_conn   = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.cursor.return_value.__exit__  = MagicMock(return_value=False)

        sink = PostgresSink()
        sink._conn = mock_conn

        events = [
            _clean_event(event_type="view"),
            _clean_event(event_type="cart"),
            _clean_event(event_type="purchase"),
        ]
        sink.update_funnel(events)
        mock_execute_values.assert_called_once()


class TestElasticsearchSink:
    @patch("modules.elasticsearch_sink.Elasticsearch")
    def test_setup_index_creates_if_missing(self, MockES):
        mock_client = MagicMock()
        mock_client.indices.exists.return_value = False
        MockES.return_value = mock_client

        from modules.elasticsearch_sink import ElasticsearchSink
        sink = ElasticsearchSink(host="localhost", port=9200, index="test-idx")
        sink.setup_index()

        mock_client.indices.create.assert_called_once()

    @patch("modules.elasticsearch_sink.Elasticsearch")
    def test_setup_index_skips_if_exists(self, MockES):
        mock_client = MagicMock()
        mock_client.indices.exists.return_value = True
        MockES.return_value = mock_client

        from modules.elasticsearch_sink import ElasticsearchSink
        sink = ElasticsearchSink()
        sink.setup_index()

        mock_client.indices.create.assert_not_called()

    @patch("modules.elasticsearch_sink.bulk")
    @patch("modules.elasticsearch_sink.Elasticsearch")
    def test_write_events_calls_bulk(self, MockES, mock_bulk):
        mock_bulk.return_value = (2, [])
        mock_client = MagicMock()
        MockES.return_value = mock_client

        from modules.elasticsearch_sink import ElasticsearchSink
        sink = ElasticsearchSink()
        events = [_clean_event(), _clean_event(event_type="cart")]
        result = sink.write_events(events)

        assert result == 2
        mock_bulk.assert_called_once()

    @patch("modules.elasticsearch_sink.Elasticsearch")
    def test_write_events_empty_returns_zero(self, MockES):
        from modules.elasticsearch_sink import ElasticsearchSink
        sink = ElasticsearchSink()
        assert sink.write_events([]) == 0
