import logging
import os
from collections import defaultdict
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS ecommerce_events (
    event_time      TIMESTAMPTZ     NOT NULL,
    event_type      VARCHAR(20)     NOT NULL,
    product_id      BIGINT          NOT NULL,
    category_id     VARCHAR(30)     NOT NULL,
    category_l1     VARCHAR(100),
    category_l2     VARCHAR(100),
    brand           VARCHAR(100),
    price           NUMERIC(12, 4),
    user_id         BIGINT          NOT NULL,
    user_session    VARCHAR(100)    NOT NULL,
    created_at      TIMESTAMPTZ     DEFAULT NOW(),
    PRIMARY KEY (user_session, event_time, product_id, event_type)
);

CREATE TABLE IF NOT EXISTS category_funnel (
    category_l1     VARCHAR(100)    NOT NULL,
    event_date      DATE            NOT NULL,
    view_count      BIGINT          DEFAULT 0,
    cart_count      BIGINT          DEFAULT 0,
    purchase_count  BIGINT          DEFAULT 0,
    PRIMARY KEY (category_l1, event_date)
);
"""


class PostgresSink:
    """Writes cleaned events to PostgreSQL with upsert and duplicate handling."""

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        dbname: str | None = None,
        user: str | None = None,
        password: str | None = None,
    ):
        self._conn_params = {
            "host":     host     or os.getenv("POSTGRES_HOST", "localhost"),
            "port":     port     or int(os.getenv("POSTGRES_PORT", "5432")),
            "dbname":   dbname   or os.getenv("POSTGRES_DB", "tpdb"),
            "user":     user     or os.getenv("POSTGRES_USER", "tpuser"),
            "password": password or os.getenv("POSTGRES_PASSWORD", "tppassword"),
        }
        self._conn: psycopg2.extensions.connection | None = None

    def connect(self) -> None:
        """Open connection and initialize schema."""
        self._conn = psycopg2.connect(**self._conn_params)
        with self._conn.cursor() as cur:
            cur.execute(_SCHEMA_SQL)
        self._conn.commit()
        logger.info("PostgreSQL connected — schema ready")

    def close(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()

    def write_events(self, events: list[dict]) -> int:
        """Upsert a batch of cleaned events into PostgreSQL.

        Args:
            events: List of cleaned event dicts.

        Returns:
            Number of rows inserted (excluding conflicts).
        """
        if not events:
            return 0
        rows = [
            (
                e["event_time"], e["event_type"], e["product_id"],
                e["category_id"], e.get("category_l1"), e.get("category_l2"),
                e.get("brand"), e.get("price"), e["user_id"], e["user_session"],
            )
            for e in events
        ]
        sql = """
            INSERT INTO ecommerce_events
                (event_time, event_type, product_id, category_id,
                 category_l1, category_l2, brand, price, user_id, user_session)
            VALUES %s
            ON CONFLICT (user_session, event_time, product_id, event_type) DO NOTHING
        """
        try:
            with self._conn.cursor() as cur:
                execute_values(cur, sql, rows)
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        logger.debug("Upserted %d events to PostgreSQL", len(rows))
        return len(rows)

    def update_funnel(self, events: list[dict]) -> None:
        """Increment category funnel aggregates from a batch of events.

        Args:
            events: List of cleaned event dicts.
        """
        counts: dict = defaultdict(lambda: {"view": 0, "cart": 0, "purchase": 0})
        for e in events:
            cat = e.get("category_l1") or "unknown"
            try:
                date = datetime.fromisoformat(e["event_time"]).date()
            except Exception:
                continue
            key = (cat, date)
            et = e.get("event_type", "")
            if et == "view":
                counts[key]["view"] += 1
            elif et == "cart":
                counts[key]["cart"] += 1
            elif et == "purchase":
                counts[key]["purchase"] += 1

        if not counts:
            return
        rows = [(cat, date, v["view"], v["cart"], v["purchase"]) for (cat, date), v in counts.items()]
        sql = """
            INSERT INTO category_funnel (category_l1, event_date, view_count, cart_count, purchase_count)
            VALUES %s
            ON CONFLICT (category_l1, event_date) DO UPDATE SET
                view_count     = category_funnel.view_count     + EXCLUDED.view_count,
                cart_count     = category_funnel.cart_count     + EXCLUDED.cart_count,
                purchase_count = category_funnel.purchase_count + EXCLUDED.purchase_count
        """
        try:
            with self._conn.cursor() as cur:
                execute_values(cur, sql, rows)
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        logger.debug("Updated funnel for %d category/date pairs", len(rows))
