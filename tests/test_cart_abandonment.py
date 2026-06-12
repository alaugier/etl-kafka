"""Unit tests for cart_abandonment_detector.detect_abandonment (pure logic)."""
from datetime import datetime, timezone, timedelta

from cart_abandonment_detector import detect_abandonment, _evict_expired_sessions, _compute_category_alerts


def _event(event_type: str, session: str = "s1", user_id: int = 1, category: str = "electronics") -> dict:
    return {
        "event_type":   event_type,
        "user_session": session,
        "user_id":      user_id,
        "category_l1":  category,
    }


class TestDetectAbandonment:
    def test_cart_without_purchase_is_abandonment(self):
        state = {}
        detect_abandonment(state, _event("cart"))
        assert detect_abandonment(state, _event("view")) is True

    def test_cart_followed_by_purchase_is_not_abandonment(self):
        state = {}
        detect_abandonment(state, _event("cart"))
        detect_abandonment(state, _event("purchase"))
        assert detect_abandonment(state, _event("view")) is False

    def test_view_only_is_not_abandonment(self):
        state = {}
        assert detect_abandonment(state, _event("view")) is False

    def test_missing_session_returns_false(self):
        state = {}
        assert detect_abandonment(state, {"event_type": "cart"}) is False

    def test_expired_cart_events_not_counted(self):
        state = {}
        detect_abandonment(state, _event("cart"), window_minutes=30)
        # Manually backdate cart events past the window
        state["s1"]["cart_events"] = [
            datetime.now(timezone.utc) - timedelta(minutes=31)
        ]
        assert detect_abandonment(state, _event("view"), window_minutes=30) is False

    def test_multiple_sessions_tracked_independently(self):
        state = {}
        detect_abandonment(state, _event("cart", session="s1"))
        detect_abandonment(state, _event("cart",     session="s2"))
        detect_abandonment(state, _event("purchase", session="s2"))
        assert detect_abandonment(state, _event("view", session="s1")) is True
        assert detect_abandonment(state, _event("view", session="s2")) is False


class TestEvictExpiredSessions:
    def test_evicts_old_sessions(self):
        old_time = datetime.now(timezone.utc) - timedelta(hours=2)
        state = {
            "stale": {"last_seen": old_time, "cart_events": [], "has_purchase": False, "category_l1": "x", "user_id": 1},
            "fresh": {"last_seen": datetime.now(timezone.utc), "cart_events": [], "has_purchase": False, "category_l1": "y", "user_id": 2},
        }
        _evict_expired_sessions(state, window_minutes=30)
        assert "stale" not in state
        assert "fresh" in state


class TestComputeCategoryAlerts:
    def test_alert_emitted_above_threshold(self):
        now = datetime.now(timezone.utc)
        state = {
            f"s{i}": {
                "cart_events":  [now],
                "has_purchase": False,
                "category_l1":  "electronics",
                "user_id":      i,
                "last_seen":    now,
            }
            for i in range(10)
        }
        alerts = _compute_category_alerts(state, threshold=0.8)
        assert len(alerts) == 1
        assert alerts[0]["category_l1"] == "electronics"
        assert alerts[0]["abandonment_rate"] == 1.0

    def test_no_alert_below_threshold(self):
        now = datetime.now(timezone.utc)
        state = {
            "s1": {"cart_events": [now], "has_purchase": True,  "category_l1": "fashion", "user_id": 1, "last_seen": now},
            "s2": {"cart_events": [now], "has_purchase": True,  "category_l1": "fashion", "user_id": 2, "last_seen": now},
            "s3": {"cart_events": [now], "has_purchase": False, "category_l1": "fashion", "user_id": 3, "last_seen": now},
        }
        alerts = _compute_category_alerts(state, threshold=0.8)
        assert len(alerts) == 0

    def test_empty_state_returns_no_alerts(self):
        assert _compute_category_alerts({}, threshold=0.5) == []

    def test_session_with_empty_cart_events_ignored(self):
        now = datetime.now(timezone.utc)
        state = {
            "s1": {"cart_events": [], "has_purchase": False, "category_l1": "tools", "user_id": 1, "last_seen": now},
        }
        alerts = _compute_category_alerts(state, threshold=0.5)
        assert len(alerts) == 0
