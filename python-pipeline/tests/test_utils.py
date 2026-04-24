import os
from typing import List
import sys

# Add parent directory to sys.path to allow importing from sibling files
# This is useful if running tests from the tests directory or from root.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from models import Event

class DemoTestUtils:
    @staticmethod
    def create_event(session_id: str, event_type: str, sequence: int, total_fragments: int, timestamp_millis: int) -> Event:
        event = Event(
            session_id=session_id,
            event_type=event_type,
            sequence=sequence,
            total_fragments=total_fragments,
            timestamp=timestamp_millis
        )
        return event

    @staticmethod
    def create_single_order_scenario(session_id: str, start_millis: int) -> List[Event]:
        return [
            DemoTestUtils.create_event(session_id, "fragment", 1, 3, start_millis),
            DemoTestUtils.create_event(session_id, "fragment", 2, 3, start_millis + 1000),
            DemoTestUtils.create_event(session_id, "fragment", 3, 3, start_millis + 5000)
        ]

    @staticmethod
    def create_interleaved_orders_scenario(session_id_a: str, session_id_b: str, start_millis: int) -> List[Event]:
        return [
            DemoTestUtils.create_event(session_id_a, "fragment", 1, 2, start_millis),
            DemoTestUtils.create_event(session_id_b, "fragment", 1, 2, start_millis + 500),
            DemoTestUtils.create_event(session_id_a, "fragment", 2, 2, start_millis + 1000),
            DemoTestUtils.create_event(session_id_b, "fragment", 2, 2, start_millis + 2000)
        ]

    @staticmethod
    def create_incomplete_order_scenario(session_id: str, start_millis: int) -> List[Event]:
        return [
            DemoTestUtils.create_event(session_id, "fragment", 1, 3, start_millis),
            DemoTestUtils.create_event(session_id, "fragment", 2, 3, start_millis + 1000)
        ]

    @staticmethod
    def check_state_file_exists(base_dir: str, session_id: str) -> bool:
        # Java code used key directly as filename if sanitized.
        # In Python FileStateStore we sanitize key to replace special chars with '_'.
        # Let's make sure we check the right path.
        # We can import FileStateStore to get the path but that would be cyclic or complex.
        # Let's just do the same sanitization here.
        import re
        sanitized_key = re.sub(r'[^a-zA-Z0-9.-]', '_', session_id)
        path = os.path.join(base_dir, sanitized_key + ".json")
        return os.path.exists(path)
