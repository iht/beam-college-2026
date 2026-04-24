import json
from dataclasses import dataclass, field
from typing import List, Dict, Optional

@dataclass
class Event:
    session_id: str
    timestamp: int
    event_type: str
    data: Dict[str, str] = field(default_factory=dict)
    sequence: Optional[int] = None
    total_fragments: Optional[int] = None

    def __lt__(self, other):
        return self.timestamp < other.timestamp

    def to_dict(self):
        return {
            "session_id": self.session_id,
            "timestamp": self.timestamp,
            "event_type": self.event_type,
            "data": self.data,
            "sequence": self.sequence,
            "total_fragments": self.total_fragments
        }

    @classmethod
    def from_dict(cls, d):
        return cls(
            session_id=d.get("session_id"),
            timestamp=d.get("timestamp"),
            event_type=d.get("event_type"),
            data=d.get("data", {}),
            sequence=d.get("sequence"),
            total_fragments=d.get("total_fragments")
        )

class Order:
    def __init__(self, session_id: Optional[str] = None):
        self.session_id = session_id
        self.events: List[Event] = []
        self.items: Dict[str, int] = {}
        self.payment_method: Optional[str] = None
        self.status: str = "NEW"

    def add_event(self, event: Event):
        if self.session_id is None:
            self.session_id = event.session_id

        # Keep events sorted by timestamp
        # In Python, we can just append and sort, or use bisect
        self.events.append(event)
        self.events.sort(key=lambda e: e.timestamp)

        # Recalculate state if inserted in middle or just to be safe like Java
        # Java code had an optimization, but also a recalculate path.
        # Let's keep it simple and recalculate like Java did for out-of-order.
        self.recalculate()

    def recalculate(self):
        self.items = {}
        self.payment_method = None
        self.status = "NEW"

        for event in self.events:
            self._apply(event)

    def _apply(self, event: Event):
        event_type = event.event_type
        data = event.data

        if event_type == "ADD_TO_CART":
            item_id = data.get("item_id")
            quantity = data.get("quantity", 0)
            if isinstance(quantity, str):
                try:
                    quantity = int(quantity)
                except ValueError:
                    quantity = 0
            elif isinstance(quantity, (int, float)):
                quantity = int(quantity)
            
            if item_id:
                self.items[item_id] = self.items.get(item_id, 0) + quantity
        elif event_type == "REMOVE_FROM_CART":
            item_id = data.get("item_id")
            if item_id in self.items:
                del self.items[item_id]
        elif event_type == "ADD_PAYMENT":
            self.payment_method = data.get("payment_method")
            self.status = "PAYING"
        elif event_type == "SUBMIT_ORDER":
            self.status = "SUBMITTED"

    def to_dict(self):
        return {
            "session_id": self.session_id,
            "events": [e.to_dict() for e in self.events],
            "items": self.items,
            "payment_method": self.payment_method,
            "status": self.status
        }

    @classmethod
    def from_dict(cls, d):
        order = cls(session_id=d.get("session_id"))
        order.events = [Event.from_dict(e) for e in d.get("events", [])]
        order.items = d.get("items", {})
        order.payment_method = d.get("payment_method")
        order.status = d.get("status", "NEW")
        return order

    def __str__(self):
        return f"Order{{sessionId='{self.session_id}', events={self.events}, items={self.items}, paymentMethod='{self.payment_method}', status='{self.status}'}}"
