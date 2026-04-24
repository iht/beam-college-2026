import random
import uuid
import time
from typing import List, Dict
from models import Event

class EventGenerator:
    @staticmethod
    def generate_session_events(session_id: str, base_timestamp: int) -> List[Event]:
        events = []
        current_timestamp = base_timestamp
        cart = []
        item_counter = 1

        # Generate 28 shopping events (ADD_TO_CART or REMOVE_FROM_CART)
        for i in range(28):
            # If cart is empty, we MUST add something
            # Otherwise, 70% chance to add, 30% to remove
            if not cart or random.random() < 0.7:
                item_id = f"p_{item_counter}"
                item_counter += 1
                cart.append(item_id)
                data = {
                    "item_id": item_id,
                    "quantity": str(random.randint(1, 5))
                }
                event = Event(session_id, current_timestamp, "ADD_TO_CART", data)
            else:
                item_to_remove = random.choice(cart)
                cart.remove(item_to_remove)
                data = {
                    "item_id": item_to_remove
                }
                event = Event(session_id, current_timestamp, "REMOVE_FROM_CART", data)
            
            event.sequence = i + 1
            event.total_fragments = 30
            events.append(event)

            current_timestamp += random.randint(1000, 5000) # 1000 to 5000ms

        # Ensure at least one item remains in the cart for payment
        if not cart:
            item_id = f"p_{item_counter}"
            item_counter += 1
            data = {
                "item_id": item_id,
                "quantity": "1"
            }
            # Replace the last event to maintain exactly 30 total events
            last_event = events[-1]
            new_event = Event(session_id, last_event.timestamp, "ADD_TO_CART", data)
            new_event.sequence = last_event.sequence
            new_event.total_fragments = last_event.total_fragments
            events[-1] = new_event
            cart.append(item_id)

        # Add payment
        payment_data = {
            "payment_method": random.choice(["credit_card", "paypal", "apple_pay"])
        }
        payment_event = Event(session_id, current_timestamp, "ADD_PAYMENT", payment_data)
        payment_event.sequence = 29
        payment_event.total_fragments = 30
        events.append(payment_event)
        current_timestamp += random.randint(1000, 5000)

        # Submit order
        submit_event = Event(session_id, current_timestamp, "SUBMIT_ORDER", {})
        submit_event.sequence = 30
        submit_event.total_fragments = 30
        events.append(submit_event)

        return events

    @staticmethod
    def generate_multiple_sessions(num_sessions: int, start_timestamp: int) -> List[Event]:
        all_events = []
        for _ in range(num_sessions):
            session_id = str(uuid.uuid4())
            session_start = start_timestamp + random.randint(0, 30000)
            all_events.extend(EventGenerator.generate_session_events(session_id, session_start))
        
        # Shuffle all events to randomize publication order
        random.shuffle(all_events)
        return all_events
