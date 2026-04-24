import logging
import json
import time
from typing import Tuple, Optional

import apache_beam as beam
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec, TimerSpec, on_timer
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.pvalue import TaggedOutput

from models import Event, Order
from storage import StateStoreProvider

logger = logging.getLogger(__name__)

class ParseEventFn(beam.PTransform):
    """Simple PTransform to parse JSON strings with error handling."""
    SUCCESS_TAG = 'success'
    FAILURE_TAG = 'failure'

    def expand(self, pcoll):
        return pcoll | beam.ParDo(self.ParseDoFn()).with_outputs(
            self.SUCCESS_TAG, self.FAILURE_TAG, main='main_unused'
        )

    class ParseDoFn(beam.DoFn):
        def process(self, json_str: str):
            try:
                d = json.loads(json_str)
                event = Event.from_dict(d)
                yield TaggedOutput(ParseEventFn.SUCCESS_TAG, event)
            except Exception as e:
                logger.error(f"Failed to parse event JSON: {json_str}", exc_info=True)
                yield TaggedOutput(ParseEventFn.FAILURE_TAG, json_str)

class KeyBySessionId(beam.PTransform):
    """A PTransform that keys events by their session ID."""
    def expand(self, pcoll):
        return pcoll | beam.Map(lambda event: (event.session_id, event))

class MergeFn(beam.PTransform):
    """A PTransform that wraps a stateful DoFn to merge shopping events into an aggregated Order state.
    It uses a two-level cache: Beam state (first level) and a persistent StateStore (second level).
    """
    SUCCESS_TAG = 'success'
    FAILURE_TAG = 'failure'

    def __init__(self, state_base_dir: str, state_move_threshold_seconds: Optional[int], state_store_provider):
        super().__init__()
        self.state_base_dir = state_base_dir
        self.state_move_threshold_seconds = state_move_threshold_seconds if state_move_threshold_seconds is not None else 2
        self.state_store_provider = state_store_provider

    def expand(self, pcoll):
        return pcoll | beam.ParDo(
            MergeDoFn(self.state_base_dir, self.state_move_threshold_seconds, self.state_store_provider)
        ).with_outputs(
            self.SUCCESS_TAG, self.FAILURE_TAG, main='main_unused'
        )

class MergeDoFn(beam.DoFn):
    ACCUMULATED_EVENTS = ReadModifyWriteStateSpec('accumulatedEvents', beam.coders.PickleCoder())
    STATE_TIMER = TimerSpec('stateTimer', TimeDomain.WATERMARK)

    def __init__(self, state_base_dir: str, state_move_threshold_seconds: int, state_store_provider):
        self.state_base_dir = state_base_dir
        self.state_move_threshold_seconds = state_move_threshold_seconds
        self.state_store_provider = state_store_provider
        self.state_store = None

    def setup(self):
        self.state_store = self.state_store_provider.get_state_store(self.state_base_dir)

    def teardown(self):
        if self.state_store:
            self.state_store.close()

    def process(self, element, 
                state=beam.DoFn.StateParam(ACCUMULATED_EVENTS),
                timer=beam.DoFn.TimerParam(STATE_TIMER),
                timestamp=beam.DoFn.TimestampParam):
        
        session_id, new_event = element

        try:
            order = state.read()

            if order is None:
                order = self._lookup_external_state(session_id)
                if order is None:
                    order = Order(session_id)
                else:
                    state.write(order)

            order.add_event(new_event)
            state.write(order)
            yield TaggedOutput(MergeFn.SUCCESS_TAG, order)

            # Set timer relative to element timestamp in event time
            timer.set(timestamp + self.state_move_threshold_seconds)
            
        except Exception as e:
            logger.error(f"Failed to process event for session: {session_id}", exc_info=True)
            try:
                yield TaggedOutput(MergeFn.FAILURE_TAG, json.dumps(new_event.to_dict()))
            except Exception as e2:
                logger.error("Failed to write failed event to DLQ", exc_info=True)

    @on_timer(STATE_TIMER)
    def on_state_timer(self, state=beam.DoFn.StateParam(ACCUMULATED_EVENTS),
                    timer=beam.DoFn.TimerParam(STATE_TIMER),
                    timestamp=beam.DoFn.TimestampParam):
        
        logger.info("Timer fired for processing state.")
        order = state.read()
        if order:
            if order.session_id:
                try:
                    self._write_to_external_state(order.session_id, json.dumps(order.to_dict()))
                    logger.info(f"Moved state to external storage for session: {order.session_id}")
                    state.clear()
                except Exception as e:
                    logger.error(f"Failed to move state to external storage for session: {order.session_id}. Will retry.", exc_info=True)
                    # Reschedule timer
                    timer.set(timestamp + self.state_move_threshold_seconds)
            else:
                state.clear()
        else:
            logger.warn("Timer fired but state was empty.")

    def _lookup_external_state(self, session_id: str) -> Optional[Order]:
        data = self.state_store.read(session_id)
        if data:
            d = json.loads(data)
            return Order.from_dict(d)
        return None

    def _write_to_external_state(self, session_id: str, data_json: str):
        self.state_store.update(session_id, data_json)
