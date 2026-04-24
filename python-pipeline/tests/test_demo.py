import unittest
import os
import sys
import tempfile
import shutil

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.test_stream import TestStream
from apache_beam.utils.timestamp import Timestamp

# Add parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from models import Event, Order
from transforms import MergeFn
from storage import DefaultStateStoreProvider
from tests.test_utils import DemoTestUtils

class TestBeamCollegeDemo(unittest.TestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_single_order_merging_and_offloading(self):
        session_id = "demo-session-1"
        start_time = Timestamp(0)
        state_base_dir = os.path.join(self.test_dir, "state")

        events = DemoTestUtils.create_single_order_scenario(session_id, 0)

        stream = (TestStream()
                  .add_elements([(session_id, events[0])])
                  .add_elements([(session_id, events[1])])
                  .advance_watermark_to(Timestamp(10))
                  .add_elements([(session_id, events[2])])
                  .advance_watermark_to_infinity())

        with TestPipeline() as p:
            results = (p 
                       | stream 
                       | MergeFn(
                           state_base_dir,
                           2,
                           DefaultStateStoreProvider()
                       ))

            def check_orders(elements):
                self.assertTrue(len(elements) >= 3, f"Should have at least 3 outputs, got {len(elements)}")

            assert_that(results[MergeFn.SUCCESS_TAG], check_orders)

        # Verify that the state file was created at some point
        self.assertTrue(
            DemoTestUtils.check_state_file_exists(state_base_dir, session_id),
            "State file should exist in cold cache"
        )

    def test_interleaved_orders_matching(self):
        session_id_a = "session-A"
        session_id_b = "session-B"
        state_base_dir = os.path.join(self.test_dir, "state-interleaved")

        events = DemoTestUtils.create_interleaved_orders_scenario(session_id_a, session_id_b, 0)

        stream = (TestStream()
                  .add_elements([(session_id_a, events[0])])
                  .add_elements([(session_id_b, events[1])])
                  .add_elements([(session_id_a, events[2])])
                  .add_elements([(session_id_b, events[3])])
                  .advance_watermark_to_infinity())

        with TestPipeline() as p:
            results = (p 
                       | stream 
                       | MergeFn(
                           state_base_dir,
                           2,
                           DefaultStateStoreProvider()
                       ))

            def check_interleaved(orders):
                count_a = sum(1 for o in orders if o.session_id == session_id_a)
                count_b = sum(1 for o in orders if o.session_id == session_id_b)
                self.assertEqual(count_a, 2, "Order A should have 2 fragments processed")
                self.assertEqual(count_b, 2, "Order B should have 2 fragments processed")

            assert_that(results[MergeFn.SUCCESS_TAG], check_interleaved)

    def test_state_warm_up_from_cold_cache(self):
        session_id = "warmup-session"
        state_base_dir = os.path.join(self.test_dir, "state-warmup")

        events = DemoTestUtils.create_single_order_scenario(session_id, 0)

        stream = (TestStream()
                  .add_elements([(session_id, events[0])])
                  .add_elements([(session_id, events[1])])
                  .advance_watermark_to(Timestamp(10))
                  .add_elements([(session_id, events[2])])
                  .advance_watermark_to_infinity())

        with TestPipeline() as p:
            results = (p 
                       | stream 
                       | MergeFn(
                           state_base_dir,
                           2,
                           DefaultStateStoreProvider()
                       ))

            def check_final_order(orders):
                final_order = None
                for order in orders:
                    final_order = order
                self.assertNotNull(final_order, "Final order should not be null")
                self.assertEqual(len(final_order.events), 3, "Final order should have 3 fragments")

            # Wait, self.assertNotNull is not a method in Python unittest, it is assertIsNotNone.
            # I should fix that.
            # Let's use self.assertIsNotNone.
            
            def check_final_order_fixed(orders):
                final_order = None
                for order in orders:
                    if order.session_id == session_id:
                        final_order = order
                self.assertIsNotNone(final_order, "Final order should not be null")
                self.assertEqual(len(final_order.events), 3, "Final order should have 3 fragments")

            assert_that(results[MergeFn.SUCCESS_TAG], check_final_order_fixed)

if __name__ == '__main__':
    unittest.main()
