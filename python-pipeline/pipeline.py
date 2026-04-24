import argparse
import logging
import sys
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from models import Order
from transforms import ParseEventFn, KeyBySessionId, MergeFn
from storage import DefaultStateStoreProvider
from generator import EventGenerator

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--numEvents',
        dest='num_events',
        type=int,
        default=100,
        help='Number of events to generate.')
    parser.add_argument(
        '--stateBaseDir',
        dest='state_base_dir',
        required=True,
        help='Base directory for external state storage.')
    parser.add_argument(
        '--stateMoveThresholdSeconds',
        dest='state_move_threshold_seconds',
        type=int,
        default=2,
        help='Threshold in seconds to move state to external storage.')
    
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    # We use save_main_session because stateful DoFns need access to global imports
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # Generate events outside the pipeline and use Create
    # This is to match Java's Create.of(events) which uses generated list.
    print(f"Generating {known_args.num_events} events...")
    events = EventGenerator.generate_multiple_sessions(
        (known_args.num_events + 29) // 30, # estimate sessions
        int(time.time() * 1000)
    )
    # Trim to exact number requested
    events = events[:known_args.num_events]
    
    # Serialize to JSON to match Java's PCollection<String> input
    raw_events = [json.dumps(e.to_dict()) for e in events]

    with beam.Pipeline(options=pipeline_options) as p:
        # 1. Internal Event Generation (via Create)
        input_events = p | "GenerateEvents" >> beam.Create(raw_events)

        # 2. Parse JSON strings into Event objects
        parsed_events = input_events | "ParseEvents" >> ParseEventFn()

        # 3. Key events by SessionId
        keyed_events = (
            parsed_events[ParseEventFn.SUCCESS_TAG]
            | "KeyBySessionId" >> KeyBySessionId()
        )

        # 4. Stateful Process and Merge
        merge_result = keyed_events | "ProcessAndMerge" >> MergeFn(
            known_args.state_base_dir,
            known_args.state_move_threshold_seconds,
            DefaultStateStoreProvider()
        )

        # 5. Handle All Failures (Parsing + Merging)
        failures = (
            (parsed_events[ParseEventFn.FAILURE_TAG], merge_result[MergeFn.FAILURE_TAG])
            | "FlattenFailures" >> beam.Flatten()
        )
        
        def log_failure(element):
            logging.error(f"Failure output: {element}")
        
        failures | "LogFailures" >> beam.Map(log_failure)

        # 6. Write successful merges to stdout
        def print_order(order):
            print(order)
            return order

        merge_result[MergeFn.SUCCESS_TAG] | "WriteToStdout" >> beam.Map(print_order)

if __name__ == '__main__':
    import time # needed for time.time() above
    logging.getLogger().setLevel(logging.INFO)
    run()
