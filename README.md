# Apache Beam Two-Level Cache Project

This project implements an Apache Beam pipeline to merge and reconstruct e-commerce session entities from partial events using a two-level cache (In-Memory State and Local Filesystem persistent storage).

## Event Generation Logic

The project generates events internally using Java (`EventGenerator.java`) to simulate high-volume, concurrent user traffic by generating complex event sequences.

### Session Structure

Each execution generates a configurable number of sessions (based on requested event count), with each session containing exactly **30 events**. While sessions overlap in time, the processing order is globally randomized to simulate real-world distributed system behavior (out-of-order arrival).

### Logical Constraints & Event Ordering

Each session follows a strict stateful progression to ensure data integrity:

1. **Shopping Phase (Events 1-28)**:
    * Consists of `ADD_TO_CART` and `REMOVE_FROM_CART` events.
    * **Constraint**: An item must be added before it can be removed.
    * **Safety**: The generator ensures at least one item remains in the cart before proceeding to checkout.
2. **Payment Phase (Event 29)**:
    * Exactly one `ADD_PAYMENT` event.
    * **Constraint**: This always occurs after the shopping phase.
3. **Completion Phase (Event 30)**:
    * Exactly one `SUBMIT_ORDER` event.
    * **Constraint**: This is **always the final event** in every session.

### Timing and Delivery

* **Internal Consistency**: Every event within a session has a strictly increasing `timestamp` (chronological order).
* **External Randomization**: Events from all generated sessions are shuffled before being processed. This simulates out-of-order data arrival, testing the pipeline's ability to handle event-time processing.

## Prerequisites

* Java 25
* Gradle 9.1+

---

## How to Run and Test

### 1. Unit Testing

Run unit tests to verify the core business logic of the pipeline.

```bash
gradle test
```

### 2. Running the Pipeline Locally

To run the pipeline locally using the `DirectRunner`. The pipeline will generate events internally, process them, and write the output to stdout.
You have to pass some arguments to the pipeline, to specify the number of events to generate or the state base directory:

```bash
gradle run --args="--numEvents=300 --stateBaseDir=/tmp/beam-state"
```

Check the output in `stdout` to see the merged sessions (Orders). Failures will be logged to `stderr`.
