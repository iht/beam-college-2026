# Assembling the Puzzle: High-Performance Entity Building streaming Beam pipeline using a Two-Tiered State Architecture

[![Beam College 2026](https://img.shields.io/badge/Beam%20College-2026-blue)](https://beamcollege.dev/)

This repository contains the official demo for the talk **"Assembling the Puzzle: High-Performance Entity Building streaming Beam pipeline using a Two-Tiered State Architecture"** presented at [Beam College 2026](https://beamcollege.dev/).

## 👥 Authors

*   [CanburakTumer](https://github.com/CanburakTumer)
*   [iht](https://github.com/iht/)

---

## 🚀 Project Overview: Apache Beam Two-Level Cache

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

### 3. Debugging the Pipeline

The tests in `src/test/java/com/google/cloud/pso/BeamCollegeDemoTest.java` are specifically designed to be run step-by-step with a debugger to help you understand how the pipeline works.

This is a standard Java Gradle project, so you can use your favorite IDE or debugger (e.g., VS Code, IntelliJ IDEA) without needing any cloud environment.

#### Step-by-Step Guide

1.  **Set Breakpoints**:
    *   Open `src/main/java/com/google/cloud/pso/transform/MergeFn.java`.
    *   Put a breakpoint inside the `processElement` method (around line 89).
    *   Put a breakpoint inside the `onTimer` method (around line 140).

2.  **Run Tests in Debug Mode**:
    *   Run the tests in `BeamCollegeDemoTest.java` using your IDE's debugger.
    *   For example, in VS Code, you can use the "Debug Test" codelens above the test methods.

3.  **Observe Variables**:
    *   When the breakpoint hits, inspect the local variables like `sessionId`, `newEvent`, and `currentStateJson`.
    *   Observe how `MergeFn` reads from internal state, updates it, and interacts with the external state store.

4.  **Inspect State Files**:
    *   While the tests are running, `MergeFn` will offload state to a file-based cache when the timer fires.
    *   In the unit tests, these files are written to a temporary folder (check the value of `stateBaseDir` in the debugger to see the path).
    *   If you run the pipeline locally using the `gradle run` command (mentioned above), the state files will be written to `/tmp/beam-state` (or whatever you specified in `--stateBaseDir`). You can inspect these files to see the JSON serialized state of the sessions.
