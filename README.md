# Replica State Replay

Replica State Replay is a lightweight system for recording, replaying, and
validating state changes across replicated application instances.

The project is designed to make replicated-state behavior reproducible,
helping developers debug synchronization problems, investigate failures,
and verify that the same sequence of events produces the same final state.

## Features

- Record state-changing events
- Replay events in deterministic order
- Reconstruct application state from an event history
- Compare expected and actual replica state
- Detect state divergence
- Support debugging and failure investigation
- Provide reproducible state-transition tests
- Keep replay logic isolated from normal application execution

## Use Cases

Replica State Replay can be useful for:

- Distributed-system debugging
- Replica consistency verification
- Event-sourced applications
- Recovery testing
- Regression testing
- Reproducing production failures
- State synchronization analysis

## How It Works

The basic replay flow is:

1. Capture state-changing operations.
2. Store them in a reproducible order.
3. Initialize a clean replica state.
4. Replay the recorded operations.
5. Reconstruct the resulting state.
6. Compare the replayed state with the expected state.
7. Report any divergence or replay failure.

Conceptually:

```text
Recorded Events
      |
      v
+----------------+
| Replay Engine  |
+----------------+
      |
      v
+----------------+
| State Reducer  |
+----------------+
      |
      v
Reconstructed State
      |
      v
Validation / Comparison
