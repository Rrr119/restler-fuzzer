# State-Machine Driven Sequence Generation

RESTler already infers producer/consumer dependencies from OpenAPI, but the
default sequence explorer only checks whether a request's dependency set is
satisfied.  It does not make resource lifecycle states explicit, so random or
short request combinations can miss transitions such as create -> update ->
read -> delete or transition after delete.

State-machine driven generation adds an explicit resource state model on top of
the existing grammar.  It is compatible with the current OpenAPI parser because
it consumes the dependency information already emitted by the compiler:
producer/consumer variables, writers, readers, and DELETE lifecycle semantics.

## Usage

Enable generation in the engine:

```bash
python restler/restler.py \
  --restler_grammar Compile/grammar.py \
  --custom_mutations Compile/dict.json \
  --stateful_sequence_generation \
  --fuzzing_mode bfs-minimal \
  --stateful_max_states 10000 \
  --stateful_max_depth 20
```

The engine writes these files to the experiment `logs` directory:

- `state_machine.json`: inferred operations, resource states, and transitions.
- `state_sequences.json`: deterministic, replayable sequence plan.
- `state_coverage.json`: planned and actual state/transition coverage, plus a
  fixed-seed random-combination baseline for comparison.

Replay a generated plan:

```bash
python restler/restler.py \
  --restler_grammar Compile/grammar.py \
  --custom_mutations Compile/dict.json \
  --stateful_sequence_plan /path/to/state_sequences.json \
  --fuzzing_mode bfs-minimal
```

Plan replay preserves sequence order and operation IDs.  For byte-for-byte
request replay, including the dynamically generated resource values, continue
to use RESTler's existing bug-bucket/trace replay logs.

## Offline generation and acceptance measurement

The offline utility accepts compiler-generated `grammar.json` files or OpenAPI
JSON/YAML documents:

```bash
python utilities/stateful_sequence_generation.py \
  --spec demo_server/swagger.json \
  --spec src/compiler/Restler.Compiler.Test/swagger/dependencyTests/input_producer_spec.json \
  --spec src/compiler/Restler.Compiler.Test/swagger/dependencyTests/nested_objects_naming.json \
  --max-depth 12 \
  --output-dir stateful_sequences
```

For each input it writes `<name>.state_machine.json`,
`<name>.state_sequences.json`, and `<name>.state_coverage.json`, then prints a
summary with state and transition coverage deltas.

Example acceptance result from the three OpenAPI documents above:

| OpenAPI document | Random state coverage | Stateful state coverage | Random transition coverage | Stateful transition coverage |
| --- | ---: | ---: | ---: | ---: |
| `demo_server/swagger.json` | 75.0% | 100% | 33.3% | 100% |
| `input_producer_spec.json` | 83.3% | 100% | 33.3% | 100% |
| `nested_objects_naming.json` | 68.8% | 100% | 29.7% | 100% |

Coverage is measured over the bounded state graph explored with
`--max-states` and `--max-depth`.  The random baseline uses the same number of
sequences as the stateful plan and a fixed seed, so results are reproducible.

## State model

Each request is a transition:

- `requires`: consumed dynamic objects that the request cannot create itself.
- `adds`: produced dynamic objects.
- `removes`: consumed objects removed by DELETE.

The initial state is empty.  A request is enabled only when all of its
`requires` are present.  The generator performs deterministic BFS over reachable
states, then uses a deterministic greedy set cover to select sequences that
cover reachable `(state, request)` transitions.

The model is bounded by `stateful_max_states` and `stateful_max_depth`; when a
bound is reached, the coverage report marks the model as truncated.  This
prevents large APIs from causing unbounded state explosion.

