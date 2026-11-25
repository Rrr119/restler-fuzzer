# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""State-machine driven sequence generation for RESTler.

The RESTler compiler already infers producer/consumer relationships and emits
them in the grammar.  This module turns those relationships into an explicit
resource state machine, explores reachable states deterministically, and emits
a replayable suite of request sequences that covers state transitions rather
than randomly combining requests.

The module intentionally has no dependency on the running RESTler engine.  It
can consume Request objects, a compiler-generated grammar.json, or an OpenAPI
document, which makes it usable both by the engine and by offline tooling.
"""
from __future__ import print_function

import hashlib
import heapq
import json
import os
import re
from collections import deque


HTTP_METHODS = ("GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "TRACE")
PRODUCER_METHODS = ("POST", "PUT")
DESTRUCTOR_METHODS = ("DELETE",)
PLAN_SCHEMA_VERSION = 1
GENERATOR_NAME = "restler-state-machine"


def _sorted_list(values):
    return sorted(values or [])


def _stable_hash(value):
    if not isinstance(value, str):
        value = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(value.encode("utf-8")).hexdigest()


def _state_to_tuple(state):
    if isinstance(state, tuple):
        return state
    return tuple(sorted((key, int(value)) for key, value in state.items() if value > 0))


def _state_to_dict(state):
    return {key: value for key, value in _state_to_tuple(state)}


def _normalize_name(value):
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def _singularize(value):
    value = str(value or "")
    if value.endswith("ies") and len(value) > 3:
        return value[:-3] + "y"
    if value.endswith("ses") and len(value) > 3:
        return value[:-2]
    if value.endswith("s") and not value.endswith("ss") and len(value) > 1:
        return value[:-1]
    return value


def _is_id_like(name):
    normalized = _normalize_name(name)
    return (
        normalized in ("id", "name", "key", "uuid", "guid")
        or normalized.endswith("id")
        or normalized.endswith("name")
        or normalized.endswith("key")
    )


def _matches_parameter_to_property(parameter_name, property_name, resource_name):
    parameter = _normalize_name(parameter_name)
    prop = _normalize_name(property_name)
    resource = _normalize_name(_singularize(resource_name))

    if parameter == prop:
        return True
    if prop in ("id", "name", "key", "uuid", "guid"):
        return True
    if parameter in (resource + "id", resource + "name", resource + "key"):
        return True
    if prop == resource or prop == resource + "id" or prop == resource + "name":
        return True
    # Handle camelCase and dash-separated IDs such as postId, post-id,
    # system-environment-id, and their collection counterparts.
    parameter_base = re.sub(r"(id|name|key)$", "", parameter)
    prop_base = re.sub(r"(id|name|key)$", "", prop)
    return bool(parameter_base) and bool(prop_base) and (
        parameter_base == prop_base or parameter_base == resource or prop_base == resource
    )


class ApiOperation(object):
    """A request template interpreted as a transition in the resource state machine."""

    def __init__(
        self,
        method,
        endpoint,
        consumes=None,
        produces=None,
        request=None,
        operation_id=None,
        definition_hash=None,
        metadata=None,
    ):
        self.method = str(method or "").upper()
        self.endpoint = str(endpoint or "")
        self.consumes = frozenset(consumes or ())
        self.produces = frozenset(produces or ())
        self.request = request
        self.definition_hash = definition_hash or ""
        metadata = metadata or {}
        self.metadata = dict(metadata)

        # A request that creates a value does not require that value to already
        # exist.  This is important for PUT create-or-update operations, where
        # the same request both reads and writes the resource identifier.
        self.requires = frozenset(self.consumes - self.produces)
        self.removes = frozenset(self.consumes if self.method in DESTRUCTOR_METHODS else ())
        self.adds = frozenset(self.produces - self.removes)

        identity = operation_id or self._default_operation_id()
        self.operation_id = str(identity)

    def _default_operation_id(self):
        suffix = self.definition_hash or "0"
        return "{} {}#{}".format(self.method, self.endpoint, suffix[:16])

    @property
    def request_id(self):
        return "{} {}".format(self.method, self.endpoint)

    @property
    def is_destructor(self):
        return self.method in DESTRUCTOR_METHODS

    @property
    def is_producer(self):
        return bool(self.produces)

    @property
    def creates_from_empty_state(self):
        return bool(self.adds) and not self.requires

    def apply(self, state, max_resource_count=1):
        """Applies this operation to a state and returns the new state.

        ``None`` means the operation is not enabled in the supplied state.
        """
        state_dict = dict(_state_to_tuple(state))
        if not self.requires.issubset(state_dict):
            return None

        for variable in self.removes:
            state_dict.pop(variable, None)
        for variable in self.adds:
            state_dict[variable] = min(
                max_resource_count, state_dict.get(variable, 0) + 1
            )
        return _state_to_tuple(state_dict)

    def to_dict(self):
        return {
            "operation_id": self.operation_id,
            "method": self.method,
            "endpoint": self.endpoint,
            "consumes": _sorted_list(self.consumes),
            "produces": _sorted_list(self.produces),
            "requires": _sorted_list(self.requires),
            "removes": _sorted_list(self.removes),
            "adds": _sorted_list(self.adds),
            "definition_hash": self.definition_hash,
        }


def _walk_payloads(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            for item in _walk_payloads(child):
                yield item
    elif isinstance(value, list):
        for child in value:
            for item in _walk_payloads(child):
                yield item


def _dynamic_objects(payload):
    """Yields all dynamic object descriptors embedded in compiler payloads."""
    for node in _walk_payloads(payload):
        if not isinstance(node, dict):
            continue
        for key in ("dynamicObject", "DynamicObject"):
            dynamic_object = node.get(key)
            if isinstance(dynamic_object, dict):
                yield dynamic_object


def operations_from_requests(requests, deduplicate=True):
    """Builds operations from RESTler Request objects.

    This is the engine-native path and uses the exact dependency data produced
    by the existing compiler and consumed by the existing sequence generator.
    """
    operations = []
    seen = set()
    for index, request in enumerate(requests):
        method = str(request.method or "").strip().upper()
        endpoint = request.endpoint_no_dynamic_objects
        if not endpoint:
            endpoint = request.endpoint
        definition_hash = str(getattr(request, "hex_definition", ""))
        consumes = set(request.consumes)
        produces = set(request.produces)
        operation = ApiOperation(
            method=method,
            endpoint=endpoint,
            consumes=consumes,
            produces=produces,
            request=request,
            operation_id="{} {}#{}".format(method, endpoint, index),
            definition_hash=definition_hash,
        )
        if deduplicate:
            key = (
                operation.method,
                operation.endpoint,
                tuple(sorted(operation.consumes)),
                tuple(sorted(operation.produces)),
            )
            if key in seen:
                continue
            seen.add(key)
        operations.append(operation)
    return _sort_operations(operations)


def _grammar_request_payloads(request_json):
    for key in (
        "path",
        "queryParameters",
        "headerParameters",
        "bodyParameters",
    ):
        for item in _walk_payloads(request_json.get(key)):
            yield item


def operations_from_grammar(grammar):
    """Builds operations from the compiler's grammar.json.

    The function only reads fields that have been part of RESTler grammar
    output for a long time.  It therefore works with grammars generated by the
    existing OpenAPI parser without requiring grammar format changes.
    """
    if isinstance(grammar, (str, os.PathLike)):
        with open(grammar, "r", encoding="utf-8") as grammar_file:
            grammar = json.load(grammar_file)

    requests = grammar.get("Requests", []) if isinstance(grammar, dict) else grammar
    operations = []
    for index, request_json in enumerate(requests):
        request_id = request_json.get("id") or {}
        method = str(
            request_json.get("method") or request_id.get("method") or ""
        ).upper()
        endpoint = request_id.get("endpoint") or ""
        consumes = set()
        produces = set()

        for node in _grammar_request_payloads(request_json):
            if not isinstance(node, dict):
                continue
            for dynamic_object in _dynamic_objects(node):
                variable = dynamic_object.get("variableName")
                if not variable:
                    continue
                if dynamic_object.get("isWriter"):
                    produces.add(variable)
                else:
                    consumes.add(variable)

        dependency_data = request_json.get("dependencyData") or {}
        response_parser = dependency_data.get("responseParser") or {}
        for writer in response_parser.get("writerVariables", []) or []:
            access_path = writer.get("accessPathParts", {}).get("path", [])
            if access_path:
                produces.add(_grammar_writer_variable(writer))
        for writer in response_parser.get("headerWriterVariables", []) or []:
            produces.add(_grammar_writer_variable(writer))
        for writer in dependency_data.get("inputWriterVariables", []) or []:
            produces.add(_grammar_writer_variable(writer))

        operation = ApiOperation(
            method=method,
            endpoint=endpoint,
            consumes=consumes,
            produces=produces,
            operation_id="{} {}#{}".format(method, endpoint, index),
            definition_hash=_stable_hash(request_json),
        )
        operations.append(operation)
    return _sort_operations(operations)


def _grammar_writer_variable(writer):
    access_path = writer.get("accessPathParts", {}).get("path", [])
    request_id = writer.get("requestId") or {}
    endpoint = request_id.get("endpoint", "")
    method = str(request_id.get("method", "")).lower()
    replace_targets = re.compile(r"/|\.|__|\{|\}|\$|-|:")
    parts = replace_targets.split(endpoint)
    parts.append(method)
    for access_path_part in access_path:
        if access_path_part == "[0]":
            access_path_part = "0"
        parts.extend(replace_targets.split(access_path_part))
    return "_".join(parts)


def _sort_operations(operations):
    return sorted(
        operations,
        key=lambda operation: (
            operation.endpoint,
            operation.method,
            operation.operation_id,
        ),
    )


class StateMachine(object):
    """A bounded, deterministic resource state machine."""

    def __init__(self, operations, max_resource_count=1):
        self.operations = tuple(_sort_operations(operations))
        self.max_resource_count = max(1, int(max_resource_count))
        self._operations_by_id = {
            operation.operation_id: operation for operation in self.operations
        }
        self._operations_by_identity = {}
        self._operations_by_definition = {}
        for operation in self.operations:
            self._operations_by_identity.setdefault(
                (operation.method, operation.endpoint), operation
            )
            if operation.definition_hash:
                self._operations_by_definition.setdefault(
                    (
                        operation.method,
                        operation.endpoint,
                        operation.definition_hash,
                    ),
                    operation,
                )
        self.initial_state = tuple()
        self._exploration = None

    @property
    def operations_by_id(self):
        return dict(self._operations_by_id)

    def operation_for_request(self, request):
        method = str(request.method or "").strip().upper()
        endpoint = request.endpoint_no_dynamic_objects or request.endpoint
        definition_hash = str(getattr(request, "hex_definition", ""))
        operation = self._operations_by_definition.get(
            (method, endpoint, definition_hash)
        )
        if operation is not None:
            return operation
        return self._operations_by_identity.get((method, endpoint))

    def enabled_operations(self, state):
        state_tuple = _state_to_tuple(state)
        return [
            operation
            for operation in self.operations
            if operation.apply(
                state_tuple, max_resource_count=self.max_resource_count
            )
            is not None
        ]

    def apply(self, operation, state):
        return operation.apply(state, max_resource_count=self.max_resource_count)

    def explore(self, max_states=10000, max_depth=100):
        """Explores reachable states and returns the explored transition graph."""
        if (
            self._exploration is not None
            and self._exploration.max_states == max(1, int(max_states))
            and self._exploration.max_depth == max(0, int(max_depth))
        ):
            return self._exploration

        max_states = max(1, int(max_states))
        max_depth = max(0, int(max_depth))
        parents = {self.initial_state: None}
        depths = {self.initial_state: 0}
        transitions = {}
        queue = deque([self.initial_state])
        terminal_reasons = {}

        while queue:
            state = queue.popleft()
            depth = depths[state]
            if depth >= max_depth:
                terminal_reasons[state] = "max_depth"
                continue

            enabled = self.enabled_operations(state)
            if not enabled:
                terminal_reasons[state] = "no_enabled_operations"

            for operation in enabled:
                next_state = self.apply(operation, state)
                edge = (state, operation.operation_id)
                if edge not in transitions:
                    transitions[edge] = next_state

                if next_state not in parents:
                    if len(parents) >= max_states:
                        terminal_reasons.setdefault("max_states", max_states)
                        continue
                    parents[next_state] = (state, operation.operation_id)
                    depths[next_state] = depth + 1
                    queue.append(next_state)

        self._exploration = StateExploration(
            machine=self,
            initial_state=self.initial_state,
            parents=parents,
            depths=depths,
            transitions=transitions,
            terminal_reasons=terminal_reasons,
            max_states=max_states,
            max_depth=max_depth,
        )
        return self._exploration

    def state_machine_dict(self, exploration=None):
        exploration = exploration or self.explore()
        return {
            "schema_version": PLAN_SCHEMA_VERSION,
            "generator": GENERATOR_NAME,
            "initial_state": {},
            "max_resource_count": self.max_resource_count,
            "operations": [operation.to_dict() for operation in self.operations],
            "states": [
                _state_to_dict(state)
                for state in sorted(exploration.parents.keys())
            ],
            "transitions": exploration.transition_dicts(),
        }


class StateExploration(object):
    """The result of bounded state exploration."""

    def __init__(
        self,
        machine,
        initial_state,
        parents,
        depths,
        transitions,
        terminal_reasons,
        max_states,
        max_depth,
    ):
        self.machine = machine
        self.initial_state = initial_state
        self.parents = parents
        self.depths = depths
        self.transitions = transitions
        self.terminal_reasons = terminal_reasons
        self.max_states = max_states
        self.max_depth = max_depth

    @property
    def states(self):
        return set(self.parents.keys())

    @property
    def transition_ids(self):
        return set(self.transitions.keys())

    def path_to_state(self, state):
        state = _state_to_tuple(state)
        operations = []
        while self.parents.get(state) is not None:
            previous_state, operation_id = self.parents[state]
            operations.append(operation_id)
            state = previous_state
        operations.reverse()
        return operations

    def path_for_transition(self, state, operation_id):
        state = _state_to_tuple(state)
        return self.path_to_state(state) + [operation_id]

    def transition_dicts(self):
        results = []
        for (state, operation_id), next_state in sorted(
            self.transitions.items(),
            key=lambda item: (
                item[0][0],
                item[0][1],
                item[1],
            ),
        ):
            results.append(
                {
                    "source_state": _state_to_dict(state),
                    "operation_id": operation_id,
                    "target_state": _state_to_dict(next_state),
                }
            )
        return results

    def coverage_dict(self):
        return {
            "states_reachable": len(self.states),
            "transitions_reachable": len(self.transitions),
            "max_states": self.max_states,
            "max_depth": self.max_depth,
            "truncated": bool(
                self.terminal_reasons.get("max_states")
                or any(reason == "max_depth" for reason in self.terminal_reasons.values())
            ),
        }


class StateCoverage(object):
    """Coverage accumulated by replaying one or more sequences."""

    def __init__(self, machine):
        self.machine = machine
        self.visited_states = set()
        self.visited_transitions = set()
        self.sequence_count = 0
        self.total_steps = 0
        self.invalid_steps = 0
        self._sequence_hashes = set()

    def record_sequence(self, operation_ids):
        operation_ids = list(operation_ids)
        sequence_hash = _stable_hash(operation_ids)
        if sequence_hash in self._sequence_hashes:
            return
        self._sequence_hashes.add(sequence_hash)
        self.sequence_count += 1

        state = self.machine.initial_state
        self.visited_states.add(state)
        for operation_id in operation_ids:
            operation = self.machine.operations_by_id.get(operation_id)
            self.total_steps += 1
            if operation is None:
                self.invalid_steps += 1
                break
            next_state = self.machine.apply(operation, state)
            if next_state is None:
                self.invalid_steps += 1
                break
            self.visited_transitions.add((state, operation_id))
            self.visited_states.add(next_state)
            state = next_state

    def to_dict(self, exploration=None, states_total=None, transitions_total=None):
        if exploration is not None:
            states_total = len(exploration.states)
            transitions_total = len(exploration.transitions)
        elif states_total is None or transitions_total is None:
            exploration = self.machine.explore()
            states_total = len(exploration.states)
            transitions_total = len(exploration.transitions)
        total_states = max(1, states_total)
        total_transitions = max(1, transitions_total)
        return {
            "sequences": self.sequence_count,
            "steps": self.total_steps,
            "invalid_steps": self.invalid_steps,
            "states_covered": len(self.visited_states),
            "states_total": states_total,
            "state_coverage": round(len(self.visited_states) / total_states, 6),
            "transitions_covered": len(self.visited_transitions),
            "transitions_total": transitions_total,
            "transition_coverage": round(
                len(self.visited_transitions) / total_transitions, 6
            ),
        }


def measure_sequences(machine, sequences, exploration=None):
    """Measures state and transition coverage of a suite of operation-id sequences."""
    coverage = StateCoverage(machine)
    for sequence in sequences:
        if isinstance(sequence, dict):
            operation_ids = [
                step.get("operation_id") for step in sequence.get("steps", [])
            ]
        else:
            operation_ids = [
                step.operation_id if hasattr(step, "operation_id") else step
                for step in sequence
            ]
        coverage.record_sequence(operation_ids)
    return coverage, coverage.to_dict(exploration or machine.explore())


class SequencePlanStep(object):
    def __init__(self, operation):
        self.operation_id = operation.operation_id
        self.method = operation.method
        self.endpoint = operation.endpoint
        self.consumes = operation.consumes
        self.produces = operation.produces
        self.requires = operation.requires
        self.removes = operation.removes
        self.adds = operation.adds

    def to_dict(self):
        return {
            "operation_id": self.operation_id,
            "method": self.method,
            "endpoint": self.endpoint,
            "consumes": _sorted_list(self.consumes),
            "produces": _sorted_list(self.produces),
            "requires": _sorted_list(self.requires),
            "removes": _sorted_list(self.removes),
            "adds": _sorted_list(self.adds),
        }


class SequencePlan(object):
    """One deterministic sequence in a state-machine plan."""

    def __init__(self, operations):
        self.operations = list(operations)
        self.sequence_id = _stable_hash(
            [operation.operation_id for operation in self.operations]
        )[:16]

    @property
    def operation_ids(self):
        return [operation.operation_id for operation in self.operations]

    @property
    def length(self):
        return len(self.operations)

    def to_dict(self):
        return {
            "sequence_id": self.sequence_id,
            "steps": [SequencePlanStep(operation).to_dict() for operation in self.operations],
        }


class StatefulSequencePlan(object):
    """A replayable set of deterministic sequences."""

    def __init__(self, machine, sequences, coverage, baseline=None):
        self.machine = machine
        self.sequences = list(sequences)
        self.coverage = coverage
        self.baseline = baseline
        self.model = None
        self.model_digest = _stable_hash(
            [operation.to_dict() for operation in machine.operations]
        )

    @property
    def plan_id(self):
        return _stable_hash(
            {
                "model_digest": self.model_digest,
                "sequences": [sequence.operation_ids for sequence in self.sequences],
            }
        )

    def to_dict(self):
        result = {
            "schema_version": PLAN_SCHEMA_VERSION,
            "generator": GENERATOR_NAME,
            "model_digest": self.model_digest,
            "plan_id": self.plan_id,
            "initial_state": {},
            "operations": [operation.to_dict() for operation in self.machine.operations],
            "sequences": [sequence.to_dict() for sequence in self.sequences],
            "coverage": self.coverage,
        }
        if self.baseline is not None:
            result["random_baseline"] = self.baseline
        if hasattr(self, "exploration"):
            result["model"] = self.exploration.coverage_dict()
        elif self.model is not None:
            result["model"] = self.model
        return result

    def to_json(self, indent=2):
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True) + "\n"

    def write(self, path):
        _write_json(path, self.to_dict(), indent=2)
        return path

    @classmethod
    def from_dict(cls, data):
        """Loads a plan for replay.

        The original StateMachine can be reconstructed from the plan's
        operations, so replay does not require re-parsing the OpenAPI spec.
        """
        operations = []
        for operation_json in data.get("operations", []):
            operations.append(
                ApiOperation(
                    method=operation_json.get("method"),
                    endpoint=operation_json.get("endpoint"),
                    consumes=operation_json.get("consumes", []),
                    produces=operation_json.get("produces", []),
                    operation_id=operation_json.get("operation_id"),
                    definition_hash=operation_json.get("definition_hash", ""),
                )
            )
        machine = StateMachine(operations)
        sequences = []
        operations_by_id = machine.operations_by_id
        for sequence_json in data.get("sequences", []):
            sequence_operations = []
            for step in sequence_json.get("steps", []):
                operation = operations_by_id.get(step.get("operation_id"))
                if operation is not None:
                    sequence_operations.append(operation)
            sequences.append(SequencePlan(sequence_operations))
        coverage = data.get("coverage", {})
        plan = cls(machine, sequences, coverage, data.get("random_baseline"))
        plan.model = data.get("model")
        return plan


def load_sequence_plan(path):
    with open(path, "r", encoding="utf-8") as plan_file:
        return StatefulSequencePlan.from_dict(json.load(plan_file))


def _write_json(path, data, indent=2):
    directory = os.path.dirname(os.path.abspath(path))
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
    with open(path, "w", encoding="utf-8") as output_file:
        json.dump(data, output_file, indent=indent, sort_keys=True)
        output_file.write("\n")


def _sequence_coverage_edges(machine, operation_ids):
    state = machine.initial_state
    edges = set()
    for operation_id in operation_ids:
        operation = machine.operations_by_id.get(operation_id)
        if operation is None:
            break
        next_state = machine.apply(operation, state)
        if next_state is None:
            break
        edges.add((state, operation_id))
        state = next_state
    return edges


def generate_stateful_sequences(
    machine,
    max_states=10000,
    max_depth=100,
    max_sequences=None,
    baseline_sequence_count=None,
    baseline_max_depth=2,
    random_seed=12345,
):
    """Generates a deterministic sequence suite that covers state transitions.

    Candidate sequences are constructed from the shortest path to each
    transition's source state.  A deterministic greedy set cover then chooses
    sequences, avoiding a large number of redundant prefixes.
    """
    exploration = machine.explore(max_states=max_states, max_depth=max_depth)
    all_edges = set(exploration.transitions.keys())
    candidates = {}

    for (state, operation_id) in sorted(
        all_edges, key=lambda edge: (edge[0], edge[1])
    ):
        operation_ids = tuple(exploration.path_for_transition(state, operation_id))
        candidates.setdefault(operation_ids, _sequence_coverage_edges(machine, operation_ids))

    uncovered = set(all_edges)
    selected = []
    candidate_heap = []
    for operation_ids, covered in candidates.items():
        heapq.heappush(
            candidate_heap,
            (-len(covered), len(operation_ids), operation_ids, 0),
        )

    while uncovered:
        selected_candidate = None
        while candidate_heap:
            negative_gain, sequence_length, operation_ids, version = heapq.heappop(
                candidate_heap
            )
            covered = candidates[operation_ids]
            gain = covered & uncovered
            if not gain:
                continue
            # The heap entry may be stale after another sequence covered some
            # of its transitions.  Reinsert it with the current gain.
            if len(gain) != -negative_gain:
                heapq.heappush(
                    candidate_heap,
                    (-len(gain), sequence_length, operation_ids, version + 1),
                )
                continue
            selected_candidate = (operation_ids, gain)
            break
        if selected_candidate is None:
            break
        operation_ids, gain = selected_candidate
        selected.append(operation_ids)
        uncovered -= gain
        if max_sequences is not None and len(selected) >= max_sequences:
            break

    selected.sort(key=lambda ids: (len(ids), ids))
    sequences = [
        SequencePlan([machine.operations_by_id[operation_id] for operation_id in ids])
        for ids in selected
    ]
    coverage_obj, coverage = measure_sequences(
        machine, [sequence.operation_ids for sequence in sequences], exploration
    )

    baseline_count = baseline_sequence_count
    if baseline_count is None:
        baseline_count = max(1, len(sequences))
    baseline_sequences = random_baseline_sequences(
        machine,
        sequence_count=baseline_count,
        max_depth=min(baseline_max_depth, max_depth),
        random_seed=random_seed,
        max_states=max_states,
    )
    _, baseline_coverage = measure_sequences(
        machine, baseline_sequences, exploration
    )
    baseline_coverage["baseline"] = "random-request-combinations"
    baseline_coverage["seed"] = random_seed

    plan = StatefulSequencePlan(
        machine, sequences, coverage, baseline=baseline_coverage
    )
    plan.exploration = exploration
    plan.actual_coverage = coverage_obj
    return plan


def random_baseline_sequences(
    machine,
    sequence_count=100,
    max_depth=2,
    random_seed=12345,
    max_states=10000,
):
    """Deterministically generates unguided request combinations for comparison."""
    import random

    rng = random.Random(random_seed)
    sequences = []
    for _ in range(max(0, int(sequence_count))):
        state = machine.initial_state
        operation_ids = []
        for _ in range(max_depth):
            enabled = sorted(machine.enabled_operations(state), key=lambda op: op.operation_id)
            if not enabled:
                break
            operation = rng.choice(enabled)
            next_state = machine.apply(operation, state)
            if next_state is None:
                break
            operation_ids.append(operation.operation_id)
            state = next_state
        sequences.append(operation_ids)
    return sequences


def state_coverage_report(plan):
    """Returns a compact, machine-readable coverage report for a plan."""
    coverage = plan.coverage
    baseline = plan.baseline or {}
    exploration = getattr(plan, "exploration", None)
    if exploration is not None:
        model = exploration.coverage_dict()
    else:
        model = plan.model or {
            "states_reachable": coverage.get("states_total", 0),
            "transitions_reachable": coverage.get("transitions_total", 0),
            "max_states": None,
            "max_depth": None,
            "truncated": False,
        }
    state_delta = coverage.get("state_coverage", 0.0) - baseline.get(
        "state_coverage", 0.0
    )
    transition_delta = coverage.get("transition_coverage", 0.0) - baseline.get(
        "transition_coverage", 0.0
    )
    report = {
        "plan_id": plan.plan_id,
        "model_digest": plan.model_digest,
        "model": model,
        "stateful": coverage,
        "random_baseline": baseline,
        "improvement": {
            "state_coverage_delta": round(state_delta, 6),
            "transition_coverage_delta": round(transition_delta, 6),
            "state_coverage_relative": _relative_improvement(
                coverage.get("state_coverage", 0.0),
                baseline.get("state_coverage", 0.0),
            ),
            "transition_coverage_relative": _relative_improvement(
                coverage.get("transition_coverage", 0.0),
                baseline.get("transition_coverage", 0.0),
            ),
        },
    }
    if hasattr(plan, "actual_coverage"):
        report["actual"] = plan.actual_coverage.to_dict(
            exploration,
            states_total=coverage.get("states_total"),
            transitions_total=coverage.get("transitions_total"),
        )
    return report


def _relative_improvement(new_value, old_value):
    if old_value <= 0:
        return None
    return round((new_value - old_value) / old_value, 6)


def record_request_sequence(machine, coverage, requests):
    """Records coverage for Request objects returned by the main driver."""
    operation_ids = []
    for request in requests:
        operation = machine.operation_for_request(request)
        if operation is not None:
            operation_ids.append(operation.operation_id)
    coverage.record_sequence(operation_ids)


# ---------------------------------------------------------------------------
# OpenAPI fallback inference
# ---------------------------------------------------------------------------


def _load_document(path_or_document):
    if isinstance(path_or_document, dict):
        return path_or_document
    with open(path_or_document, "r", encoding="utf-8") as document_file:
        contents = document_file.read()
    try:
        return json.loads(contents)
    except ValueError:
        try:
            import yaml
        except ImportError:
            raise ValueError(
                "YAML OpenAPI documents require PyYAML to be installed"
            )
        return yaml.safe_load(contents)


def _resolve_ref(document, value, seen=None):
    if not isinstance(value, dict):
        return value
    ref = value.get("$ref")
    if not ref or not ref.startswith("#/"):
        return value
    seen = seen or set()
    if ref in seen:
        return {}
    seen.add(ref)
    current = document
    for part in ref[2:].split("/"):
        part = part.replace("~1", "/").replace("~0", "~")
        current = current[part]
    return _resolve_ref(document, current, seen)


def _schema_properties(document, schema, prefix=None, seen=None):
    """Returns leaf property names in a schema, including nested object paths."""
    schema = _resolve_ref(document, schema or {})
    prefix = prefix or []
    seen = seen or set()
    schema_id = id(schema)
    if schema_id in seen:
        return []
    seen.add(schema_id)

    properties = []
    for composition_key in ("allOf", "oneOf", "anyOf"):
        for composed in schema.get(composition_key, []) or []:
            properties.extend(
                _schema_properties(document, composed, prefix, seen)
            )

    if schema.get("type") == "array" or "items" in schema:
        properties.extend(
            _schema_properties(document, schema.get("items", {}), prefix, seen)
        )

    for name, child_schema in (schema.get("properties") or {}).items():
        child_schema = _resolve_ref(document, child_schema or {})
        child_prefix = prefix + [name]
        if child_schema.get("properties") or child_schema.get("allOf"):
            nested = _schema_properties(
                document, child_schema, child_prefix, seen
            )
            properties.extend(nested)
        else:
            properties.append((".".join(child_prefix), name))
    return properties


def _operation_parameters(document, path_item, operation):
    parameters = {}
    for source in (path_item.get("parameters", []), operation.get("parameters", [])):
        for parameter in source or []:
            parameter = _resolve_ref(document, parameter)
            name = parameter.get("name")
            if name:
                parameters[(name, parameter.get("in"))] = parameter
    return list(parameters.values())


def _request_schema(document, operation):
    schemas = []
    for parameter in operation.get("_parameters", []):
        if parameter.get("in") == "body":
            schemas.append(_resolve_ref(document, parameter.get("schema", {})))
    request_body = _resolve_ref(document, operation.get("requestBody", {}))
    for content in (request_body.get("content") or {}).values():
        schemas.append(_resolve_ref(document, content.get("schema", {})))
    return schemas


def _response_schemas(document, operation):
    schemas = []
    for status, response in (operation.get("responses") or {}).items():
        if not str(status).startswith("2"):
            continue
        response = _resolve_ref(document, response)
        if isinstance(response.get("schema"), dict):
            schemas.append(_resolve_ref(document, response["schema"]))
        for content in (response.get("content") or {}).values():
            schemas.append(_resolve_ref(document, content.get("schema", {})))
    return schemas


def _path_parameters(endpoint):
    return re.findall(r"\{([^{}]+)\}", endpoint)


def _collection_endpoint(endpoint):
    parts = [part for part in endpoint.split("/") if part]
    if parts and parts[-1].startswith("{") and parts[-1].endswith("}"):
        parts = parts[:-1]
    return "/" + "/".join(parts)


def _resource_name(endpoint):
    collection = _collection_endpoint(endpoint)
    parts = [part for part in collection.split("/") if part]
    return _singularize(parts[-1]) if parts else "resource"


def _find_producer(
    consumer_operation,
    parameter_name,
    producer_records,
):
    endpoint = consumer_operation["endpoint"]
    parameter_token = "{" + parameter_name + "}"
    parameter_index = endpoint.find(parameter_token)
    parent_endpoint = (
        endpoint[:parameter_index].rstrip("/")
        if parameter_index > 0
        else _collection_endpoint(endpoint)
    )
    candidates = []
    for record in producer_records:
        producer = record["operation"]
        producer_endpoint = producer["endpoint"]
        endpoint_match = (
            producer_endpoint == parent_endpoint
            or producer_endpoint == endpoint
        )
        if not endpoint_match:
            continue
        property_name = record["property"]
        if not _matches_parameter_to_property(
            parameter_name, property_name, record["resource"]
        ):
            continue
        rank = (
            0 if producer_endpoint == parent_endpoint else 1,
            0 if property_name.lower() == parameter_name.lower() else 1,
            0 if producer["method"] == "POST" else 1,
            len(producer_endpoint),
            producer["operation_id"],
        )
        candidates.append((rank, record))
    return min(candidates, key=lambda item: item[0])[1] if candidates else None


def operations_from_openapi(document):
    """Infers state-machine operations directly from an OpenAPI document.

    This fallback is intentionally conservative and mirrors the compiler's
    dependency heuristics for common REST resource hierarchies.  When a
    compiler-generated grammar is available, prefer ``operations_from_grammar``
    because it contains the exact annotations and dictionary decisions used by
    the RESTler engine.
    """
    document = _load_document(document)
    operations = []
    for path, path_item in (document.get("paths") or {}).items():
        path_item = _resolve_ref(document, path_item)
        for method in HTTP_METHODS:
            operation_json = path_item.get(method.lower())
            if not isinstance(operation_json, dict):
                continue
            operation = dict(operation_json)
            operation["_parameters"] = _operation_parameters(
                document, path_item, operation
            )
            path_parameters = _path_parameters(path)
            body_properties = []
            for schema in _request_schema(document, operation):
                body_properties.extend(_schema_properties(document, schema))
            response_properties = []
            for schema in _response_schemas(document, operation):
                response_properties.extend(_schema_properties(document, schema))
            operation.update(
                {
                    "endpoint": path,
                    "method": method,
                    "operation_id": "{} {}#0".format(method, path),
                    "path_parameters": path_parameters,
                    "body_properties": body_properties,
                    "response_properties": response_properties,
                }
            )
            operations.append(operation)

    operations.sort(key=lambda item: (item["endpoint"], item["method"]))
    producer_records = []
    for operation in operations:
        method = operation["method"]
        endpoint = operation["endpoint"]
        if method not in PRODUCER_METHODS:
            continue
        response_properties = operation["response_properties"]
        id_properties = [
            name for _, name in response_properties if _is_id_like(name)
        ]
        if not id_properties:
            id_properties = [_resource_name(endpoint) + "Id"]
        for property_name in sorted(set(id_properties)):
            producer_records.append(
                {
                    "operation": operation,
                    "property": property_name,
                    "resource": _resource_name(endpoint),
                    "variable": "{} {}::{}".format(
                        method, endpoint, property_name
                    ),
                }
            )

    produces_by_operation = {}
    consumes_by_operation = {}
    for record in producer_records:
        produces_by_operation.setdefault(
            record["operation"]["operation_id"], set()
        ).add(record["variable"])

    for operation in operations:
        operation_id = operation["operation_id"]
        for parameter_name in operation["path_parameters"]:
            record = _find_producer(operation, parameter_name, producer_records)
            if record is None:
                continue
            producer_id = record["operation"]["operation_id"]
            variable = record["variable"]
            if producer_id == operation_id:
                # The operation is allowed to create the value it uses (PUT
                # create-or-update).  Keep the writer but do not require it.
                produces_by_operation.setdefault(operation_id, set()).add(variable)
                continue
            consumes_by_operation.setdefault(operation_id, set()).add(variable)

            # A producer that itself consumes an ancestor resource remains
            # enabled only after that ancestor has been created.
            for parent_parameter in record["operation"]["path_parameters"][:-1]:
                parent_record = _find_producer(
                    record["operation"], parent_parameter, producer_records
                )
                if parent_record is not None:
                    consumes_by_operation.setdefault(producer_id, set()).add(
                        parent_record["variable"]
                    )
                    produces_by_operation.setdefault(
                        parent_record["operation"]["operation_id"], set()
                    ).add(parent_record["variable"])

        for property_path, property_name in operation["body_properties"]:
            if not _is_id_like(property_name):
                continue
            pseudo_consumer = {
                "endpoint": operation["endpoint"],
                "method": operation["method"],
                "operation_id": operation_id,
            }
            record = _find_producer(
                pseudo_consumer, property_name, producer_records
            )
            if record is not None and record["operation"]["operation_id"] != operation_id:
                variable = record["variable"]
                consumes_by_operation.setdefault(operation_id, set()).add(variable)
                produces_by_operation.setdefault(
                    record["operation"]["operation_id"], set()
                ).add(variable)

    result = []
    for operation in operations:
        result.append(
            ApiOperation(
                method=operation["method"],
                endpoint=operation["endpoint"],
                consumes=consumes_by_operation.get(operation["operation_id"], set()),
                produces=produces_by_operation.get(operation["operation_id"], set()),
                operation_id=operation["operation_id"],
                definition_hash=_stable_hash(
                    {
                        "method": operation["method"],
                        "endpoint": operation["endpoint"],
                    }
                ),
            )
        )
    return _sort_operations(result)


def build_state_machine(operations, max_resource_count=1):
    if isinstance(operations, dict):
        operations = operations_from_grammar(operations)
    elif isinstance(operations, (str, os.PathLike)):
        operations = operations_from_grammar(operations)
    else:
        operations = list(operations)
        if operations and not isinstance(operations[0], ApiOperation):
            operations = operations_from_requests(operations)
    return StateMachine(operations, max_resource_count=max_resource_count)


def build_state_machine_from_requests(requests, max_resource_count=1):
    return build_state_machine(
        operations_from_requests(requests), max_resource_count=max_resource_count
    )


def build_state_machine_from_grammar(grammar, max_resource_count=1):
    return build_state_machine(
        operations_from_grammar(grammar), max_resource_count=max_resource_count
    )


def build_state_machine_from_openapi(document, max_resource_count=1):
    return build_state_machine(
        operations_from_openapi(document), max_resource_count=max_resource_count
    )

